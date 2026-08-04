from django.shortcuts import render, redirect
from django.contrib.auth import authenticate, login, logout
from django.contrib.auth.models import User
from django.contrib import messages
from django.http import JsonResponse
from django.conf import settings
from django.contrib.sessions.models import Session
from django.db import connections, transaction
import json
import csv
import re
import datetime
import glob
import os
import threading
import logging
from django.utils import timezone

from ..kotak_neo_api import KotakNeoAPI
from ..models import UserNeoCredentials, SessionActivity, SMTPSettings, UserSecurity, PlatformSettings, ActiveMarketData
from ..forms import LoginForm, RegistrationForm, UserNeoCredentialsForm, UserProfileForm, TOTPForm, ForgotPasswordForm, SetNewPasswordForm, ChangePasswordForm, OTPVerifyForm
from ..decorators import login_required_with_session_check, ajax_login_required
from .helpers import logout_sdk_for_user, generate_temp_password, send_password_change_confirmation_email

logger = logging.getLogger(__name__)


def login_view(request):
    """Handle user login"""
    if request.user.is_authenticated:
        return redirect('index')
    
    if request.method == 'POST':
        form = LoginForm(request.POST)
        if form.is_valid():
            username = form.cleaned_data.get('username')
            password = form.cleaned_data.get('password')
            user = authenticate(request, username=username, password=password)
            
            if user is not None:
                logger.info(f"User '{username}' authenticated successfully via login page.")
                login(request, user)
                # Create or update session activity
                SessionActivity.objects.update_or_create(
                    session_key=request.session.session_key,
                    defaults={
                        'user': user,
                        'ip_address': get_client_ip(request)
                    }
                )
                request.session['server_boot_id'] = settings.SERVER_BOOT_ID
                messages.success(request, f"Welcome back, {username}!")
                
                # Redirect to next page or index
                next_page = request.GET.get('next', 'index')
                return redirect(next_page)
            else:
                logger.warning(f"Failed login attempt for username '{username}'.")
                messages.error(request, "Invalid username or password.")
    else:
        form = LoginForm()
    
    context = {
        'form': form,
        'expired': request.GET.get('expired') == 'true',
        'smtp_settings': SMTPSettings.get_settings(),
        'platform_settings': PlatformSettings.get_settings()
    }
    return render(request, 'login.html', context)


from django.views.decorators.csrf import ensure_csrf_cookie


@ensure_csrf_cookie
def ajax_login_view(request):
    """Handle AJAX login from the modal"""
    if request.method != 'POST':
        return JsonResponse({'status': 'error', 'message': 'Only POST allowed'}, status=405)
    
    try:
        data = json.loads(request.body)
        username = data.get('username')
        password = data.get('password')
        
        if not username or not password:
            return JsonResponse({'status': 'error', 'message': 'Username and password required'}, status=400)
            
        user = authenticate(request, username=username, password=password)
        if user is not None:
            login(request, user)
            SessionActivity.objects.update_or_create(
                session_key=request.session.session_key,
                defaults={
                    'user': user,
                    'ip_address': get_client_ip(request),
                    'last_activity': timezone.now()
                }
            )
            request.session['server_boot_id'] = settings.SERVER_BOOT_ID
            logger.info(f"User '{username}' re-authenticated via AJAX modal.")
            return JsonResponse({'status': 'success', 'message': 'Logged in successfully'})
        else:
            return JsonResponse({'status': 'error', 'message': 'Invalid username or password'}, status=401)
            
    except json.JSONDecodeError:
        return JsonResponse({'status': 'error', 'message': 'Invalid JSON'}, status=400)
    except Exception as e:
        logger.error(f"Error in ajax_login_view: {e}")
        return JsonResponse({'status': 'error', 'message': str(e)}, status=500)


def register_view(request):
    """Handle user registration"""
    if request.user.is_authenticated:
        return redirect('index')
    
    # Check if this is the first user ever registering
    is_first_user = not User.objects.exists()
    
    platform_settings = PlatformSettings.get_settings()
    if not is_first_user and not platform_settings.enable_user_registration:
        messages.error(request, "User registration is currently disabled by the administrator.")
        return redirect('login')
    
    if request.method == 'POST':
        form = RegistrationForm(request.POST)
        if form.is_valid():
            settings_obj = SMTPSettings.get_settings()
            
            # If first user, skip OTP to ensure they can gain access without SMTP setup
            if settings_obj.enable_registration_otp and not is_first_user:
                user = form.save(commit=False)
                user.is_active = False  # Deactivate until verified
                user.save()
                
                # Generate and store OTP
                import random
                import string
                otp = ''.join(random.choice(string.digits) for _ in range(6))
                
                request.session['registration_user_id'] = user.id
                request.session['registration_otp'] = otp
                
                # Send email
                try:
                    from django.core.mail import get_connection
                    connection = get_connection(
                        host=settings_obj.host,
                        port=settings_obj.port,
                        username=settings_obj.host_user,
                        password=settings_obj.get_decrypted_password(),
                        use_tls=settings_obj.use_tls
                    )
                    email_msg = settings_obj.send_html_email(
                        subject_template=settings_obj.otp_subject,
                        body_template=settings_obj.otp_template,
                        context_dict={'username': user.username, 'otp': otp},
                        to_emails=[user.email],
                        connection=connection
                    )
                    email_msg.send(fail_silently=False)
                    messages.success(request, "A verification code has been sent to your email.")
                except Exception as e:
                    import logging
                    logging.getLogger(__name__).error(f"Error sending OTP email: {e}")
                    user.delete() # Revert account creation since code could not dispatch
                    messages.error(request, "Failed to send verification email. Please try again later.")
                    return redirect('register')
                    
                return redirect('otp_verify')
            else:
                user = form.save(commit=False)
                if is_first_user:
                    user.is_superuser = True
                    user.is_staff = True
                    user.is_active = True
                    
                user.save()
                
                if is_first_user:
                    logger.info(f"First user registered and promoted to superuser: '{user.username}'.")
                    messages.success(request, f"Welcome, {user.username}! As the first user, you have been granted administrative privileges.")
                else:
                    logger.info(f"New user registered: '{user.username}'.")
                    messages.success(request, "Registration successful! Please configure your Neo API credentials.")
                
                # Redirect to credentials setup
                login(request, user)
                SessionActivity.objects.update_or_create(
                    session_key=request.session.session_key,
                    defaults={
                        'user': user,
                        'ip_address': get_client_ip(request)
                    }
                )
                return redirect('setup_credentials')
    else:
        form = RegistrationForm()
        
    return render(request, 'register.html', {'form': form})


def otp_verify_view(request):
    """Verify numeric OTP to confirm email and activate account"""
    user_id = request.session.get('registration_user_id')
    stored_otp = request.session.get('registration_otp')
    
    if not user_id or not stored_otp:
        messages.error(request, "OTP session expired or invalid. Please register again.")
        return redirect('register')
        
    if request.method == 'POST':
        form = OTPVerifyForm(request.POST)
        if form.is_valid():
            entered_otp = form.cleaned_data.get('otp')
            if entered_otp == stored_otp:
                try:
                    user = User.objects.get(id=user_id)
                    user.is_active = True
                    user.save()
                    
                    # Clean up
                    del request.session['registration_user_id']
                    del request.session['registration_otp']
                    
                    messages.success(request, "Email verified successfully! Welcome.")
                    login(request, user)
                    SessionActivity.objects.update_or_create(
                        session_key=request.session.session_key,
                        defaults={
                            'user': user,
                            'ip_address': get_client_ip(request)
                        }
                    )
                    request.session['server_boot_id'] = settings.SERVER_BOOT_ID
                    return redirect('setup_credentials')
                except User.DoesNotExist:
                    messages.error(request, "User account no longer exists.")
                    return redirect('register')
            else:
                form.add_error('otp', "Invalid verification code.")
    else:
        form = OTPVerifyForm()
        
    return render(request, 'otp_verify.html', {'form': form})


def logout_view(request):
    """Handle user logout"""
    if request.user.is_authenticated:
        username = request.user.username
        logout_sdk_for_user(request.user, request=request)
        SessionActivity.objects.filter(session_key=request.session.session_key).delete()
        logout(request)
        logger.info(f"User '{username}' logged out.")
        messages.success(request, f"Logged out successfully. Goodbye, {username}!")
    return redirect('login')


def forgot_password_view(request):
    """Handle forgotten password requests utilizing SMTP settings"""
    settings_obj = SMTPSettings.get_settings()
    if not settings_obj.enable_password_reset:
        messages.error(request, "Password reset is currently disabled by the administrator.")
        return redirect('login')

    if request.method == 'POST':
        form = ForgotPasswordForm(request.POST)
        if form.is_valid():
            email = form.cleaned_data.get('email')
            try:
                user = User.objects.get(email=email)
            except User.DoesNotExist:
                user = None

            if user:
                temp_password = generate_temp_password()
                
                try:
                    from django.core.mail import get_connection
                    connection = get_connection(
                        host=settings_obj.host,
                        port=settings_obj.port,
                        username=settings_obj.host_user,
                        password=settings_obj.get_decrypted_password(),
                        use_tls=settings_obj.use_tls
                    )
                    email_msg = settings_obj.send_html_email(
                        subject_template=settings_obj.forgot_password_subject,
                        body_template=settings_obj.forgot_password_template,
                        context_dict={'username': user.username, 'temp_password': temp_password},
                        to_emails=[user.email],
                        connection=connection
                    )
                    email_msg.send(fail_silently=False)
                    
                    # If email sent successfully, apply password and force change lock
                    user.set_password(temp_password)
                    user.save()
                    
                    security, _ = UserSecurity.objects.get_or_create(user=user)
                    security.force_password_change = True
                    security.save()
                    
                    messages.success(request, "If an account exists with that email, a temporary password has been sent.")
                except Exception as e:
                    import logging
                    logging.getLogger(__name__).error(f"Error sending password reset email: {e}")
                    messages.error(request, "Failed to send reset email. Your password was not changed. Please try again later or contact an administrator.")
            else:
                # Still show success to prevent email enumeration
                messages.success(request, "If an account exists with that email, a temporary password has been sent.")
            return redirect('login')
    else:
        form = ForgotPasswordForm()
        
    return render(request, 'forgot_password.html', {'form': form})


@login_required_with_session_check
def set_new_password_view(request):
    """Force user to specify a new password after a reset"""
    security = getattr(request.user, 'security', None)
    if not security or not security.force_password_change:
        messages.info(request, "You are not required to set a new password at this time.")
        return redirect('index')

    if request.method == 'POST':
        form = SetNewPasswordForm(request.POST)
        if form.is_valid():
            new_password = form.cleaned_data.get('new_password')
            request.user.set_password(new_password)
            request.user.save()
            
            # Clear flag
            security.force_password_change = False
            security.save()
            
            # Re-authenticate the user without logging them out entirely
            from django.contrib.auth import update_session_auth_hash
            update_session_auth_hash(request, request.user)
            
            # Send confirmation
            send_password_change_confirmation_email(request.user)
            
            messages.success(request, "Your new password has been set successfully.")
            return redirect('index')
    else:
        form = SetNewPasswordForm()

    return render(request, 'change_password.html', {
        'form': form, 
        'title': 'Set New Permanent Password',
        'is_force_change': True
    })


@login_required_with_session_check
def change_password_view(request):
    """Allow user to manually change their password from profile requiring current password"""
    if request.method == 'POST':
        form = ChangePasswordForm(request.POST)
        if form.is_valid():
            current_password = form.cleaned_data.get('current_password')
            if not request.user.check_password(current_password):
                form.add_error('current_password', "Your current password was entered incorrectly.")
            else:
                new_password = form.cleaned_data.get('new_password')
                request.user.set_password(new_password)
                request.user.save()
                
                # Maintain authenticated session
                from django.contrib.auth import update_session_auth_hash
                update_session_auth_hash(request, request.user)
                
                # Send confirmation
                send_password_change_confirmation_email(request.user)
                
                messages.success(request, "Your password has been changed successfully.")
                return redirect('profile')
    else:
        form = ChangePasswordForm()

    return render(request, 'change_password.html', {
        'form': form,
        'title': 'Change Password',
        'is_force_change': False
    })



# ==================== Trading Views (Protected) ====================

_scrip_refresh_lock = threading.Lock()


@login_required_with_session_check
def profile_view(request):
    """View and edit user profile"""
    if request.method == 'POST':
        form = UserProfileForm(request.POST, instance=request.user)
        if form.is_valid():
            form.save()
            messages.success(request, "Profile updated successfully!")
            return redirect('profile')
    else:
        form = UserProfileForm(instance=request.user)
    
    try:
        user_creds = UserNeoCredentials.objects.get(user=request.user)
        has_credentials = True
    except UserNeoCredentials.DoesNotExist:
        has_credentials = False
    
    sdk_status = False
    if has_credentials:
        try:
            session_activity = SessionActivity.objects.get(session_key=request.session.session_key)
            sdk_status = session_activity.is_sdk_session_valid()
        except Exception:
            sdk_status = False

    return render(request, 'profile.html', {
        'form': form,
        'has_credentials': has_credentials,
        'sdk_status': sdk_status,
    })


@login_required_with_session_check
def admin_settings_view(request):
    """View and update global SMTP settings (Superuser only)"""
    if not request.user.is_superuser:
        messages.error(request, "Access denied. Superuser only.")
        return redirect('index')
    
    settings_obj = SMTPSettings.get_settings()
    platform_settings = PlatformSettings.get_settings()
    
    if request.method == 'POST':
        # Handle SMTP Settings
        settings_obj.host = request.POST.get('host', 'smtp.gmail.com')
        try:
            settings_obj.port = int(request.POST.get('port', 587))
        except ValueError:
            settings_obj.port = 587
        settings_obj.use_tls = request.POST.get('use_tls') == 'on'
        settings_obj.enable_password_reset = request.POST.get('enable_password_reset') == 'on'
        settings_obj.enable_registration_otp = request.POST.get('enable_registration_otp') == 'on'
        settings_obj.enable_order_notifications = request.POST.get('enable_order_notifications') == 'on'
        settings_obj.host_user = request.POST.get('host_user', '')
        settings_obj.from_address = request.POST.get('from_address', '')
        settings_obj.from_name = request.POST.get('from_name', 'JK Terminal')
        
        # Save email templates
        settings_obj.otp_subject = request.POST.get('otp_subject', '')
        settings_obj.otp_template = request.POST.get('otp_template', '')
        settings_obj.password_changed_subject = request.POST.get('password_changed_subject', '')
        settings_obj.password_changed_template = request.POST.get('password_changed_template', '')
        settings_obj.forgot_password_subject = request.POST.get('forgot_password_subject', '')
        settings_obj.forgot_password_template = request.POST.get('forgot_password_template', '')
        settings_obj.order_placed_subject = request.POST.get('order_placed_subject', '')
        settings_obj.order_placed_template = request.POST.get('order_placed_template', '')
        settings_obj.order_status_subject = request.POST.get('order_status_subject', '')
        settings_obj.order_status_template = request.POST.get('order_status_template', '')
        settings_obj.order_modified_subject = request.POST.get('order_modified_subject', '')
        settings_obj.order_modified_template = request.POST.get('order_modified_template', '')
        
        new_password = request.POST.get('host_password', '')
        if new_password:
            settings_obj.host_password = new_password
        settings_obj.save()

        # Handle Platform Settings
        platform_settings.session_timeout_enabled = request.POST.get('session_timeout_enabled') == 'on'
        platform_settings.sdk_timeout_enabled = request.POST.get('sdk_timeout_enabled') == 'on'
        platform_settings.enable_user_registration = request.POST.get('enable_user_registration') == 'on'
        platform_settings.allow_direct_secret_auth = request.POST.get('allow_direct_secret_auth') == 'on'
        try:
            platform_settings.session_timeout_seconds = int(request.POST.get('session_timeout_seconds', 300))
            platform_settings.sdk_timeout_seconds = int(request.POST.get('sdk_timeout_seconds', 1800))
        except ValueError:
            pass # Keep previous values if invalid
        
        old_allow_restore = platform_settings.allow_session_restore
        new_allow_restore = request.POST.get('allow_session_restore') == 'on'
        
        platform_settings.allow_session_restore = new_allow_restore
        platform_settings.save()

        if old_allow_restore != new_allow_restore:
            # Delete all other sessions except the current one
            current_session_key = request.session.session_key
            if current_session_key:
                Session.objects.exclude(session_key=current_session_key).delete()
            else:
                Session.objects.all().delete()
                
            SessionActivity.objects.all().delete()
            KotakNeoAPI._session_cache.clear()
            
            # Safely flush current session
            logout(request)
            return redirect('login')

        messages.success(request, "All settings updated successfully!")
        return redirect('admin_settings')

    users = User.objects.all().order_by('-is_superuser', 'username')
    active_sessions = SessionActivity.objects.select_related('user').all().order_by('user__username', '-last_activity')
    registration_form = RegistrationForm()

    return render(request, 'admin_settings.html', {
        'settings': settings_obj,
        'platform_settings': platform_settings,
        'users': users,
        'active_sessions': active_sessions,
        'registration_form': registration_form
    })


@login_required_with_session_check
def admin_toggle_superuser(request, user_id):
    """Toggle superuser status"""
    if not request.user.is_superuser:
        messages.error(request, "Access denied. Superuser only.")
        return redirect('index')
        
    if request.method == 'POST':
        try:
            target_user = User.objects.get(id=user_id)
            if target_user == request.user:
                messages.warning(request, "You cannot modify your own superuser status.")
            else:
                target_user.is_superuser = not target_user.is_superuser
                target_user.is_staff = target_user.is_superuser  # Staff matches superuser for access
                target_user.save()
                
                status = "promoted to" if target_user.is_superuser else "demoted from"
                messages.success(request, f"User {target_user.username} successfully {status} superuser.")
        except User.DoesNotExist:
            messages.error(request, "User does not exist.")
            
    return redirect('admin_settings')


@login_required_with_session_check
def admin_delete_user(request, user_id):
    """Forcefully delete a user"""
    if not request.user.is_superuser:
        messages.error(request, "Access denied. Superuser only.")
        return redirect('index')
        
    if request.method == 'POST':
        try:
            target_user = User.objects.get(id=user_id)
            if target_user == request.user:
                messages.error(request, "You cannot delete your own active session account.")
            else:
                username = target_user.username
                target_user.delete()
                messages.success(request, f"User '{username}' was permanently deleted.")
        except User.DoesNotExist:
            messages.error(request, "User does not exist.")
            
    return redirect('admin_settings')


@login_required_with_session_check
def admin_reset_user_password(request, user_id):
    """Forcefully reset a user's password with an optional force-change flag"""
    if not request.user.is_superuser:
        messages.error(request, "Access denied. Superuser only.")
        return redirect('index')
        
    if request.method == 'POST':
        try:
            target_user = User.objects.get(id=user_id)
            if target_user == request.user:
                messages.error(request, "You cannot forcefully reset your own active session account.")
            else:
                new_password = request.POST.get('new_password')
                force_change = request.POST.get('force_change') == 'on'
                
                if new_password and len(new_password) >= 8:
                    target_user.set_password(new_password)
                    target_user.save()
                    
                    security, _ = UserSecurity.objects.get_or_create(user=target_user)
                    security.force_password_change = force_change
                    security.save()
                    
                    messages.success(request, f"Password for {target_user.username} was forcefully reset successfully.")
                else:
                    messages.error(request, "The constructed password must be at least 8 characters.")
        except User.DoesNotExist:
            messages.error(request, "User does not exist.")
            
    return redirect('admin_settings')

# ==================== Password Management Views ====================


@login_required_with_session_check
def admin_add_user_view(request):
    """Add a new user directly from the admin panel"""
    if not request.user.is_superuser:
        messages.error(request, "Access denied. Superuser only.")
        return redirect('index')
        
    if request.method == 'POST':
        form = RegistrationForm(request.POST)
        if form.is_valid():
            new_user = form.save()
            messages.success(request, f"User '{new_user.username}' created successfully.")
        else:
            for field, errors in form.errors.items():
                for error in errors:
                    messages.error(request, f"Creation failed: {field} - {error}")
                    
    return redirect('admin_settings')


@login_required_with_session_check
def admin_delete_session(request, session_id):
    """Forcefully terminate a user session"""
    if not request.user.is_superuser:
        messages.error(request, "Access denied. Superuser only.")
        return redirect('index')
        
    if request.method == 'POST':
        try:
            activity = SessionActivity.objects.get(id=session_id)
            session_key = activity.session_key
            username = activity.user.username
            ip_addr = activity.ip_address
            
            # 1. Delete the Django session
            if session_key:
                Session.objects.filter(session_key=session_key).delete()
            
            # 2. Delete the activity record
            activity.delete()
            
            messages.success(request, f"Session for user '{username}' (IP: {ip_addr}) was terminated.")
            logger.info(f"Admin {request.user.username} terminated session for {username} (IP: {ip_addr})")
            
        except SessionActivity.DoesNotExist:
            messages.error(request, "Session no longer exists.")
        except Exception as e:
            logger.error(f"Error deleting session: {e}")
            messages.error(request, f"Error terminating session: {str(e)}")
            
    return redirect('admin_settings')


@login_required_with_session_check
def admin_bulk_delete_sessions(request):
    """Forcefully terminate multiple user sessions based on selection or action"""
    if not request.user.is_superuser:
        messages.error(request, "Access denied. Superuser only.")
        return redirect('index')

    if request.method == 'POST':
        action = request.POST.get('action')
        current_session_key = request.session.session_key
        
        if action == 'delete_selected':
            session_ids = request.POST.getlist('session_ids')
            if session_ids:
                # Exclude current session to prevent accidental self-logout
                activities = SessionActivity.objects.filter(id__in=session_ids).exclude(session_key=current_session_key)
                count = activities.count()
                for activity in activities:
                    if activity.session_key:
                        Session.objects.filter(session_key=activity.session_key).delete()
                    activity.delete()
                messages.success(request, f"Successfully terminated {count} selected session(s).")
                logger.info(f"Admin {request.user.username} bulk terminated {count} selected session(s).")
            else:
                messages.warning(request, "No sessions were selected.")
                
        elif action == 'delete_all_except_current':
            activities = SessionActivity.objects.exclude(session_key=current_session_key)
            count = activities.count()
            for activity in activities:
                if activity.session_key:
                    Session.objects.filter(session_key=activity.session_key).delete()
                activity.delete()
            messages.success(request, f"Successfully terminated {count} other session(s).")
            logger.info(f"Admin {request.user.username} terminated all other sessions ({count} sessions).")
            
        elif action == 'delete_inactive_sdk':
            activities = SessionActivity.objects.filter(sdk_session_active=False).exclude(session_key=current_session_key)
            count = activities.count()
            for activity in activities:
                if activity.session_key:
                    Session.objects.filter(session_key=activity.session_key).delete()
                activity.delete()
            messages.success(request, f"Successfully terminated {count} SDK-inactive session(s).")
            logger.info(f"Admin {request.user.username} terminated {count} SDK-inactive session(s).")
            
        else:
            messages.error(request, "Invalid bulk action specified.")
            
    return redirect('admin_settings')



@login_required_with_session_check
def admin_test_smtp_view(request):
    """Test SMTP settings by sending a test email (Superuser only)"""
    if not request.user.is_superuser:
        return JsonResponse({'status': 'error', 'message': 'Access denied. Superuser only.'}, status=403)
    
    if request.method != 'POST':
        return JsonResponse({'status': 'error', 'message': 'Only POST method is allowed.'}, status=405)
    
    try:
        data = json.loads(request.body)
    except json.JSONDecodeError:
        return JsonResponse({'status': 'error', 'message': 'Invalid JSON body.'}, status=400)
    
    host = data.get('host')
    port = data.get('port')
    use_tls = data.get('use_tls') is True or data.get('use_tls') == 'on' or data.get('use_tls') == 'true'
    host_user = data.get('host_user', '')
    from_address = data.get('from_address', '')
    test_recipient = data.get('test_recipient', '').strip()
    host_password = data.get('host_password', '')

    if not host or not port or not host_user:
        return JsonResponse({'status': 'error', 'message': 'SMTP Host, Port, and Logon User are required fields.'}, status=400)

    if not test_recipient:
        test_recipient = request.user.email
        if not test_recipient:
            return JsonResponse({'status': 'error', 'message': 'Please specify a recipient email address.'}, status=400)

    try:
        port = int(port)
    except ValueError:
        return JsonResponse({'status': 'error', 'message': 'Port must be a valid integer.'}, status=400)

    # If no password is provided in the test request, use the saved/decrypted one
    if not host_password:
        settings_obj = SMTPSettings.get_settings()
        host_password = settings_obj.get_decrypted_password()
        if not host_password:
            return JsonResponse({'status': 'error', 'message': 'No password provided or saved.'}, status=400)

    try:
        settings_obj = SMTPSettings.get_settings()
        settings_obj.host = host
        settings_obj.port = port
        settings_obj.host_user = host_user
        settings_obj.from_address = from_address
        
        from django.core.mail import get_connection
        connection = get_connection(
            host=host,
            port=port,
            username=host_user,
            password=host_password,
            use_tls=use_tls,
            timeout=10
        )
        
        test_subject = "JK Terminal - SMTP Test Connection"
        test_template = """<!DOCTYPE html>
<html>
<head>
  <style>
    body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; background-color: #f4f5f7; margin: 0; padding: 0; color: #1e293b; }
    .container { max-width: 600px; margin: 40px auto; background: #ffffff; border-radius: 12px; overflow: hidden; box-shadow: 0 4px 6px rgba(0,0,0,0.05); border: 1px solid #e2e8f0; }
    .header { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: #ffffff; padding: 32px 24px; text-align: center; }
    .header h1 { margin: 0; font-size: 24px; font-weight: 700; letter-spacing: -0.5px; }
    .content { padding: 32px 24px; line-height: 1.6; }
    .footer { text-align: center; padding: 24px; font-size: 12px; color: #64748b; background-color: #f8fafc; border-top: 1px solid #f1f5f9; }
  </style>
</head>
<body>
  <div class="container">
    <div class="header">
      <h1>JK Terminal</h1>
    </div>
    <div class="content">
      <p>Hello,</p>
      <p>This is a test email sent from JK Terminal to verify your SMTP settings. If you received this, your SMTP configuration is correct!</p>
      <p>Regards,<br>JK Terminal Team</p>
    </div>
    <div class="footer">
      This is an automated notification from JK Terminal. Please do not reply to this email.
    </div>
  </div>
</body>
</html>"""

        email_msg = settings_obj.send_html_email(
            subject_template=test_subject,
            body_template=test_template,
            context_dict={},
            to_emails=[test_recipient],
            connection=connection
        )
        email_msg.send(fail_silently=False)
        return JsonResponse({
            'status': 'success', 
            'message': f'Test email sent successfully to {test_recipient}!'
        })
    except Exception as e:
        import traceback
        error_detail = str(e)
        logger.error(f"SMTP Test failed: {error_detail}\n{traceback.format_exc()}")
        return JsonResponse({
            'status': 'error', 
            'message': f'Failed to send test email: {error_detail}'
        }, status=500)


# ==================== User Management Views (Superuser Only) ====================


def get_client_ip(request):
    """Extract client IP address from request"""
    x_forwarded_for = request.META.get('HTTP_X_FORWARDED_FOR')
    if x_forwarded_for:
        ip = x_forwarded_for.split(',')[0]
    else:
        ip = request.META.get('REMOTE_ADDR')
    return ip


@login_required_with_session_check
def extend_session(request):
    """Extend the user's session by updating the last activity."""
    if request.method == 'POST':
        # Update session activity to extend the session
        SessionActivity.objects.update_or_create(
            session_key=request.session.session_key,
            defaults={'user': request.user, 'last_activity': timezone.now()}
        )
        return JsonResponse({'status': 'success', 'message': 'Session extended successfully.'})
    return JsonResponse({'status': 'error', 'message': 'Invalid request method.'})


# ==================== Credentials Management Views ====================


