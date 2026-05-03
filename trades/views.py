from django.shortcuts import render, redirect
from django.contrib.auth import authenticate, login, logout
from django.contrib.auth.models import User
from django.contrib import messages
from django.http import JsonResponse
from django.conf import settings
from .kotak_neo_api import KotakNeoAPI
from .models import UserNeoCredentials, SessionActivity, SMTPSettings, UserSecurity, PlatformSettings
from .forms import LoginForm, RegistrationForm, UserNeoCredentialsForm, UserProfileForm, TOTPForm, ForgotPasswordForm, SetNewPasswordForm, ChangePasswordForm, OTPVerifyForm
from .decorators import login_required_with_session_check, ajax_login_required
import json
import duckdb
import glob
import os
import threading
import logging
from django.utils import timezone

logger = logging.getLogger(__name__)

_duckdb_connection = duckdb.connect(database=':memory:')
_duckdb_lock = threading.Lock()

def _quote_sql_string(value):
    return "'" + value.replace("'", "''") + "'"


SCRIP_GROUP_MEANINGS = {
    'bse_cm': {
        'A': 'Active',
        'B': 'Large/Mid Cap',
        'E': 'ETF',
        'F': 'Debt Market',
        'G': 'G-Sec',
        'IF': 'Inst-Debt',
        'M': 'SME',
        'MS': 'SME',
        'MT': 'SME-T2T',
        'P': 'Penny/Surv',
        'R': 'Rights',
        'T': 'T2T',
        'TS': 'SME-T2T',
        'X': 'BSE-Only',
        'XT': 'BSE-Only T2T',
        'Y': 'Debt',
        'Z': 'Non-Compliant',
        'ZP': 'Non-Compliant'
    },
    'nse_cm': {
        'EQ': 'Equity',
        'BE': 'T2T',
        'BL': 'Block Deal',
        'BT': 'Physical',
        'MF': 'Mutual Fund',
        'SM': 'SME',
        'ST': 'SME-T2T',
        'TB': 'T-Bill',
        'GB': 'G-Bond',
        'GS': 'G-Sec',
        'IV': 'InvIT',
        'RR': 'Rights',
        'DR': 'Depository Rec',
        'W1': 'Warrant',
        'SF': 'Security Rec',
        'SG': 'Gold Bond',
        'BZ': 'Surveillance',
        'SZ': 'SME-Surv',
        'IT': 'InvIT',
        'D1': 'Debt',
        'E1': 'ETF',
        'P1': 'Preference'
    }
}


def get_p_group_description(exch, group):
    if not group or group == 'XX':
        return ""
    
    exch_lower = exch.lower() if exch else ""
    if exch_lower not in ('nse_cm', 'bse_cm'):
        return ""
    
    meaning = SCRIP_GROUP_MEANINGS.get(exch_lower, {}).get(group)
    if meaning:
        return meaning
    
    # Pattern matching for NSE series
    if exch_lower == 'nse_cm':
        if group.startswith(('N', 'Y', 'Z')) and len(group) == 2:
            return "NCD/Debt"
        if group.startswith('A') and len(group) == 2:
            return "Debt"
            
    return group # Fallback to code itself


def _get_scrip_data_files():
    scrip_dir = os.path.join(settings.BASE_DIR, 'trades', 'scrip_data')
    if not os.path.isdir(scrip_dir):
        raise FileNotFoundError(f"Scrip data folder not found: {scrip_dir}")

    csv_files = sorted(glob.glob(os.path.join(scrip_dir, '*.csv')))
    if not csv_files:
        return []

    target_keywords = ['nse_fo', 'bse_fo', 'nse_cm', 'bse_cm']
    matched_files = [path for path in csv_files if any(keyword in os.path.basename(path).lower() for keyword in target_keywords)]
    if matched_files:
        return matched_files

    if len(csv_files) <= 4:
        return csv_files

    return []


def _perform_scrip_cache_refresh():
    """Internal function to refresh scrip cache in DuckDB"""
    try:
        csv_files = _get_scrip_data_files()
        if not csv_files:
            logger.error("No scrip CSV files found for refresh.")
            return False, "No scrip CSV files found."

        with _duckdb_lock:
            # _duckdb_connection.execute('DROP VIEW IF EXISTS active_market_data')
            _duckdb_connection.execute('DROP TABLE IF EXISTS active_market_data')
            _duckdb_connection.execute('DROP TABLE IF EXISTS temp_market_data')
            file_list_sql = ', '.join(_quote_sql_string(path) for path in csv_files)
            _duckdb_connection.execute(
                f"CREATE TABLE temp_market_data AS SELECT * FROM read_csv([{file_list_sql}], union_by_name=True)"
            )
            
            _duckdb_connection.execute(r"""
                CREATE TABLE active_market_data AS 
                WITH option_underlyings AS (
                    SELECT DISTINCT pAssetCode 
                    FROM temp_market_data 
                    WHERE pInstType IN ('OPTIDX', 'OPTSTK', 'IO', 'SO')
                )
                SELECT t.*,
                       try_strptime(regexp_extract(COALESCE(t.pScripRefKey, ''), '(\d{2}[A-Z]{3}\d{2})', 1), '%d%b%y') as expire_date,
                       (ou.pAssetCode IS NOT NULL) as has_option_chain
                FROM temp_market_data t
                LEFT JOIN option_underlyings ou ON CAST(t.pSymbol AS VARCHAR) = CAST(ou.pAssetCode AS VARCHAR)
                WHERE try_strptime(regexp_extract(COALESCE(t.pScripRefKey, ''), '(\d{2}[A-Z]{3}\d{2})', 1), '%d%b%y') IS NULL 
                   OR try_strptime(regexp_extract(COALESCE(t.pScripRefKey, ''), '(\d{2}[A-Z]{3}\d{2})', 1), '%d%b%y') >= current_date()
            """)
            
            _duckdb_connection.execute('DROP TABLE temp_market_data')
            row_count = _duckdb_connection.execute('SELECT COUNT(*) FROM active_market_data').fetchone()[0]
            logger.info(f"Refreshed active_market_data with {row_count} active scrips.")
            return True, f"Refreshed with {row_count} scrips from {len(csv_files)} files.", row_count
    except Exception as e:
        logger.error(f"Error performing scrip cache refresh: {e}")
        return False, str(e), 0


def ensure_scrip_cache():
    """Ensure active_market_data is loaded in DuckDB"""
    try:
        with _duckdb_lock:
            # Check if table exists
            table_check = _duckdb_connection.execute("SELECT count(*) FROM information_schema.tables WHERE table_name = 'active_market_data'").fetchone()[0]
            if table_check > 0:
                count = _duckdb_connection.execute("SELECT count(*) FROM active_market_data").fetchone()[0]
                if count > 0:
                    return True
        
        # If not exists or empty, trigger refresh
        success, _, _ = _perform_scrip_cache_refresh()
        return success
    except Exception as e:
        logger.error(f"Error in ensure_scrip_cache: {e}")
        return False


# ==================== Authentication Views ====================

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
    return render(request, 'trades/login.html', context)


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
                    from django.core.mail import get_connection, EmailMessage
                    connection = get_connection(
                        host=settings_obj.host,
                        port=settings_obj.port,
                        username=settings_obj.host_user,
                        password=settings_obj.get_decrypted_password(),
                        use_tls=settings_obj.use_tls
                    )
                    from_addr = settings_obj.from_address if settings_obj.from_address else settings_obj.host_user
                    email_msg = EmailMessage(
                        subject="JK Terminal - Registration Verification",
                        body=f"Hello {user.username},\n\nYour account verification code is: {otp}\n\nPlease enter this code to complete your registration.\n\nThank you.",
                        from_email=from_addr,
                        to=[user.email],
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
        
    return render(request, 'trades/register.html', {'form': form})

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
        
    return render(request, 'trades/otp_verify.html', {'form': form})


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

@login_required_with_session_check
def setup_credentials(request):
    """Setup or update Neo API credentials"""
    try:
        user_creds = UserNeoCredentials.objects.get(user=request.user)
    except UserNeoCredentials.DoesNotExist:
        user_creds = None
    
    if request.method == 'POST':
        form = UserNeoCredentialsForm(request.POST, instance=user_creds)
        if form.is_valid():
            credentials = form.save(commit=False)
            credentials.user = request.user
            credentials.save()
            logout_sdk_for_user(request.user, request=request)
            messages.success(request, "Neo API credentials updated successfully! Please reauthenticate the trading session.")
            return redirect('index')
    else:
        form = UserNeoCredentialsForm(instance=user_creds)
    
    return render(request, 'trades/credentials.html', {'form': form, 'has_credentials': user_creds is not None})


@login_required_with_session_check
def view_credentials(request):
    """View credentials (read-only)"""
    try:
        user_creds = UserNeoCredentials.objects.get(user=request.user)
        credentials = user_creds.get_decrypted_credentials()
    except UserNeoCredentials.DoesNotExist:
        credentials = None
        user_creds = None
    
    return render(request, 'trades/view_credentials.html', {
        'credentials': credentials,
        'user_creds': user_creds
    })


@login_required_with_session_check
def reauthenticate_view(request):
    """Prompt for a one-time TOTP to establish or refresh the SDK session."""
    try:
        user_creds = UserNeoCredentials.objects.get(user=request.user, is_active=True)
    except UserNeoCredentials.DoesNotExist:
        if request.headers.get('x-requested-with') == 'XMLHttpRequest' or request.content_type == 'application/json':
            return JsonResponse({'status': 'error', 'message': "Please configure your Neo API credentials first."}, status=400)
        messages.warning(request, "Please configure your Neo API credentials first.")
        return redirect('setup_credentials')

    if request.method == 'POST':
        totp = None
        if request.headers.get('x-requested-with') == 'XMLHttpRequest' or request.content_type == 'application/json':
            try:
                data = json.loads(request.body)
                totp = data.get('totp')
            except json.JSONDecodeError:
                return JsonResponse({'status': 'error', 'message': 'Invalid JSON data'}, status=400)
        else:
            form = TOTPForm(request.POST)
            if form.is_valid():
                totp = form.cleaned_data['totp']

        if totp:
            api = KotakNeoAPI(user=request.user, session_id=request.session.session_key)
            auth_result = api.authenticate(totp=totp, force_refresh=True)
            if auth_result.get('status') == 'success':
                if request.headers.get('x-requested-with') == 'XMLHttpRequest' or request.content_type == 'application/json':
                    return JsonResponse({'status': 'success', 'message': "Neo SDK session authenticated successfully."})
                messages.success(request, "Neo SDK session authenticated successfully.")
                return redirect('index')
            
            error_msg = auth_result.get('error', 'Authentication failed.')
            if request.headers.get('x-requested-with') == 'XMLHttpRequest' or request.content_type == 'application/json':
                return JsonResponse({'status': 'error', 'message': error_msg}, status=400)
            messages.error(request, error_msg)
        else:
            if request.headers.get('x-requested-with') == 'XMLHttpRequest' or request.content_type == 'application/json':
                return JsonResponse({'status': 'error', 'message': 'TOTP is required.'}, status=400)
            form = TOTPForm(request.POST)
    else:
        form = TOTPForm()

    return render(request, 'trades/reauthenticate.html', {
        'form': form,
        'has_credentials': True,
    })


@login_required_with_session_check
def logout_sdk_session(request):
    """Force logout of the user's Neo SDK session."""
    logout_sdk_for_user(request.user, request=request)
    messages.success(request, "Neo SDK session has been logged out.")
    return redirect('profile')


@login_required_with_session_check
def edit_credentials(request):
    """Edit credentials"""
    try:
        user_creds = UserNeoCredentials.objects.get(user=request.user)
    except UserNeoCredentials.DoesNotExist:
        messages.error(request, "Please setup your credentials first.")
        return redirect('setup_credentials')
    
    if request.method == 'POST':
        form = UserNeoCredentialsForm(request.POST, instance=user_creds)
        if form.is_valid():
            form.save()
            logout_sdk_for_user(request.user, request=request)
            messages.success(request, "Credentials updated successfully! Please reauthenticate the trading session.")
            return redirect('index')
    else:
        form = UserNeoCredentialsForm(instance=user_creds)
    
    return render(request, 'trades/credentials.html', {'form': form, 'has_credentials': True})


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

    return render(request, 'trades/profile.html', {
        'form': form,
        'has_credentials': has_credentials,
        'sdk_status': sdk_status,
    })


def get_client_ip(request):
    """Extract client IP address from request"""
    x_forwarded_for = request.META.get('HTTP_X_FORWARDED_FOR')
    if x_forwarded_for:
        ip = x_forwarded_for.split(',')[0]
    else:
        ip = request.META.get('REMOTE_ADDR')
    return ip


def logout_sdk_for_user(user, request=None):
    """Logout the Kotak Neo SDK session for the given user."""
    try:
        session_id = request.session.session_key if request else None
        api = KotakNeoAPI(user=user, session_id=session_id)
        api.logout()
    except Exception as e:
        import logging
        logging.getLogger(__name__).warning(f"SDK logout failed: {e}")


# ==================== Admin Views (Protected) ====================

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
        settings_obj.host_user = request.POST.get('host_user', '')
        settings_obj.from_address = request.POST.get('from_address', '')
        
        new_password = request.POST.get('host_password', '')
        if new_password:
            settings_obj.host_password = new_password
        settings_obj.save()

        # Handle Platform Settings
        platform_settings.session_timeout_enabled = request.POST.get('session_timeout_enabled') == 'on'
        platform_settings.sdk_timeout_enabled = request.POST.get('sdk_timeout_enabled') == 'on'
        platform_settings.enable_user_registration = request.POST.get('enable_user_registration') == 'on'
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
            from django.contrib.sessions.models import Session
            from .models import SessionActivity
            from .kotak_neo_api import KotakNeoAPI
            from django.contrib.auth import logout
            
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
    registration_form = RegistrationForm()

    return render(request, 'trades/admin_settings.html', {
        'settings': settings_obj,
        'platform_settings': platform_settings,
        'users': users,
        'registration_form': registration_form
    })

# ==================== User Management Views (Superuser Only) ====================

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

def generate_temp_password(length=8):
    import random
    import string
    chars = string.ascii_letters + string.digits
    return ''.join(random.choice(chars) for _ in range(length))

def send_password_change_confirmation_email(user):
    """Send confirmation email when password is changed successfully"""
    settings_obj = SMTPSettings.get_settings()
    if not user.email or not settings_obj.host:
        return
        
    try:
        from django.core.mail import get_connection, EmailMessage
        connection = get_connection(
            host=settings_obj.host,
            port=settings_obj.port,
            username=settings_obj.host_user,
            password=settings_obj.get_decrypted_password(),
            use_tls=settings_obj.use_tls
        )
        from_addr = settings_obj.from_address if settings_obj.from_address else settings_obj.host_user
        email_msg = EmailMessage(
            subject="JK Terminal - Password Changed Successfully",
            body=f"Hello {user.username},\n\nYour password has been successfully changed.\n\nIf you did not authorize this change, please contact an administrator immediately.\n\nThank you.",
            from_email=from_addr,
            to=[user.email],
            connection=connection
        )
        email_msg.send(fail_silently=False)
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f"Error sending password confirmation email: {e}")

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
                
                # Send email FIRST to ensure no lockout on failure
                try:
                    from django.core.mail import get_connection, EmailMessage
                    connection = get_connection(
                        host=settings_obj.host,
                        port=settings_obj.port,
                        username=settings_obj.host_user,
                        password=settings_obj.get_decrypted_password(),
                        use_tls=settings_obj.use_tls
                    )
                    from_addr = settings_obj.from_address if settings_obj.from_address else settings_obj.host_user
                    email_msg = EmailMessage(
                        subject="JK Terminal - Temporary Password",
                        body=f"Hello {user.username},\n\nYour temporary password is: {temp_password}\n\nPlease login using this password. You will be asked to set a new permanent password immediately.\n\nThank you.",
                        from_email=from_addr,
                        to=[user.email],
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
        
    return render(request, 'trades/forgot_password.html', {'form': form})


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

    return render(request, 'trades/change_password.html', {
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

    return render(request, 'trades/change_password.html', {
        'form': form,
        'title': 'Change Password',
        'is_force_change': False
    })



# ==================== Trading Views (Protected) ====================

def _check_scrip_status_logic():
    """Internal logic to check if scrip master files exist and are up to date."""
    from datetime import datetime
    
    scrip_dir = os.path.join(settings.BASE_DIR, 'trades', 'scrip_data')
    
    if not os.path.exists(scrip_dir):
        return {'needs_refresh': True, 'reason': 'Directory missing'}
    
    csv_files = glob.glob(os.path.join(scrip_dir, '*.csv'))
    if not csv_files:
        return {'needs_refresh': True, 'reason': 'No files found'}
    
    now = timezone.now()
    cutoff_time = now.replace(hour=8, minute=0, second=0, microsecond=0)
    
    if now.hour < 8:
        cutoff_time = cutoff_time - timezone.timedelta(days=1)
    
    try:
        latest_mtime = max(os.path.getmtime(f) for f in csv_files)
        latest_dt = timezone.make_aware(datetime.fromtimestamp(latest_mtime))
        
        if latest_dt < cutoff_time:
            return {
                'needs_refresh': True, 
                'reason': 'Data outdated', 
                'latest_update': latest_dt.strftime('%Y-%m-%d %H:%M:%S'),
                'cutoff': cutoff_time.strftime('%Y-%m-%d %H:%M:%S')
            }
            
        with _duckdb_lock:
            table_check = _duckdb_connection.execute("SELECT count(*) FROM information_schema.tables WHERE table_name = 'active_market_data'").fetchone()[0]
            if table_check == 0:
                return {'needs_refresh': True, 'reason': 'Cache empty'}
            
            count = _duckdb_connection.execute("SELECT count(*) FROM active_market_data").fetchone()[0]
            if count == 0:
                return {'needs_refresh': True, 'reason': 'Cache empty'}
                
    except Exception as e:
        return {'needs_refresh': True, 'reason': f'Error checking files/cache: {str(e)}'}

    return {'needs_refresh': False}


@login_required_with_session_check
def check_scrip_status(request):
    """Check if scrip master files exist and are up to date (from today)."""
    return JsonResponse(_check_scrip_status_logic())


@login_required_with_session_check
def refresh_scrip_master(request):
    try:
        api = KotakNeoAPI(user=request.user, session_id=request.session.session_key)
    except Exception as e:
        return JsonResponse({'status': 'error', 'message': str(e)}, status=400)
    
    try:
        result = api.download_scrip_master()
        if result.get('status') == 'success':
            return JsonResponse({'status': 'success', 'message': f"Scrip master data downloaded successfully to {result.get('downloaded_files')}"})
        else:
            return JsonResponse({'status': 'error', 'message': result.get('error', 'An unknown error occurred.')})
    except Exception as e:
        return JsonResponse({'status': 'error', 'message': str(e)})


@login_required_with_session_check
def refresh_scrip_cache(request):
    if request.method != 'GET':
        return JsonResponse({'status': 'error', 'message': 'Only GET requests are allowed.'}, status=405)

    success, message, row_count = _perform_scrip_cache_refresh()
    if success:
        return JsonResponse({'status': 'success', 'message': message, 'row_count': row_count})
    else:
        return JsonResponse({'status': 'error', 'message': message}, status=500)


@login_required_with_session_check
def search_scrip_cache(request):
    if request.method != 'GET':
        return JsonResponse({'error': 'Only GET requests are allowed'}, status=405)

    try:
        search_term = request.GET.get('q', '').strip()
        exchange = request.GET.get('exchange', 'all')  # all, nse_cm, bse_cm, nse_fo, bse_fo
        inst_type = request.GET.get('inst_type', 'all')  # all, stock, option, future

        if not search_term or len(search_term) < 2:
            return JsonResponse({'error': 'Search term must be at least 2 characters.'}, status=400)

        with _duckdb_lock:
            try:
                row_count = _duckdb_connection.execute('SELECT COUNT(*) FROM active_market_data').fetchone()[0]
                if row_count == 0:
                    return JsonResponse({
                        'error': 'Scrip cache is empty. Please refresh the scrip cache and try again.',
                        'action': 'refresh_cache'
                    }, status=400)
            except Exception:
                return JsonResponse({
                    'error': 'Scrip cache table not found. Please refresh the scrip cache and try again.',
                    'action': 'refresh_cache'
                }, status=400)

            # Build filter conditions
            filters = []
            if exchange != 'all':
                filters.append(f"pExchSeg = '{exchange}'")

            if inst_type != 'all':
                if inst_type == 'stock':
                    filters.append("(pInstType IS NULL OR pInstType = '')")
                elif inst_type == 'option':
                    filters.append("(pInstType IN ('OPTSTK', 'OPTIDX'))")
                elif inst_type == 'future':
                    filters.append("(pInstType IN ('FUTIDX', 'FUTSTK'))")

            where_clause = " AND ".join(filters) if filters else "1=1"

            # Build elastic search: make it tighter by requiring more matches
            search_terms = search_term.lower().split()
            
            # Escape single quotes in search terms
            safe_terms = [term.replace("'", "''") for term in search_terms]
            
            # Build conditions for options/futures: search only in pScripRefKey (AND logic)
            fno_conditions = []
            for term in safe_terms:
                fno_conditions.append(f"LOWER(COALESCE(pScripRefKey, '')) LIKE '%{term}%'")
            fno_search = " AND ".join(fno_conditions) if fno_conditions else "1=1"
            
            # Build conditions for stocks: search in pScripRefKey OR pDesc (OR logic for terms)
            stock_conditions = []
            for term in safe_terms:
                stock_conditions.append(f"""
                    (LOWER(COALESCE(pScripRefKey, '')) LIKE '%{term}%'
                    OR LOWER(COALESCE(pDesc, '')) LIKE '%{term}%')
                """)
            stock_search = " OR ".join(stock_conditions) if stock_conditions else "1=1"
            
            # Combine: prioritize F&O search if looking for options/futures or when exchange is F&O, otherwise use stock search
            if inst_type in ('option', 'future') or exchange in ('nse_fo', 'bse_fo'):
                final_search = f"({fno_search})"
            else:
                # For stocks or non-F&O search, use stock search (looser)
                final_search = f"({stock_search})"

            first_term = safe_terms[0] if safe_terms else ''
            
            # Check if user likely wants F&O based on month abbreviations in search
            months = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']
            has_date_term = any(m in term for term in search_terms for m in months)
            wants_fo = inst_type in ('option', 'future') or exchange in ('nse_fo', 'bse_fo') or has_date_term
            
            stock_priority_clause = "expire_date IS NOT NULL ASC," if not wants_fo else ""
            
            order_by_clause = f"""
                ORDER BY 
                    -- 1. Exact prefix match gets top priority
                    CASE 
                        WHEN LOWER(COALESCE(pSymbolName, '')) LIKE '{first_term}%' THEN 0 
                        WHEN LOWER(COALESCE(pScripRefKey, '')) LIKE '{first_term}%' THEN 1
                        ELSE 2 
                    END ASC,
                    -- 2. Equity prioritization if no FO intent
                    {stock_priority_clause}
                    -- 3. Sort by Nearest Expiry Date (Options/Futures)
                    expire_date ASC NULLS LAST,
                    -- 4. Strike price sorting (closest to 0 or sequential)
                    dStrikePrice ASC,
                    -- 5. Finally alphabetical
                    pSymbolName ASC,
                    pScripRefKey ASC
            """

            query = f"""
                SELECT 
                    pSymbol,
                    pExchSeg,
                    pSymbolName,
                    pTrdSymbol,
                    pOptionType,
                    pInstType,
                    CAST(COALESCE("dStrikePrice;", 0) AS DECIMAL) / 100 as dStrikePrice,
                    pScripRefKey,
                    pDesc,
                    COALESCE(pGroup, '') as pGroup,
                    COALESCE(CAST(pAssetCode AS VARCHAR), '') as pAssetCode,
                    has_option_chain,
                    CAST(COALESCE(dTickSize, dTickSize, 0) AS DECIMAL) / 100 as dTickSize,
                    CAST(COALESCE(lLotSize, 0) AS INTEGER) as lLotSize
                FROM active_market_data
                WHERE {where_clause} AND {final_search}
                {order_by_clause}
                LIMIT 50
            """

            results = _duckdb_connection.execute(query).fetchall()
            columns = ['pSymbol', 'pExchSeg', 'pSymbolName', 'pTrdSymbol', 'pOptionType', 'pInstType', 'dStrikePrice', 'pScripRefKey', 'pDesc', 'pGroup', 'pAssetCode', 'has_option_chain', 'dTickSize', 'lLotSize']
            
            data = [dict(zip(columns, row)) for row in results]
            
            # Add pGroup description
            for item in data:
                item['pGroupDesc'] = get_p_group_description(item.get('pExchSeg'), item.get('pGroup'))
            
            logger.info(f"DuckDB DB Scrip search execution for '{search_term}' using filters (exchange: {exchange}, inst_type: {inst_type}) returned {len(data)} results.")
            
            return JsonResponse({
                'results': data,
                'count': len(data),
                'total_available': min(50, len(data))
            })

    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)


@login_required_with_session_check
def get_scrip_info_ajax(request):
    token = request.GET.get('token')
    exch = request.GET.get('exch')
    if not token or not exch:
        return JsonResponse({'error': 'Missing token or exchange'}, status=400)

    try:
        query = f"""
            SELECT 
                pSymbol, pExchSeg, pSymbolName, pTrdSymbol, pOptionType, pInstType,
                CAST(COALESCE("dStrikePrice;", 0) AS DECIMAL) / 100 as dStrikePrice,
                pScripRefKey, pDesc,
                COALESCE(pGroup, '') as pGroup,
                COALESCE(CAST(pAssetCode AS VARCHAR), '') as pAssetCode,
                has_option_chain,
                CAST(COALESCE(dTickSize, 0) AS DECIMAL) / 100 as dTickSize,
                CAST(COALESCE(lLotSize, 0) AS INTEGER) as lLotSize
            FROM active_market_data
            WHERE pSymbol = '{token}' AND pExchSeg = '{exch}'
            LIMIT 1
        """
        with _duckdb_lock:
            results = _duckdb_connection.execute(query).fetchall()
        
        if not results:
            return JsonResponse({'error': 'Scrip not found in cache'}, status=404)
        
        columns = ['pSymbol', 'pExchSeg', 'pSymbolName', 'pTrdSymbol', 'pOptionType', 'pInstType', 'dStrikePrice', 'pScripRefKey', 'pDesc', 'pGroup', 'pAssetCode', 'has_option_chain', 'dTickSize', 'lLotSize']
        data = dict(zip(columns, results[0]))
        data['pGroupDesc'] = get_p_group_description(data.get('pExchSeg'), data.get('pGroup'))
        
        return JsonResponse({'status': 'success', 'data': data})
    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)


@login_required_with_session_check
def get_option_chain_ajax(request):
    p_symbol = request.GET.get('p_symbol')
    if not p_symbol:
        return JsonResponse({'error': 'Missing p_symbol'}, status=400)

    try:
        query = f"""
            SELECT 
                pSymbol, pExchSeg, pSymbolName, pTrdSymbol, pOptionType, pInstType,
                CAST(COALESCE("dStrikePrice;", 0) AS DECIMAL) / 100 as dStrikePrice,
                pScripRefKey, pDesc,
                CAST(COALESCE(dTickSize, 0) AS DECIMAL) / 100 as dTickSize,
                CAST(COALESCE(lLotSize, 0) AS INTEGER) as lLotSize,
                strftime(expire_date, '%Y-%m-%d') as expire_date_str
            FROM active_market_data
            WHERE CAST(pAssetCode AS VARCHAR) = '{p_symbol}' 
              AND pInstType IN ('OPTIDX', 'OPTSTK', 'IO', 'SO')
            ORDER BY expire_date, dStrikePrice
        """
        
        with _duckdb_lock:
            results = _duckdb_connection.execute(query).fetchall()
        
        columns = ['pSymbol', 'pExchSeg', 'pSymbolName', 'pTrdSymbol', 'pOptionType', 'pInstType', 'dStrikePrice', 'pScripRefKey', 'pDesc', 'dTickSize', 'lLotSize', 'expire_date_str']
        raw_data = [dict(zip(columns, row)) for row in results]
        
        # Group by expiry and strike
        chain_data = {}
        expiries = []
        
        for row in raw_data:
            exp = row['expire_date_str']
            strike = row['dStrikePrice']
            opt_type = row['pOptionType']
            
            if exp not in chain_data:
                chain_data[exp] = {}
                expiries.append(exp)
            
            if strike not in chain_data[exp]:
                chain_data[exp][strike] = {'CE': None, 'PE': None}
            
            if opt_type == 'CE':
                chain_data[exp][strike]['CE'] = row
            elif opt_type == 'PE':
                chain_data[exp][strike]['PE'] = row
        
        # Convert strikes to sorted list for each expiry
        final_chain = {}
        for exp in expiries:
            sorted_strikes = []
            for strike in sorted(chain_data[exp].keys()):
                strike_row = chain_data[exp][strike]
                strike_row['strike'] = float(strike)
                sorted_strikes.append(strike_row)
            final_chain[exp] = sorted_strikes

        return JsonResponse({
            'status': 'success',
            'expiries': expiries,
            'chain': final_chain
        })
    except Exception as e:
        logger.error(f"Error fetching option chain: {e}")
        return JsonResponse({'error': str(e)}, status=500)


@ajax_login_required
def place_trade_ajax(request):
    if request.method != 'POST':
        return JsonResponse({'error': 'Only POST requests are allowed'}, status=405)

    try:
        data = json.loads(request.body)
        instrument_token = data.get('instrument_token')
        trading_symbol = data.get('trading_symbol')
        quantity = data.get('quantity')
        price = data.get('price')  # Optional for market orders
        transaction_type = data.get('transaction_type')
        exchange_segment = data.get('exchange_segment')
        product_type = data.get('product_type')
        order_type = data.get('order_type', 'L')  # Default to limit

        if not all([instrument_token, trading_symbol, quantity, transaction_type, exchange_segment, product_type]):
            return JsonResponse({'error': 'Required fields are missing.'}, status=400)

        logger.info(f"User '{request.user.username}' attempting to place {transaction_type} trade for {quantity} of {trading_symbol} ({order_type}).")

        if order_type == 'MKT':
            price = 0
        elif price is None or price == '':
            return JsonResponse({'error': 'Price is required for limit orders.'}, status=400)

        api = KotakNeoAPI(user=request.user, session_id=request.session.session_key)
        margin_response = api.margin_required(
            instrument_token=instrument_token,
            quantity=quantity,
            price=0 if order_type == 'MKT' else price,
            transaction_type=transaction_type,
            exchange_segment=exchange_segment,
            product=product_type,
            order_type=order_type
        )

        if isinstance(margin_response, dict) and 'error' in margin_response:
            if 'One-time TOTP code is required' in margin_response['error']:
                return JsonResponse({'status': 'reauth_required', 'message': 'Trade session expired. Please reauthenticate.'}, status=401)
            return JsonResponse({'status': 'error', 'message': f"Margin check failed: {margin_response['error']}"}, status=400)

        margin_data = margin_response.get('data', {}) if isinstance(margin_response, dict) else {}
        insuf_fund = float(margin_data.get('insufFund', '0') or '0')
        rms_validated = str(margin_data.get('rmsVldtd', '')).upper()

        if insuf_fund > 0 or rms_validated != 'OK':
            message = f"Insufficient margin. Required: {margin_data.get('reqdMrgn', '0')}, Available: {margin_data.get('avlMrgn', '0')}."
            if margin_data.get('insufFund'):
                message += f" Add ₹{margin_data.get('insufFund')} and try again."
            return JsonResponse({'status': 'error', 'message': message, 'margin': margin_data}, status=400)

        api_response = api.place_trade(
            trading_symbol=trading_symbol,
            quantity=int(quantity),
            price=float(price),
            transaction_type=transaction_type,
            exchange_segment=exchange_segment,
            product=product_type,
            order_type=order_type
        )

        if isinstance(api_response, dict) and 'error' in api_response:
            if 'One-time TOTP code is required' in api_response['error']:
                return JsonResponse({'status': 'reauth_required', 'message': 'Trade session expired. Please reauthenticate.'}, status=401)
            return JsonResponse({'status': 'error', 'message': api_response['error']}, status=400)

        if 'errMsg' in api_response:
            logger.warning(f"Trade failed for '{request.user.username}': {api_response['errMsg']}")
            return JsonResponse({'status': 'error', 'message': api_response['errMsg']}, status=400)
        
        order_id = api_response.get('nOrdNo', 'N/A')
        logger.info(f"Trade placed successfully for '{request.user.username}'. Order ID: {order_id}")
        return JsonResponse({'status': 'success', 'message': f"Trade placed successfully! Order ID: {order_id}", 'data': api_response})

    except (json.JSONDecodeError, ValueError, TypeError) as e:
        return JsonResponse({'error': f"Invalid request data: {e}"}, status=400)
    except Exception as e:
        return JsonResponse({'error': f"An unexpected error occurred: {e}"}, status=500)


@ajax_login_required
def check_margin_ajax(request):
    if request.method != 'POST':
        return JsonResponse({'error': 'Only POST requests are allowed'}, status=405)

    try:
        data = json.loads(request.body)
        instrument_token = data.get('instrument_token')
        quantity = data.get('quantity')
        price = data.get('price')
        transaction_type = data.get('transaction_type')
        exchange_segment = data.get('exchange_segment')
        product_type = data.get('product_type')
        order_type = data.get('order_type', 'L')

        if not all([instrument_token, quantity, transaction_type, exchange_segment, product_type]):
            return JsonResponse({'error': 'Required fields are missing.'}, status=400)

        if order_type == 'MKT':
            price = 0
        elif price is None or price == '':
            return JsonResponse({'error': 'Price is required for limit orders.'}, status=400)

        api = KotakNeoAPI(user=request.user, session_id=request.session.session_key)
        margin_response = api.margin_required(
            instrument_token=instrument_token,
            quantity=quantity,
            price=0 if order_type == 'MKT' else price,
            transaction_type=transaction_type,
            exchange_segment=exchange_segment,
            product=product_type,
            order_type=order_type
        )

        if isinstance(margin_response, dict) and 'error' in margin_response:
            if 'One-time TOTP code is required' in margin_response['error']:
                return JsonResponse({'status': 'reauth_required', 'message': 'Trade session expired. Please reauthenticate.'}, status=401)
            return JsonResponse({'error': f"Margin check failed: {margin_response['error']}"}, status=400)

        return JsonResponse({'status': 'success', 'data': margin_response.get('data', margin_response)})

    except (json.JSONDecodeError, ValueError, TypeError) as e:
        return JsonResponse({'error': f"Invalid request data: {e}"}, status=400)
    except Exception as e:
        return JsonResponse({'error': f"An unexpected error occurred: {e}"}, status=500)


@ajax_login_required
def cancel_order_ajax(request):
    if request.method != 'POST':
        return JsonResponse({'error': 'Only POST requests are allowed'}, status=405)

    try:
        data = json.loads(request.body)
        order_id = data.get('order_id')

        if not order_id:
            return JsonResponse({'error': 'Order ID is required.'}, status=400)

        logger.info(f"User '{request.user.username}' requesting cancellation of order ID: {order_id}")

        api = KotakNeoAPI(user=request.user, session_id=request.session.session_key)
        api_response = api.cancel_order(order_id)
        
        if isinstance(api_response, dict) and 'error' in api_response:
            if 'One-time TOTP code is required' in api_response['error']:
                return JsonResponse({'status': 'reauth_required', 'message': 'Trade session expired. Please reauthenticate.'}, status=401)
            return JsonResponse({'status': 'error', 'message': api_response['error']}, status=400)
        
        if 'errMsg' in api_response:
            return JsonResponse({'status': 'error', 'message': api_response['errMsg']}, status=400)
        
        return JsonResponse({'status': 'success', 'message': f"Order cancellation requested: {api_response.get('result', 'Success')} - {api_response.get('stat', 'Success')}"})

    except (json.JSONDecodeError, ValueError, TypeError) as e:
        return JsonResponse({'error': f"Invalid request data: {e}"}, status=400)
    except Exception as e:
        return JsonResponse({'error': f"An unexpected error occurred: {e}"}, status=500)


@login_required_with_session_check
def search_scrips_ajax(request):
    if request.method != 'GET':
        return JsonResponse({'error': 'Only GET requests are allowed'}, status=405)

    symbol = request.GET.get('symbol', '')
    exchange_segment = request.GET.get('exchange_segment', 'nse_cm')

    if not symbol:
        return JsonResponse({'error': 'Symbol is required.'}, status=400)

    try:
        api = KotakNeoAPI(user=request.user, session_id=request.session.session_key)
    except Exception as e:
        return JsonResponse({'error': str(e)}, status=400)
    
    logger.debug(f"User '{request.user.username}' searching scrips for symbol '{symbol}' in {exchange_segment}.")
    results = api.search_scrip(exchange_segment=exchange_segment, symbol=symbol)

    if 'error' in results:
        return JsonResponse({'error': results['error']}, status=400)

    return JsonResponse(results, safe=False)


@login_required_with_session_check
def get_depth(request):
    if request.method != 'GET':
        return JsonResponse({'error': 'Only GET requests are allowed'}, status=405)

    p_symbol = request.GET.get('p_symbol', '')
    p_exch_seg = request.GET.get('p_exch_seg', '')

    if not p_symbol or not p_exch_seg:
        return JsonResponse({'error': 'p_symbol and p_exch_seg are required.'}, status=400)

    try:
        api = KotakNeoAPI(user=request.user, session_id=request.session.session_key)
    except Exception as e:
        return JsonResponse({'error': str(e)}, status=400)
    
    instrument_tokens = [{"instrument_token": p_symbol, "exchange_segment": p_exch_seg}]
    result = api.quotes(instrument_tokens=instrument_tokens, quote_type="all")

    if 'error' in result:
        return JsonResponse({'error': result['error']}, status=400)

    # The result is a list with one item
    if isinstance(result, list) and len(result) > 0:
        quote = result[0]
        depth_data = {
            'ltp': quote.get('ltp'),
            'buy_depth': quote.get('depth', {}).get('buy', []),
            'sell_depth': quote.get('depth', {}).get('sell', [])
        }
        logger.info(f"Successfully retrieved static depth and LTP ({quote.get('ltp')}) from SDK for target {p_symbol} ({p_exch_seg}).")
        return JsonResponse(depth_data)
    else:
        return JsonResponse({'error': 'No depth data received'}, status=400)


@login_required_with_session_check
def get_ltp(request):
    if request.method != 'GET':
        return JsonResponse({'error': 'Only GET requests are allowed'}, status=405)

    p_symbol = request.GET.get('p_symbol', '')
    p_exch_seg = request.GET.get('p_exch_seg', '')

    if not p_symbol or not p_exch_seg:
        return JsonResponse({'error': 'p_symbol and p_exch_seg are required.'}, status=400)

    try:
        api = KotakNeoAPI(user=request.user, session_id=request.session.session_key)
    except Exception as e:
        return JsonResponse({'error': str(e)}, status=400)

    instrument_tokens = [{"instrument_token": p_symbol, "exchange_segment": p_exch_seg}]
    result = api.quotes(instrument_tokens=instrument_tokens, quote_type="all")

    if 'error' in result:
        return JsonResponse({'error': result['error']}, status=400)

    if isinstance(result, list) and len(result) > 0:
        quote = result[0]
        return JsonResponse({
            'ltp': quote.get('ltp'),
            'lower_circuit': quote.get('low_price_range'),
            'upper_circuit': quote.get('high_price_range')
        })

    return JsonResponse({'error': 'No quote data received'}, status=400)


@login_required_with_session_check
def index(request):
    """Main trading dashboard - requires authentication"""
    api_response = None
    logger.info(f"User '{request.user.username}' loading trading dashboard.")
    
    # Scrip cache is now handled asynchronously via check_scrip_status and the frontend modal
    
    # Check if user has credentials setup
    try:
        user_creds = UserNeoCredentials.objects.get(user=request.user, is_active=True)
    except UserNeoCredentials.DoesNotExist:
        messages.warning(request, "Please configure your Neo API credentials to start trading.")
        return redirect('setup_credentials')

    try:
        session_activity = SessionActivity.objects.get(session_key=request.session.session_key)
        sdk_active = session_activity.is_sdk_session_valid()
    except SessionActivity.DoesNotExist:
        sdk_active = False

    if not sdk_active:
        messages.warning(request, "Your Neo SDK session is not active or has expired. Please reauthenticate.")

    try:
        api = KotakNeoAPI(user=request.user, session_id=request.session.session_key)
    except Exception as e:
        messages.error(request, f"Error initializing API: {str(e)}")
        return redirect('setup_credentials')

    sdk_expires_at = None
    if sdk_active:
        session_info = api.get_cached_session()
        if session_info and session_info.get('expires_at'):
            sdk_expires_at = session_info['expires_at'].isoformat()

    if request.method == 'POST':
        if 'cancel_order_id' in request.POST:
            order_id = request.POST.get('cancel_order_id')
            if sdk_active:
                api_response = api.cancel_order(order_id)
                if 'error' in api_response:
                    messages.error(request, f"Cancellation failed: {api_response['error']}")
                else:
                    messages.success(request, f"Order cancellation requested: {api_response.get('result', 'Success')}")
            else:
                messages.warning(request, "Cannot cancel orders because the Neo SDK session is not active. Please reauthenticate.")

    # Fetch account information, holdings, limits, and order book for display.
    account_info = {}
    holdings = []
    raw_limits = {}
    order_book = []
    positions = []

    if sdk_active:
        # These methods will trigger authentication on the first call if the SDK session is available.
        account_info = api.get_account_info()
        holdings = api.get_holdings()
        raw_limits = api.get_limits()
        order_book = api.get_order_book()
        positions = api.get_positions()
    else:
        messages.info(request, "SDK session is inactive. Use the Reauthenticate link in your profile to restore trading access.")

    # Handle potential errors from the API calls to prevent page crashes
    if 'error' in account_info:
        messages.warning(request, f"Could not fetch account info: {account_info['error']}")
        account_info = {} # Reset to avoid template errors
    
    processed_holdings, portfolio_summary = _process_holdings_data(holdings, request)
    processed_positions = _process_positions_data(positions, request)
    limits, debug_limits = _process_limits_data(raw_limits, request)

    if isinstance(order_book, dict) and 'error' in order_book:
        messages.warning(request, f"Could not fetch order book: {order_book['error']}")
        order_book = [] # Reset to avoid template errors

    show_restore_modal = request.session.pop('show_restore_modal', False)
    if show_restore_modal:
        request.session.modified = True

    context = {
        'api_response': api_response,
        'account_info': account_info,
        'holdings': processed_holdings,
        'positions': processed_positions,
        'limits': limits,
        'order_book': order_book,
        'portfolio_summary': portfolio_summary,
        'debug_limits': debug_limits,
        'sdk_active': sdk_active,
        'is_connected': True if account_info and 'error' not in account_info else False,
        'platform_settings': PlatformSettings.get_settings(),
        'show_restore_modal': show_restore_modal,
        'sdk_expires_at': sdk_expires_at,
    }

    return render(request, 'trades/index.html', context)


@login_required_with_session_check
def extend_sdk_session_ajax(request):
    """Extend the Kotak Neo SDK Session via AJAX."""
    if request.method != 'POST':
        return JsonResponse({'status': 'error', 'message': 'Invalid request method'}, status=405)
        
    try:
        api = KotakNeoAPI(user=request.user, session_id=request.session.session_key)
        success, result = api.extend_session()
        if success:
            return JsonResponse({'status': 'success', 'new_expires_at': result})
        else:
            return JsonResponse({'status': 'error', 'message': result}, status=400)
    except Exception as e:
        return JsonResponse({'status': 'error', 'message': str(e)}, status=500)


def _process_holdings_data(holdings, request=None):
    """Helper to process raw holdings data from SDK"""
    processed_holdings = []
    portfolio_summary = {
        'total_invested': 0,
        'current_value': 0,
        'total_pnl': 0,
        'pnl_percentage': 0
    }
    
    if isinstance(holdings, dict) and 'error' in holdings:
        if request:
            messages.warning(request, f"Could not fetch holdings: {holdings['error']}")
        return [], portfolio_summary

    if isinstance(holdings, list):
        for h in holdings:
            if not isinstance(h, dict):
                continue

            try:
                qty = float(h.get('quantity', 0))
                avg_price = float(h.get('averagePrice', 0))
                last_price = float(h.get('closingPrice', 0))
                mkt_value = float(h.get('mktValue', 0))
                holding_cost = float(h.get('holdingCost', 0))

                pnl = mkt_value - holding_cost
                
                portfolio_summary['total_invested'] += holding_cost
                portfolio_summary['current_value'] += mkt_value

                processed_holdings.append({
                    'symbol': h.get('symbol', 'N/A'),
                    'tradingsymbol': h.get('displaySymbol', h.get('symbol', 'N/A')),
                    'instrument_token': str(h.get('exchangeIdentifier', h.get('instrumentToken', ''))),
                    'exchange_segment': h.get('exchangeSegment', 'nse_cm'),
                    'quantity': qty,
                    'average_price': avg_price,
                    'last_price': last_price,
                    'pnl': pnl,
                })
            except (ValueError, TypeError):
                continue
        
        portfolio_summary['total_pnl'] = portfolio_summary['current_value'] - portfolio_summary['total_invested']
        if portfolio_summary['total_invested'] > 0:
            portfolio_summary['pnl_percentage'] = (portfolio_summary['total_pnl'] / portfolio_summary['total_invested']) * 100
            
    return processed_holdings, portfolio_summary


def _process_positions_data(positions, request=None):
    """Helper to process raw positions data from SDK"""
    processed_positions = []
    
    if isinstance(positions, dict) and 'error' in positions:
        if request:
            messages.warning(request, f"Could not fetch positions: {positions['error']}")
        return []

    if isinstance(positions, list):
        for p in positions:
            if not isinstance(p, dict):
                continue
            
            try:
                cf_buy = float(p.get('cfBuyQty', 0))
                cf_sell = float(p.get('cfSellQty', 0))
                fl_buy = float(p.get('flBuyQty', 0))
                fl_sell = float(p.get('flSellQty', 0))
                
                qty = (cf_buy + fl_buy) - (cf_sell + fl_sell)
                
                buy_amt = float(p.get('buyAmt', 0)) + float(p.get('cfBuyAmt', 0))
                buy_qty = cf_buy + fl_buy
                avg_price = buy_amt / buy_qty if buy_qty > 0 else 0
                
                ltp = float(p.get('upldPrc', 0)) or avg_price
                pnl = (ltp - avg_price) * qty
                
                processed_positions.append({
                    'trdSym': p.get('trdSym', 'N/A'),
                    'token': str(p.get('tok', '')),
                    'exchange': p.get('exSeg', 'nse_cm'),
                    'qty': int(qty),
                    'avgPrc': avg_price,
                    'ltp': ltp,
                    'pnl': pnl,
                    'dayPnl': 0, 
                    'multiplier': float(p.get('multiplier', 1)),
                })
            except (ValueError, TypeError, ZeroDivisionError):
                continue
    return processed_positions


def _process_limits_data(raw_limits, request=None):
    """Helper to process raw limits data from SDK"""
    limits = {}
    debug_limits = raw_limits
    
    if isinstance(raw_limits, dict) and 'error' in raw_limits:
        if request:
            messages.warning(request, f"Could not fetch limits: {raw_limits['error']}")
        return {}, raw_limits

    if isinstance(raw_limits, dict) and raw_limits.get('stat') == 'Ok':
        limits = {
            'available_trade': raw_limits.get('Net', '0.00'),
            'margin_used': raw_limits.get('MarginUsed', '0.00'),
            'collateral': raw_limits.get('CollateralValue', '0.00'),
            'total_cash': raw_limits.get('RmsPayInAmt', '0.00'),
            'unsettled_credit': raw_limits.get('CncSellcrdPresent', '0.00'), 
        }
    return limits, debug_limits


@ajax_login_required
def get_order_book_ajax(request):
    """AJAX view to fetch order book"""
    try:
        api = KotakNeoAPI(user=request.user, session_id=request.session.session_key)
        order_book = api.get_order_book()
        
        if isinstance(order_book, dict) and 'error' in order_book:
            return JsonResponse({'status': 'error', 'message': order_book['error']}, status=400)
            
        return JsonResponse({'status': 'success', 'data': order_book})
    except Exception as e:
        return JsonResponse({'status': 'error', 'message': str(e)}, status=500)


@ajax_login_required
def get_holdings_ajax(request):
    """AJAX view to fetch holdings"""
    try:
        api = KotakNeoAPI(user=request.user, session_id=request.session.session_key)
        holdings = api.get_holdings()
        processed_holdings, portfolio_summary = _process_holdings_data(holdings)
        
        return JsonResponse({
            'status': 'success', 
            'data': processed_holdings,
            'summary': portfolio_summary
        })
    except Exception as e:
        return JsonResponse({'status': 'error', 'message': str(e)}, status=500)


@ajax_login_required
def get_positions_ajax(request):
    """AJAX view to fetch positions"""
    try:
        api = KotakNeoAPI(user=request.user, session_id=request.session.session_key)
        positions = api.get_positions()
        processed_positions = _process_positions_data(positions)
        
        return JsonResponse({'status': 'success', 'data': processed_positions})
    except Exception as e:
        return JsonResponse({'status': 'error', 'message': str(e)}, status=500)


@ajax_login_required
def get_limits_ajax(request):
    """AJAX view to fetch financial limits"""
    try:
        api = KotakNeoAPI(user=request.user, session_id=request.session.session_key)
        raw_limits = api.get_limits()
        limits, debug_limits = _process_limits_data(raw_limits)
        
        return JsonResponse({'status': 'success', 'data': limits})
    except Exception as e:
        return JsonResponse({'status': 'error', 'message': str(e)}, status=500)

@ajax_login_required
def check_sdk_status(request):
    """Check if the SDK is authenticated for the current session."""
    api = KotakNeoAPI(user=request.user, session_id=request.session.session_key)
    # Check cache directly to avoid any heavy authenticate() calls
    is_hot = api.get_cached_session() is not None
    return JsonResponse({
        "status": "success",
        "is_authenticated": is_hot
    })
