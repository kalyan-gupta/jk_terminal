from functools import wraps
from django.contrib.auth.decorators import login_required as django_login_required
from django.shortcuts import redirect
from django.urls import reverse
from trades.models import SessionActivity
from django.contrib.auth.models import AnonymousUser
import logging

logger = logging.getLogger(__name__)


def login_required_with_session_check(view_func):
    """
    Decorator that checks both authentication and session expiry.
    Redirects to login if user is not authenticated or session has expired.
    """
    @wraps(view_func)
    def wrapped_view(request, *args, **kwargs):
        # Check if user is authenticated
        if not request.user.is_authenticated:
            return redirect(reverse('login') + f'?next={request.path}')
        
        # Check session activity/expiry
        try:
            session_activity = SessionActivity.objects.get(user=request.user)
            if session_activity.is_expired(timeout_seconds=300):  # 5 minutes
                # Session expired
                from trades.kotak_neo_api import logout_sdk_session_for_user
                logout_sdk_session_for_user(request.user)
                request.session.flush()
                request.user = AnonymousUser()
                logger.info(f"Session expired for user {session_activity.user.username} at {request.path}")
                return redirect(reverse('login') + '?expired=true' + f'&next={request.path}')
        except SessionActivity.DoesNotExist:
            # Create session activity record if it doesn't exist
            SessionActivity.objects.create(
                user=request.user,
                session_key=request.session.session_key,
                ip_address=get_client_ip(request)
            )
        except Exception as e:
            logger.error(f"Error checking session activity: {e}")
        
        # Call the original view
        return view_func(request, *args, **kwargs)
    
    return wrapped_view


def get_client_ip(request):
    """Extract client IP address from request"""
    x_forwarded_for = request.META.get('HTTP_X_FORWARDED_FOR')
    if x_forwarded_for:
        ip = x_forwarded_for.split(',')[0]
    else:
        ip = request.META.get('REMOTE_ADDR')
    return ip


def ajax_login_required(view_func):
    """
    Decorator for AJAX views that require authentication.
    Returns JSON response instead of redirecting.
    """
    @wraps(view_func)
    def wrapped_view(request, *args, **kwargs):
        from django.http import JsonResponse
        
        if not request.user.is_authenticated:
            return JsonResponse({'error': 'Authentication required'}, status=401)
        
        # Check session activity/expiry
        try:
            session_activity = SessionActivity.objects.get(user=request.user)
            if session_activity.is_expired(timeout_seconds=300):  # 5 minutes
                from trades.kotak_neo_api import logout_sdk_session_for_user
                logout_sdk_session_for_user(request.user)
                request.session.flush()
                request.user = AnonymousUser()
                return JsonResponse({'error': 'Session expired', 'expired': True}, status=401)
        except SessionActivity.DoesNotExist:
            SessionActivity.objects.create(
                user=request.user,
                session_key=request.session.session_key,
                ip_address=get_client_ip(request)
            )
        except Exception as e:
            logger.error(f"Error checking session activity: {e}")
        
        return view_func(request, *args, **kwargs)
    
    return wrapped_view
