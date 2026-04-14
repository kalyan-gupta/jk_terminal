from django.utils import timezone
from django.contrib.auth.models import AnonymousUser
from trades.models import SessionActivity
import logging

logger = logging.getLogger(__name__)


class SessionExpiryMiddleware:
    """
    Middleware to handle session expiry based on inactivity.
    Checks if user's last activity is older than SESSION_COOKIE_AGE.
    """
    
    def __init__(self, get_response):
        self.get_response = get_response
        self.SESSION_TIMEOUT = 300  # 5 minutes in seconds
    
    def __call__(self, request):
        # Check if user is authenticated and has a session
        if request.user.is_authenticated:
            try:
                # Get or create session activity record
                session_activity, created = SessionActivity.objects.get_or_create(
                    user=request.user,
                    defaults={'session_key': request.session.session_key, 'ip_address': self.get_client_ip(request)}
                )
                
                # Check if session has expired
                if session_activity.is_expired(self.SESSION_TIMEOUT):
                    # Session expired, logout SDK session and flush it
                    from trades.kotak_neo_api import logout_sdk_session_for_user
                    logout_sdk_session_for_user(request.user)
                    request.session.flush()
                    request.user = AnonymousUser()
                    logger.info(f"Session expired for user {session_activity.user.username}")
                else:
                    # Update last activity
                    session_activity.save()
            except Exception as e:
                logger.error(f"Error in SessionExpiryMiddleware: {e}")
        
        response = self.get_response(request)
        return response
    
    @staticmethod
    def get_client_ip(request):
        """Extract client IP from request"""
        x_forwarded_for = request.META.get('HTTP_X_FORWARDED_FOR')
        if x_forwarded_for:
            ip = x_forwarded_for.split(',')[0]
        else:
            ip = request.META.get('REMOTE_ADDR')
        return ip
