from rest_framework import permissions
from rest_framework.exceptions import PermissionDenied, AuthenticationFailed
from django.utils import timezone
from trades.models import SessionActivity
from trades.kotak_neo_api import logout_sdk_session_for_user

class IsSessionValidAndPasswordChangeNotRequired(permissions.BasePermission):
    """
    Custom permission to:
    1. Check if user is authenticated.
    2. Check if a password change is forced (security.force_password_change).
    3. Check if the user's API session activity has expired due to inactivity.
    """

    def has_permission(self, request, view):
        # 1. Check authentication
        if not request.user or not request.user.is_authenticated:
            return False

        # 2. Check force password change
        if getattr(request.user, 'security', None) and request.user.security.force_password_change:
            raise PermissionDenied({
                'error': 'Password change required',
                'code': 'password_change_required'
            }, code='password_change_required')

        # 3. Check session activity
        session_id = f"api_{request.user.username}"
        ip_address = self.get_client_ip(request)
        
        # Check if the view is exempt from blocking on session inactivity
        from trades.api.views import AuthenticateSDKAPIView, CheckSDKStatusAPIView, LogoutSDKAPIView
        is_exempt_from_inactivity_block = isinstance(view, (AuthenticateSDKAPIView, CheckSDKStatusAPIView, LogoutSDKAPIView))
        
        try:
            # Fetch existing session activity for the API
            activity = SessionActivity.objects.get(session_key=session_id)
            
            # Check if it has expired
            if activity.is_expired():
                # Session expired, logout SDK session
                logout_sdk_session_for_user(request.user)
                if not is_exempt_from_inactivity_block:
                    # Raise AuthenticationFailed to trigger a 401 Unauthorized response
                    raise AuthenticationFailed({
                        'error': 'Session expired',
                        'expired': True,
                        'reauth_required': True
                    }, code='session_expired')
            
            # If not expired (or if exempt), update last_activity and ip_address
            activity.last_activity = timezone.now()
            activity.ip_address = ip_address
            activity.save(update_fields=['last_activity', 'ip_address'])

        except SessionActivity.DoesNotExist:
            # If it doesn't exist, create it (new session start)
            SessionActivity.objects.create(
                user=request.user,
                session_key=session_id,
                ip_address=ip_address
            )

        return True

    @staticmethod
    def get_client_ip(request):
        x_forwarded_for = request.META.get('HTTP_X_FORWARDED_FOR')
        if x_forwarded_for:
            return x_forwarded_for.split(',')[0]
        return request.META.get('REMOTE_ADDR', '0.0.0.0')
