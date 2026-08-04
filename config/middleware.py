import time
import uuid
import logging
from config.logging_utils import request_id_var, request_user_var

logger = logging.getLogger('config.requests')

class RequestLoggingMiddleware:
    """
    Middleware to assign a unique UUID to each request, capture the authenticated user, 
    and log the duration and status. Placed after AuthenticationMiddleware.
    """
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        # Generate a unique request ID
        req_id = str(uuid.uuid4())
        
        # Determine the user
        user_name = '-'
        if hasattr(request, 'user'):
            if request.user.is_authenticated:
                user_name = request.user.username
            else:
                user_name = "Anonymous"
        
        # Set the variables in the contextvar for this execution flow
        token_id = request_id_var.set(req_id)
        token_user = request_user_var.set(user_name)
        
        start_time = time.time()
        
        # Extract params securely
        query_params = dict(request.GET.items())
        post_params = {}
        if request.method in ['POST', 'PUT', 'PATCH']:
            try:
                post_params = dict(request.POST.items())
                sensitive_keys = ['password', 'password1', 'password2', 'mpin', 'consumer_key', 'host_password', 'new_password', 'current_password', 'totp']
                for key in sensitive_keys:
                    if key in post_params:
                        post_params[key] = '***'
            except Exception:
                pass # Don't crash if body is unparseable raw json
                
        # Log the beginning of the request
        logger.info(f"Request Started: {request.method} {request.path} | GET: {query_params} | POST: {post_params}")
        
        try:
            # Process the request
            response = self.get_response(request)
            duration = time.time() - start_time
            
            # Log the successful completion
            logger.info(f"Request Finished: {request.method} {request.path} - Status: {response.status_code} - Duration: {duration:.3f}s")
            
            return response
            
        except Exception as e:
            duration = time.time() - start_time
            # Log an error if the request failed before rendering a response
            logger.error(f"Request Failed: {request.method} {request.path} - Exception: {e} - Duration: {duration:.3f}s")
            raise
            
        finally:
            # Always reset the context variable back to its previous state
            request_id_var.reset(token_id)
            request_user_var.reset(token_user)

class RestartDetectionMiddleware:
    """
    Middleware to detect if the server has restarted and force users to re-login.
    """
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        from django.conf import settings
        from django.contrib.auth import logout
        from django.shortcuts import redirect
        from django.contrib import messages

        # Skip for login/logout views to avoid redirect loops
        exempt_paths = [settings.LOGIN_URL, '/logout/', '/register/']
        if any(request.path.startswith(p) for p in exempt_paths):
            return self.get_response(request)

        if request.user.is_authenticated:
            session_boot_id = request.session.get('server_boot_id')
            if session_boot_id != settings.SERVER_BOOT_ID:
                from trades.models import PlatformSettings
                platform_settings = PlatformSettings.get_settings()
                if platform_settings.allow_session_restore:
                    # Update the boot ID immediately to prevent duplicate triggers
                    request.session['server_boot_id'] = settings.SERVER_BOOT_ID
                    
                    # Only show the "Restored" modal if:
                    # 1. There WAS a previous boot ID (it's a restore, not a fresh login)
                    # 2. It's a full page GET request (not an AJAX call or asset)
                    is_ajax = request.headers.get('x-requested-with') == 'XMLHttpRequest' or 'application/json' in request.headers.get('Accept', '')
                    if session_boot_id and request.method == 'GET' and not is_ajax:
                        request.session['show_restore_modal'] = True
                    
                    request.session.modified = True
                else:
                    # Server has restarted, clear the session
                    from trades.decorators import is_ajax
                    from django.http import JsonResponse
                    
                    if is_ajax(request):
                        logout(request)
                        return JsonResponse({'error': 'Server restarted', 'reauth_required': True}, status=401)
                        
                    logout(request)
                    messages.info(request, "The server has restarted. Please login again.")
                    return redirect('login')

        return self.get_response(request)
