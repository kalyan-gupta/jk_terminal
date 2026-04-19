import time
import uuid
import logging
from trading_platform.logging_utils import request_id_var, request_user_var

logger = logging.getLogger('trading_platform.requests')

logger = logging.getLogger('trading_platform.requests')

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
                if 'password' in post_params:
                    post_params['password'] = '***'
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
                # Server has restarted, clear the session
                logout(request)
                messages.info(request, "The server has restarted. Please login again.")
                return redirect('login')

        return self.get_response(request)
