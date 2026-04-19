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
        
        # Log the beginning of the request
        logger.info(f"Request Started: {request.method} {request.path}")
        
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
