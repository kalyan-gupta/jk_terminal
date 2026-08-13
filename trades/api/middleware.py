from urllib.parse import parse_qs
from django.contrib.auth.models import AnonymousUser, User
from rest_framework_simplejwt.tokens import AccessToken
from channels.db import database_sync_to_async
from channels.routing import URLRouter
from channels.auth import AuthMiddlewareStack

@database_sync_to_async
def get_user_from_token(token_key):
    try:
        token = AccessToken(token_key)
        user_id = token['user_id']
        return User.objects.get(id=user_id)
    except Exception:
        return AnonymousUser()

@database_sync_to_async
def get_or_create_api_session_activity(user, session_key):
    from trades.models import SessionActivity
    activity, created = SessionActivity.objects.get_or_create(
        user=user,
        session_key=session_key,
        defaults={'ip_address': '0.0.0.0'}
    )
    return activity

class TokenAuthOrSessionAuthMiddleware:
    """
    Middleware that uses JWT token if present in query string,
    otherwise falls back to standard AuthMiddlewareStack.
    """
    def __init__(self, url_router):
        self.url_router = url_router
        self.session_auth = AuthMiddlewareStack(self.url_router)

    async def __call__(self, scope, receive, send):
        query_string = scope.get('query_string', b'').decode()
        query_params = parse_qs(query_string)
        token = query_params.get('token')
        
        if token:
            user = await get_user_from_token(token[0])
            scope['user'] = user
            if user and not isinstance(user, AnonymousUser):
                session_key = f"api_{user.username}"
                scope['api_session_key'] = session_key
                # Ensure SessionActivity record exists in the DB for KotakNeoAPI handler
                await get_or_create_api_session_activity(user, session_key)
            # Bypass session stack and run the router directly
            return await self.url_router(scope, receive, send)
        else:
            # Fall back to cookie-based session auth
            return await self.session_auth(scope, receive, send)
