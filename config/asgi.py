"""
ASGI config for trading_platform project.

It exposes the ASGI callable as a module-level variable named ``application``.

For more information on this file, see
https://docs.djangoproject.com/en/5.0/howto/deployment/asgi/
"""

import os
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')

from channels.routing import ProtocolTypeRouter, URLRouter
from channels.auth import AuthMiddlewareStack
from django.core.asgi import get_asgi_application
# Import routing later

# Settings already set above

# Initialize Django ASGI application early to ensure the AppRegistry
# is populated before importing code that may import ORM models.
django_asgi_app = get_asgi_application()

from django.contrib.staticfiles.handlers import ASGIStaticFilesHandler
from trades import routing

class ForceASGIStaticFilesHandler(ASGIStaticFilesHandler):
    def __init__(self, application):
        super().__init__(application)
        self.insecure_serving = True

from trades.api.middleware import TokenAuthOrSessionAuthMiddleware

application = ProtocolTypeRouter({
    "http": django_asgi_app,
    "websocket": TokenAuthOrSessionAuthMiddleware(
        URLRouter(
            routing.websocket_urlpatterns
        )
    ),
})

application = ForceASGIStaticFilesHandler(application)


