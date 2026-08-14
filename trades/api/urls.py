from django.urls import path, include
from rest_framework.routers import DefaultRouter
from rest_framework_simplejwt.views import TokenObtainPairView, TokenRefreshView
from .views import (
    RegisterAPIView, UserProfileAPIView, UserNeoCredentialsAPIView,
    CheckSDKStatusAPIView, AuthenticateSDKAPIView, ExtendSDKSessionAPIView,
    LogoutSDKAPIView, OrderBookAPIView, CheckMarginAPIView,
    HoldingsAPIView, PositionsAPIView, LimitsAPIView,
    BasketOrderViewSet, ExecuteBasketAPIView, MarketSearchAPIView, LTPAPIView,
    DepthAPIView, OptionChainAPIView, ScripInfoAPIView, RefreshScripMasterAPIView,
    RefreshScripCacheAPIView
)

router = DefaultRouter()
router.register(r'basket', BasketOrderViewSet, basename='basket')

urlpatterns = [
    # JWT Authentication
    path('auth/login/', TokenObtainPairView.as_view(), name='token_obtain_pair'),
    path('auth/token/refresh/', TokenRefreshView.as_view(), name='token_refresh'),
    path('auth/register/', RegisterAPIView.as_view(), name='api_register'),
    
    # Profile & API Credentials
    path('profile/', UserProfileAPIView.as_view(), name='api_profile'),
    path('credentials/', UserNeoCredentialsAPIView.as_view(), name='api_credentials'),
    
    # Kotak SDK session management
    path('sdk/status/', CheckSDKStatusAPIView.as_view(), name='api_sdk_status'),
    path('sdk/authenticate/', AuthenticateSDKAPIView.as_view(), name='api_sdk_authenticate'),
    path('sdk/extend/', ExtendSDKSessionAPIView.as_view(), name='api_sdk_extend'),
    path('sdk/logout/', LogoutSDKAPIView.as_view(), name='api_sdk_logout'),
    
    # Order management
    path('orders/', OrderBookAPIView.as_view(), name='api_orders'),
    path('orders/check-margin/', CheckMarginAPIView.as_view(), name='api_check_margin'),
    
    # Portfolio
    path('holdings/', HoldingsAPIView.as_view(), name='api_holdings'),
    path('positions/', PositionsAPIView.as_view(), name='api_positions'),
    path('limits/', LimitsAPIView.as_view(), name='api_limits'),
    
    # Basket executions & router URLs
    path('', include(router.urls)),
    path('basket/execute/', ExecuteBasketAPIView.as_view(), name='api_basket_execute'),
    
    # Market & scrip discovery
    path('market/search/', MarketSearchAPIView.as_view(), name='api_market_search'),
    path('market/ltp/', LTPAPIView.as_view(), name='api_market_ltp'),
    path('market/depth/', DepthAPIView.as_view(), name='api_market_depth'),
    path('market/options/', OptionChainAPIView.as_view(), name='api_market_options'),
    path('market/scrip/', ScripInfoAPIView.as_view(), name='api_market_scrip'),
    path('market/refresh-master/', RefreshScripMasterAPIView.as_view(), name='api_market_refresh_master'),
    path('market/refresh-cache/', RefreshScripCacheAPIView.as_view(), name='api_market_refresh_cache'),
]

