from django.urls import path
from . import views

urlpatterns = [
    # Authentication URLs
    path('login/', views.login_view, name='login'),
    path('register/', views.register_view, name='register'),
    path('logout/', views.logout_view, name='logout'),
    
    # Credentials Management URLs
    path('credentials/setup/', views.setup_credentials, name='setup_credentials'),
    path('credentials/view/', views.view_credentials, name='view_credentials'),
    path('credentials/edit/', views.edit_credentials, name='edit_credentials'),
    
    # Profile URL
    path('profile/', views.profile_view, name='profile'),
    
    # Trading URLs (Protected)
    path('', views.index, name='index'),
    path('place_trade_ajax/', views.place_trade_ajax, name='place_trade_ajax'),
    path('check_margin_ajax/', views.check_margin_ajax, name='check_margin_ajax'),
    path('cancel_order_ajax/', views.cancel_order_ajax, name='cancel_order_ajax'),
    path('search_scrips_ajax/', views.search_scrips_ajax, name='search_scrips_ajax'),
    path('search_scrip_cache/', views.search_scrip_cache, name='search_scrip_cache'),
    path('refresh_scrip_master/', views.refresh_scrip_master, name='refresh_scrip_master'),
    path('refresh_scrip_cache/', views.refresh_scrip_cache, name='refresh_scrip_cache'),
    path('get_depth/', views.get_depth, name='get_depth'),
    path('get_ltp/', views.get_ltp, name='get_ltp'),
    path('reauthenticate/', views.reauthenticate_view, name='reauthenticate'),
    path('logout_sdk/', views.logout_sdk_session, name='logout_sdk'),
]
