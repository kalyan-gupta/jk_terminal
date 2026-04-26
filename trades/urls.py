from django.urls import path
from . import views, views_basket

urlpatterns = [
    # Authentication URLs
    path('login/', views.login_view, name='login'),
    path('register/', views.register_view, name='register'),
    path('logout/', views.logout_view, name='logout'),
    path('extend_session/', views.extend_session, name='extend_session'),
    path('forgot-password/', views.forgot_password_view, name='forgot_password'),
    path('set-new-password/', views.set_new_password_view, name='set_new_password'),
    path('change-password/', views.change_password_view, name='change_password'),
    path('verify-email/', views.otp_verify_view, name='otp_verify'),
    
    # Credentials Management URLs
    path('credentials/setup/', views.setup_credentials, name='setup_credentials'),
    path('credentials/view/', views.view_credentials, name='view_credentials'),
    path('credentials/edit/', views.edit_credentials, name='edit_credentials'),
    
    # Profile URL
    path('profile/', views.profile_view, name='profile'),
    
    # Admin Options
    path('admin-settings/', views.admin_settings_view, name='admin_settings'),
    path('admin-settings/user/<int:user_id>/toggle/', views.admin_toggle_superuser, name='admin_toggle_superuser'),
    path('admin-settings/user/<int:user_id>/delete/', views.admin_delete_user, name='admin_delete_user'),
    path('admin-settings/user/<int:user_id>/reset-password/', views.admin_reset_user_password, name='admin_reset_user_password'),
    path('admin-settings/user/add/', views.admin_add_user_view, name='admin_add_user'),
    
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
    path('get_option_chain_ajax/', views.get_option_chain_ajax, name='get_option_chain_ajax'),
    path('reauthenticate/', views.reauthenticate_view, name='reauthenticate'),
    path('logout_sdk/', views.logout_sdk_session, name='logout_sdk'),
    path('check_sdk_status/', views.check_sdk_status, name='check_sdk_status'),
    
    # Basket URLs
    path('basket/add/', views_basket.add_to_basket_ajax, name='add_to_basket_ajax'),
    path('basket/get/', views_basket.get_basket_ajax, name='get_basket_ajax'),
    path('basket/remove/', views_basket.remove_from_basket_ajax, name='remove_from_basket_ajax'),
    path('basket/clear/', views_basket.clear_basket_ajax, name='clear_basket_ajax'),
    path('basket/update_sequence/', views_basket.update_basket_sequence_ajax, name='update_basket_sequence_ajax'),
    path('basket/update_item/', views_basket.update_basket_item_ajax, name='update_basket_item_ajax'),
    path('basket/execute/', views_basket.execute_basket_ajax, name='execute_basket_ajax'),
]
