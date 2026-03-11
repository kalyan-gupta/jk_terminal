from django.urls import path
from . import views

urlpatterns = [
    path('', views.index, name='index'),
    path('place_trade_ajax/', views.place_trade_ajax, name='place_trade_ajax'),
    path('search_scrips_ajax/', views.search_scrips_ajax, name='search_scrips_ajax'),
    path('refresh_scrip_master/', views.refresh_scrip_master, name='refresh_scrip_master'),
]
