from django.shortcuts import render, redirect
from django.contrib.auth import authenticate, login, logout
from django.contrib.auth.models import User
from django.contrib import messages
from django.http import JsonResponse
from django.conf import settings
from django.contrib.sessions.models import Session
from django.db import connections, transaction
import json
import csv
import re
import datetime
import glob
import os
import threading
import logging
from django.utils import timezone

from ..kotak_neo_api import KotakNeoAPI
from ..models import UserNeoCredentials, SessionActivity, SMTPSettings, UserSecurity, PlatformSettings, ActiveMarketData
from ..forms import LoginForm, RegistrationForm, UserNeoCredentialsForm, UserProfileForm, TOTPForm, ForgotPasswordForm, SetNewPasswordForm, ChangePasswordForm, OTPVerifyForm
from ..decorators import login_required_with_session_check, ajax_login_required
from .helpers import _process_holdings_data, _process_positions_data, _process_limits_data, logout_sdk_for_user

logger = logging.getLogger(__name__)


@login_required_with_session_check
def index(request):
    """Main trading dashboard - requires authentication"""
    api_response = None
    logger.info(f"User '{request.user.username}' loading trading dashboard.")
    
    # Scrip cache is now handled asynchronously via check_scrip_status and the frontend modal
    
    # Check if user has credentials setup
    try:
        user_creds = UserNeoCredentials.objects.get(user=request.user, is_active=True)
    except UserNeoCredentials.DoesNotExist:
        messages.warning(request, "Please configure your Neo API credentials to start trading.")
        return redirect('setup_credentials')

    try:
        api = KotakNeoAPI(user=request.user, session_id=request.session.session_key)
    except Exception as e:
        messages.error(request, f"Error initializing API: {str(e)}")
        return redirect('setup_credentials')

    try:
        session_activity = SessionActivity.objects.get(session_key=request.session.session_key)
        sdk_active = session_activity.is_sdk_session_valid()
    except SessionActivity.DoesNotExist:
        sdk_active = False

    if not sdk_active:
        # Try to auto-authenticate if using direct secret auth
        plat_settings = PlatformSettings.get_settings()
        if user_creds.auth_mode == 'secret' and plat_settings.allow_direct_secret_auth and not request.session.get('sdk_auto_auth_failed'):
            auth_result = api.authenticate(force_refresh=True)
            if auth_result.get('status') == 'success':
                sdk_active = True
            else:
                request.session['sdk_auto_auth_failed'] = True

    if not sdk_active:
        messages.warning(request, "Your Neo SDK session is not active or has expired. Please reauthenticate.")

    sdk_expires_at = None
    if sdk_active:
        session_info = api.get_cached_session()
        if session_info and session_info.get('expires_at'):
            sdk_expires_at = session_info['expires_at'].isoformat()

    if request.method == 'POST':
        if 'cancel_order_id' in request.POST:
            order_id = request.POST.get('cancel_order_id')
            if sdk_active:
                api_response = api.cancel_order(order_id)
                if 'error' in api_response:
                    messages.error(request, f"Cancellation failed: {api_response['error']}")
                else:
                    messages.success(request, f"Order cancellation requested: {api_response.get('result', 'Success')}")
            else:
                messages.warning(request, "Cannot cancel orders because the Neo SDK session is not active. Please reauthenticate.")

    # Fetch account information, holdings, limits, and order book for display.
    account_info = {}
    holdings = []
    raw_limits = {}
    order_book = []
    positions = []

    if sdk_active:
        # These methods will trigger authentication on the first call if the SDK session is available.
        account_info = api.get_account_info()
        holdings = api.get_holdings()
        raw_limits = api.get_limits()
        order_book = api.get_order_book()
        positions = api.get_positions()
    else:
        messages.info(request, "SDK session is inactive. Use the Reauthenticate link in your profile to restore trading access.")

    # Handle potential errors from the API calls to prevent page crashes
    if 'error' in account_info:
        messages.warning(request, f"Could not fetch account info: {account_info['error']}")
        account_info = {} # Reset to avoid template errors
    
    processed_holdings, portfolio_summary = _process_holdings_data(holdings, request)
    processed_positions = _process_positions_data(positions, request)
    limits, debug_limits = _process_limits_data(raw_limits, request)

    if isinstance(order_book, dict) and 'error' in order_book:
        messages.warning(request, f"Could not fetch order book: {order_book['error']}")
        order_book = [] # Reset to avoid template errors

    show_restore_modal = request.session.pop('show_restore_modal', False)
    if show_restore_modal:
        request.session.modified = True

    context = {
        'api_response': api_response,
        'account_info': account_info,
        'holdings': processed_holdings,
        'positions': processed_positions,
        'limits': limits,
        'order_book': order_book,
        'portfolio_summary': portfolio_summary,
        'debug_limits': debug_limits,
        'sdk_active': sdk_active,
        'is_connected': True if account_info and 'error' not in account_info else False,
        'platform_settings': PlatformSettings.get_settings(),
        'show_restore_modal': show_restore_modal,
        'sdk_expires_at': sdk_expires_at,
    }

    return render(request, 'index.html', context)


@login_required_with_session_check
def reauthenticate_view(request):
    """Prompt for a one-time TOTP to establish or refresh the SDK session."""
    try:
        user_creds = UserNeoCredentials.objects.get(user=request.user, is_active=True)
    except UserNeoCredentials.DoesNotExist:
        if request.headers.get('x-requested-with') == 'XMLHttpRequest' or request.content_type == 'application/json':
            return JsonResponse({'status': 'error', 'message': "Please configure your Neo API credentials first."}, status=400)
        messages.warning(request, "Please configure your Neo API credentials first.")
        return redirect('setup_credentials')

    plat_settings = PlatformSettings.get_settings()

    # If user has direct secret activation enabled and active, and it is allowed globally, try auto-authenticating on GET
    if request.method == 'GET' and user_creds.auth_mode == 'secret' and plat_settings.allow_direct_secret_auth:
        api = KotakNeoAPI(user=request.user, session_id=request.session.session_key)
        auth_result = api.authenticate(force_refresh=True)
        if auth_result.get('status') == 'success':
            if 'sdk_auto_auth_failed' in request.session:
                del request.session['sdk_auto_auth_failed']
            messages.success(request, "Neo SDK session authenticated automatically using saved TOTP secret.")
            return redirect('index')
        else:
            request.session['sdk_auto_auth_failed'] = True
            messages.warning(request, f"Automatic session activation failed: {auth_result.get('error', 'Unknown error')}. Please authenticate manually.")

    if request.method == 'POST':
        totp = None
        if request.headers.get('x-requested-with') == 'XMLHttpRequest' or request.content_type == 'application/json':
            try:
                data = json.loads(request.body)
                totp = data.get('totp')
            except json.JSONDecodeError:
                return JsonResponse({'status': 'error', 'message': 'Invalid JSON data'}, status=400)
        else:
            form = TOTPForm(request.POST)
            if form.is_valid():
                totp = form.cleaned_data['totp']

        if totp:
            api = KotakNeoAPI(user=request.user, session_id=request.session.session_key)
            auth_result = api.authenticate(totp=totp, force_refresh=True)
            if auth_result.get('status') == 'success':
                if 'sdk_auto_auth_failed' in request.session:
                    del request.session['sdk_auto_auth_failed']
                if request.headers.get('x-requested-with') == 'XMLHttpRequest' or request.content_type == 'application/json':
                    return JsonResponse({'status': 'success', 'message': "Neo SDK session authenticated successfully."})
                messages.success(request, "Neo SDK session authenticated successfully.")
                return redirect('index')
            
            error_msg = auth_result.get('error', 'Authentication failed.')
            if request.headers.get('x-requested-with') == 'XMLHttpRequest' or request.content_type == 'application/json':
                return JsonResponse({'status': 'error', 'message': error_msg}, status=400)
            messages.error(request, error_msg)
        else:
            if request.headers.get('x-requested-with') == 'XMLHttpRequest' or request.content_type == 'application/json':
                return JsonResponse({'status': 'error', 'message': 'TOTP is required.'}, status=400)
            form = TOTPForm(request.POST)
    else:
        form = TOTPForm()

    return render(request, 'reauthenticate.html', {
        'form': form,
        'has_credentials': True,
    })


@login_required_with_session_check
def extend_sdk_session_ajax(request):
    """Extend the Kotak Neo SDK Session via AJAX."""
    if request.method != 'POST':
        return JsonResponse({'status': 'error', 'message': 'Invalid request method'}, status=405)
        
    try:
        api = KotakNeoAPI(user=request.user, session_id=request.session.session_key)
        success, result = api.extend_session()
        if success:
            return JsonResponse({'status': 'success', 'new_expires_at': result})
        else:
            return JsonResponse({'status': 'error', 'message': result}, status=400)
    except Exception as e:
        return JsonResponse({'status': 'error', 'message': str(e)}, status=500)


@login_required_with_session_check
def logout_sdk_session(request):
    """Force logout of the user's Neo SDK session."""
    logout_sdk_for_user(request.user, request=request)
    messages.success(request, "Neo SDK session has been logged out.")
    return redirect('profile')


@ajax_login_required
def check_sdk_status(request):
    """Check if the SDK is authenticated for the current session."""
    api = KotakNeoAPI(user=request.user, session_id=request.session.session_key)
    # Check cache directly to avoid any heavy authenticate() calls
    is_hot = api.get_cached_session() is not None
    
    # If not authenticated but we have TOTP secret authentication enabled, try to authenticate automatically
    if not is_hot and not request.session.get('sdk_auto_auth_failed'):
        from trades.models import PlatformSettings, UserNeoCredentials
        plat_settings = PlatformSettings.get_settings()
        try:
            user_creds = UserNeoCredentials.objects.get(user=request.user, is_active=True)
            if user_creds.auth_mode == 'secret' and plat_settings.allow_direct_secret_auth:
                auth_result = api.authenticate(force_refresh=True)
                is_hot = auth_result.get('status') == 'success'
                if not is_hot:
                    request.session['sdk_auto_auth_failed'] = True
        except UserNeoCredentials.DoesNotExist:
            pass
            
    return JsonResponse({
        "status": "success",
        "is_authenticated": is_hot
    })


