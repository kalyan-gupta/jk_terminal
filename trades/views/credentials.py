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
from .helpers import logout_sdk_for_user

logger = logging.getLogger(__name__)


@login_required_with_session_check
def setup_credentials(request):
    """Setup or update Neo API credentials"""
    try:
        user_creds = UserNeoCredentials.objects.get(user=request.user)
    except UserNeoCredentials.DoesNotExist:
        user_creds = None
    
    plat_settings = PlatformSettings.get_settings()
    
    if request.method == 'POST':
        form = UserNeoCredentialsForm(request.POST, instance=user_creds)
        if form.is_valid():
            credentials = form.save(commit=False)
            credentials.user = request.user
            credentials.save()
            logout_sdk_for_user(request.user, request=request)
            if 'sdk_auto_auth_failed' in request.session:
                del request.session['sdk_auto_auth_failed']
            messages.success(request, "Neo API credentials updated successfully! Please reauthenticate the trading session.")
            return redirect('index')
    else:
        form = UserNeoCredentialsForm(instance=user_creds)
    
    return render(request, 'credentials.html', {
        'form': form, 
        'has_credentials': user_creds is not None,
        'allow_direct_secret_auth': plat_settings.allow_direct_secret_auth
    })


@login_required_with_session_check
def view_credentials(request):
    """View credentials (read-only)"""
    try:
        user_creds = UserNeoCredentials.objects.get(user=request.user)
        credentials = user_creds.get_decrypted_credentials()
    except UserNeoCredentials.DoesNotExist:
        credentials = None
        user_creds = None
    
    return render(request, 'view_credentials.html', {
        'credentials': credentials,
        'user_creds': user_creds
    })


@login_required_with_session_check
def edit_credentials(request):
    """Edit credentials"""
    try:
        user_creds = UserNeoCredentials.objects.get(user=request.user)
    except UserNeoCredentials.DoesNotExist:
        messages.error(request, "Please setup your credentials first.")
        return redirect('setup_credentials')
    
    plat_settings = PlatformSettings.get_settings()
    
    if request.method == 'POST':
        form = UserNeoCredentialsForm(request.POST, instance=user_creds)
        if form.is_valid():
            form.save()
            logout_sdk_for_user(request.user, request=request)
            if 'sdk_auto_auth_failed' in request.session:
                del request.session['sdk_auto_auth_failed']
            messages.success(request, "Credentials updated successfully! Please reauthenticate the trading session.")
            return redirect('index')
    else:
        form = UserNeoCredentialsForm(instance=user_creds)
    
    return render(request, 'credentials.html', {
        'form': form, 
        'has_credentials': True,
        'allow_direct_secret_auth': plat_settings.allow_direct_secret_auth
    })


