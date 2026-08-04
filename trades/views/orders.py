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
from ..models import UserNeoCredentials, SessionActivity, SMTPSettings, UserSecurity, PlatformSettings, ActiveMarketData, TrackedOrder
from ..forms import LoginForm, RegistrationForm, UserNeoCredentialsForm, UserProfileForm, TOTPForm, ForgotPasswordForm, SetNewPasswordForm, ChangePasswordForm, OTPVerifyForm
from ..decorators import login_required_with_session_check, ajax_login_required
from .helpers import _process_holdings_data, _process_positions_data, _process_limits_data

logger = logging.getLogger(__name__)


@ajax_login_required
def place_trade_ajax(request):
    if request.method != 'POST':
        return JsonResponse({'error': 'Only POST requests are allowed'}, status=405)

    try:
        data = json.loads(request.body)
        instrument_token = data.get('instrument_token')
        trading_symbol = data.get('trading_symbol')
        quantity = data.get('quantity')
        price = data.get('price')  # Optional for market orders
        transaction_type = data.get('transaction_type')
        exchange_segment = data.get('exchange_segment')
        product_type = data.get('product_type')
        order_type = data.get('order_type', 'L')  # Default to limit

        if not all([instrument_token, trading_symbol, quantity, transaction_type, exchange_segment, product_type]):
            return JsonResponse({'error': 'Required fields are missing.'}, status=400)

        logger.info(f"User '{request.user.username}' attempting to place {transaction_type} trade for {quantity} of {trading_symbol} ({order_type}).")

        if order_type == 'MKT':
            price = 0
        elif price is None or price == '':
            return JsonResponse({'error': 'Price is required for limit orders.'}, status=400)

        api = KotakNeoAPI(user=request.user, session_id=request.session.session_key)
        margin_response = api.margin_required(
            instrument_token=instrument_token,
            quantity=quantity,
            price=0 if order_type == 'MKT' else price,
            transaction_type=transaction_type,
            exchange_segment=exchange_segment,
            product=product_type,
            order_type=order_type
        )

        if isinstance(margin_response, dict) and 'error' in margin_response:
            if 'One-time TOTP code is required' in margin_response['error']:
                return JsonResponse({'status': 'reauth_required', 'message': 'Trade session expired. Please reauthenticate.'}, status=401)
            return JsonResponse({'status': 'error', 'message': f"Margin check failed: {margin_response['error']}"}, status=400)

        margin_data = margin_response.get('data', margin_response) if isinstance(margin_response, dict) else {}
        insuf_fund = float(margin_data.get('insufFund', '0') or '0')
        rms_validated = str(margin_data.get('rmsVldtd', '')).upper()

        if insuf_fund > 0 or rms_validated != 'OK':
            message = f"Insufficient margin. Required: {margin_data.get('reqdMrgn', '0')}, Available: {margin_data.get('avlMrgn', '0')}."
            if margin_data.get('insufFund'):
                message += f" Add ₹{margin_data.get('insufFund')} and try again."
            return JsonResponse({'status': 'error', 'message': message, 'margin': margin_data}, status=400)

        api_response = api.place_trade(
            trading_symbol=trading_symbol,
            quantity=int(quantity),
            price=float(price),
            transaction_type=transaction_type,
            exchange_segment=exchange_segment,
            product=product_type,
            order_type=order_type
        )

        if isinstance(api_response, dict) and 'error' in api_response:
            if 'One-time TOTP code is required' in api_response['error']:
                return JsonResponse({'status': 'reauth_required', 'message': 'Trade session expired. Please reauthenticate.'}, status=401)
            return JsonResponse({'status': 'error', 'message': api_response['error']}, status=400)

        if 'errMsg' in api_response:
            logger.warning(f"Trade failed for '{request.user.username}': {api_response['errMsg']}")
            return JsonResponse({'status': 'error', 'message': api_response['errMsg']}, status=400)
        order_id = api_response.get('nOrdNo', 'N/A')
        logger.info(f"Trade placed successfully for '{request.user.username}'. Order ID: {order_id}")
        return JsonResponse({'status': 'success', 'message': f"Trade placed successfully! Order ID: {order_id}", 'data': api_response})

    except (json.JSONDecodeError, ValueError, TypeError) as e:
        return JsonResponse({'error': f"Invalid request data: {e}"}, status=400)
    except Exception as e:
        return JsonResponse({'error': f"An unexpected error occurred: {e}"}, status=500)


@ajax_login_required
def check_margin_ajax(request):
    if request.method != 'POST':
        return JsonResponse({'error': 'Only POST requests are allowed'}, status=405)

    try:
        data = json.loads(request.body)
        instrument_token = data.get('instrument_token')
        quantity = data.get('quantity')
        price = data.get('price')
        transaction_type = data.get('transaction_type')
        exchange_segment = data.get('exchange_segment')
        product_type = data.get('product_type')
        order_type = data.get('order_type', 'L')

        if not all([instrument_token, quantity, transaction_type, exchange_segment, product_type]):
            return JsonResponse({'error': 'Required fields are missing.'}, status=400)

        if order_type == 'MKT':
            price = 0
        elif price is None or price == '':
            return JsonResponse({'error': 'Price is required for limit orders.'}, status=400)

        api = KotakNeoAPI(user=request.user, session_id=request.session.session_key)
        margin_response = api.margin_required(
            instrument_token=instrument_token,
            quantity=quantity,
            price=0 if order_type == 'MKT' else price,
            transaction_type=transaction_type,
            exchange_segment=exchange_segment,
            product=product_type,
            order_type=order_type
        )

        if isinstance(margin_response, dict) and 'error' in margin_response:
            if 'One-time TOTP code is required' in margin_response['error']:
                return JsonResponse({'status': 'reauth_required', 'message': 'Trade session expired. Please reauthenticate.'}, status=401)
            return JsonResponse({'error': f"Margin check failed: {margin_response['error']}"}, status=400)

        return JsonResponse({'status': 'success', 'data': margin_response.get('data', margin_response)})

    except (json.JSONDecodeError, ValueError, TypeError) as e:
        return JsonResponse({'error': f"Invalid request data: {e}"}, status=400)
    except Exception as e:
        return JsonResponse({'error': f"An unexpected error occurred: {e}"}, status=500)


@ajax_login_required
def cancel_order_ajax(request):
    if request.method != 'POST':
        return JsonResponse({'error': 'Only POST requests are allowed'}, status=405)

    try:
        data = json.loads(request.body)
        order_id = data.get('order_id')

        if not order_id:
            return JsonResponse({'error': 'Order ID is required.'}, status=400)

        logger.info(f"User '{request.user.username}' requesting cancellation of order ID: {order_id}")

        api = KotakNeoAPI(user=request.user, session_id=request.session.session_key)
        api_response = api.cancel_order(order_id)
        
        if isinstance(api_response, dict) and 'error' in api_response:
            if 'One-time TOTP code is required' in api_response['error']:
                return JsonResponse({'status': 'reauth_required', 'message': 'Trade session expired. Please reauthenticate.'}, status=401)
            return JsonResponse({'status': 'error', 'message': api_response['error']}, status=400)
        
        if 'errMsg' in api_response:
            return JsonResponse({'status': 'error', 'message': api_response['errMsg']}, status=400)
        
        return JsonResponse({'status': 'success', 'message': f"Order cancellation requested: {api_response.get('result', 'Success')} - {api_response.get('stat', 'Success')}"})

    except (json.JSONDecodeError, ValueError, TypeError) as e:
        return JsonResponse({'error': f"Invalid request data: {e}"}, status=400)
    except Exception as e:
        return JsonResponse({'error': f"An unexpected error occurred: {e}"}, status=500)


@ajax_login_required
def modify_order_ajax(request):
    if request.method != 'POST':
        return JsonResponse({'error': 'Only POST requests are allowed'}, status=405)

    try:
        data = json.loads(request.body)
        order_id = data.get('order_id')
        quantity = data.get('quantity')
        price = data.get('price')
        order_type = data.get('order_type', 'L')
        trading_symbol = data.get('trading_symbol')
        exchange_segment = data.get('exchange_segment')
        transaction_type = data.get('transaction_type')
        product_type = data.get('product_type')

        if not all([order_id, quantity, price, trading_symbol, transaction_type, exchange_segment, product_type]):
            return JsonResponse({'error': 'Required fields are missing.'}, status=400)

        logger.info(f"User '{request.user.username}' requesting modification of order ID: {order_id} (New Qty: {quantity}, New Price: {price})")

        api = KotakNeoAPI(user=request.user, session_id=request.session.session_key)
        api_response = api.modify_order(
            order_id=order_id,
            quantity=int(quantity),
            price=float(price),
            trading_symbol=trading_symbol,
            transaction_type=transaction_type,
            exchange_segment=exchange_segment,
            product=product_type,
            order_type=order_type
        )

        if isinstance(api_response, dict) and 'error' in api_response:
            if 'One-time TOTP code is required' in api_response['error']:
                return JsonResponse({'status': 'reauth_required', 'message': 'Trade session expired. Please reauthenticate.'}, status=401)
            return JsonResponse({'status': 'error', 'message': api_response['error']}, status=400)

        if 'errMsg' in api_response:
            return JsonResponse({'status': 'error', 'message': api_response['errMsg']}, status=400)

        # Update local tracking order
        try:
            tracked_order = TrackedOrder.objects.get(order_id=str(order_id))
            tracked_order.quantity = int(quantity)
            tracked_order.price = float(price)
            tracked_order.order_type = order_type
            tracked_order.save()
            logger.info(f"Updated TrackedOrder {order_id} with new parameters (qty: {quantity}, prc: {price}, type: {order_type})")
        except TrackedOrder.DoesNotExist:
            logger.warning(f"No TrackedOrder found for modified order {order_id}")

        # Send immediate email notification for modification
        try:
            smtp_settings = SMTPSettings.get_settings()
            if smtp_settings.enable_order_notifications and smtp_settings.host and smtp_settings.host_user and request.user.email:
                from django.core.mail import get_connection
                connection = get_connection(
                    host=smtp_settings.host,
                    port=smtp_settings.port,
                    username=smtp_settings.host_user,
                    password=smtp_settings.get_decrypted_password(),
                    use_tls=smtp_settings.use_tls,
                    timeout=5
                )
                
                email_msg = smtp_settings.send_html_email(
                    subject_template=smtp_settings.order_modified_subject,
                    body_template=smtp_settings.order_modified_template,
                    context_dict={
                        'username': request.user.username,
                        'trading_symbol': trading_symbol,
                        'transaction_type': 'BUY' if transaction_type[0].upper() == 'B' else 'SELL',
                        'quantity': quantity,
                        'price': price,
                        'order_type': order_type,
                        'product_type': product_type,
                        'exchange_segment': exchange_segment,
                        'order_id': order_id,
                    },
                    to_emails=[request.user.email],
                    connection=connection
                )
                
                def send_email_async(msg):
                    try:
                        msg.send(fail_silently=False)
                        logger.info(f"Order modification email sent successfully to {request.user.email} for order {order_id}")
                    except Exception as ex:
                        logger.error(f"Failed to send order modification email async: {ex}")

                threading.Thread(target=send_email_async, args=(email_msg,)).start()
        except Exception as email_ex:
            logger.error(f"Failed to setup order modification email: {email_ex}", exc_info=True)

        return JsonResponse({'status': 'success', 'message': f"Order modification requested successfully! Order ID: {order_id}"})

    except (json.JSONDecodeError, ValueError, TypeError) as e:
        return JsonResponse({'error': f"Invalid request data: {e}"}, status=400)
    except Exception as e:
        return JsonResponse({'error': f"An unexpected error occurred: {e}"}, status=500)


@ajax_login_required
def get_order_book_ajax(request):
    """AJAX view to fetch order book"""
    try:
        api = KotakNeoAPI(user=request.user, session_id=request.session.session_key)
        order_book = api.get_order_book()
        
        if isinstance(order_book, dict) and 'error' in order_book:
            return JsonResponse({'status': 'error', 'message': order_book['error']}, status=400)
            
        return JsonResponse({'status': 'success', 'data': order_book})
    except Exception as e:
        return JsonResponse({'status': 'error', 'message': str(e)}, status=500)


@ajax_login_required
def get_holdings_ajax(request):
    """AJAX view to fetch holdings"""
    try:
        api = KotakNeoAPI(user=request.user, session_id=request.session.session_key)
        holdings = api.get_holdings()
        processed_holdings, portfolio_summary = _process_holdings_data(holdings)
        
        return JsonResponse({
            'status': 'success', 
            'data': processed_holdings,
            'summary': portfolio_summary
        })
    except Exception as e:
        return JsonResponse({'status': 'error', 'message': str(e)}, status=500)


@ajax_login_required
def get_positions_ajax(request):
    """AJAX view to fetch positions"""
    try:
        api = KotakNeoAPI(user=request.user, session_id=request.session.session_key)
        positions = api.get_positions()
        processed_positions = _process_positions_data(positions)
        
        return JsonResponse({'status': 'success', 'data': processed_positions})
    except Exception as e:
        return JsonResponse({'status': 'error', 'message': str(e)}, status=500)


@ajax_login_required
def get_limits_ajax(request):
    """AJAX view to fetch financial limits"""
    try:
        api = KotakNeoAPI(user=request.user, session_id=request.session.session_key)
        raw_limits = api.get_limits()
        limits, debug_limits = _process_limits_data(raw_limits)
        
        return JsonResponse({'status': 'success', 'data': limits})
    except Exception as e:
        return JsonResponse({'status': 'error', 'message': str(e)}, status=500)


