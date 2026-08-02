from django.contrib import messages
from ..kotak_neo_api import KotakNeoAPI

def _process_holdings_data(holdings, request=None):
    """Helper to process raw holdings data from SDK"""
    processed_holdings = []
    portfolio_summary = {
        'total_invested': 0,
        'current_value': 0,
        'total_pnl': 0,
        'pnl_percentage': 0
    }
    
    if isinstance(holdings, dict) and 'error' in holdings:
        if request:
            messages.warning(request, f"Could not fetch holdings: {holdings['error']}")
        return [], portfolio_summary

    if isinstance(holdings, list):
        for h in holdings:
            if not isinstance(h, dict):
                continue

            try:
                qty = float(h.get('quantity', 0))
                avg_price = float(h.get('averagePrice', 0))
                last_price = float(h.get('closingPrice', 0))
                mkt_value = float(h.get('mktValue', 0))
                holding_cost = float(h.get('holdingCost', 0))

                pnl = mkt_value - holding_cost
                
                portfolio_summary['total_invested'] += holding_cost
                portfolio_summary['current_value'] += mkt_value

                processed_holdings.append({
                    'symbol': h.get('symbol', 'N/A'),
                    'tradingsymbol': h.get('displaySymbol', h.get('symbol', 'N/A')),
                    'instrument_token': str(h.get('exchangeIdentifier', h.get('instrumentToken', ''))),
                    'exchange_segment': h.get('exchangeSegment', 'nse_cm'),
                    'quantity': qty,
                    'average_price': avg_price,
                    'last_price': last_price,
                    'pnl': pnl,
                })
            except (ValueError, TypeError):
                continue
        
        portfolio_summary['total_pnl'] = portfolio_summary['current_value'] - portfolio_summary['total_invested']
        if portfolio_summary['total_invested'] > 0:
            portfolio_summary['pnl_percentage'] = (portfolio_summary['total_pnl'] / portfolio_summary['total_invested']) * 100
            
    return processed_holdings, portfolio_summary


def _process_positions_data(positions, request=None):
    """Helper to process raw positions data from SDK"""
    processed_positions = []
    
    if isinstance(positions, dict) and 'error' in positions:
        if request:
            messages.warning(request, f"Could not fetch positions: {positions['error']}")
        return []

    if isinstance(positions, list):
        for p in positions:
            if not isinstance(p, dict):
                continue
            
            try:
                cf_buy = float(p.get('cfBuyQty', 0))
                cf_sell = float(p.get('cfSellQty', 0))
                fl_buy = float(p.get('flBuyQty', 0))
                fl_sell = float(p.get('flSellQty', 0))
                
                qty = (cf_buy + fl_buy) - (cf_sell + fl_sell)
                
                buy_amt = float(p.get('buyAmt', 0)) + float(p.get('cfBuyAmt', 0))
                buy_qty = cf_buy + fl_buy
                avg_price = buy_amt / buy_qty if buy_qty > 0 else 0
                
                ltp = float(p.get('upldPrc', 0)) or avg_price
                pnl = (ltp - avg_price) * qty
                
                processed_positions.append({
                    'trdSym': p.get('trdSym', 'N/A'),
                    'token': str(p.get('tok', '')),
                    'exchange': p.get('exSeg', 'nse_cm'),
                    'qty': int(qty),
                    'avgPrc': avg_price,
                    'ltp': ltp,
                    'pnl': pnl,
                    'dayPnl': 0, 
                    'multiplier': float(p.get('multiplier', 1)),
                })
            except (ValueError, TypeError, ZeroDivisionError):
                continue
    return processed_positions


def _process_limits_data(raw_limits, request=None):
    """Helper to process raw limits data from SDK"""
    limits = {}
    debug_limits = raw_limits
    
    if isinstance(raw_limits, dict) and 'error' in raw_limits:
        if request:
            messages.warning(request, f"Could not fetch limits: {raw_limits['error']}")
        return {}, raw_limits

    if isinstance(raw_limits, dict) and raw_limits.get('stat') == 'Ok':
        limits = {
            'available_trade': raw_limits.get('Net', '0.00'),
            'margin_used': raw_limits.get('MarginUsed', '0.00'),
            'collateral': raw_limits.get('CollateralValue', '0.00'),
            'total_cash': raw_limits.get('RmsPayInAmt', '0.00'),
            'unsettled_credit': raw_limits.get('CncSellcrdPresent', '0.00'), 
        }
    return limits, debug_limits


def logout_sdk_for_user(user, request=None):
    """Logout the Kotak Neo SDK session for the given user."""
    try:
        session_id = request.session.session_key if request else None
        api = KotakNeoAPI(user=user, session_id=session_id)
        api.logout()
    except Exception as e:
        import logging
        logging.getLogger(__name__).warning(f"SDK logout failed: {e}")


def _check_scrip_status_logic():
    """Internal logic to check if scrip master files exist and are up to date."""
    import glob
    import os
    from django.conf import settings
    from django.utils import timezone
    from django.db import connections
    from datetime import datetime
    from ..models import ActiveMarketData
    
    scrip_dir = os.path.join(settings.BASE_DIR, 'trades', 'scrip_data')
    
    if not os.path.exists(scrip_dir):
        return {'needs_refresh': True, 'reason': 'Directory missing'}
    
    csv_files = glob.glob(os.path.join(scrip_dir, '*.csv'))
    if not csv_files:
        return {'needs_refresh': True, 'reason': 'No files found'}
    
    now = timezone.localtime(timezone.now())
    cutoff_time = now.replace(hour=8, minute=0, second=0, microsecond=0)
    
    if now.hour < 8:
        cutoff_time = cutoff_time - timezone.timedelta(days=1)
    
    try:
        latest_mtime = max(os.path.getmtime(f) for f in csv_files)
        latest_dt = timezone.make_aware(datetime.fromtimestamp(latest_mtime))
        
        if latest_dt < cutoff_time:
            return {
                'needs_refresh': True, 
                'reason': 'Data outdated', 
                'latest_update': latest_dt.strftime('%Y-%m-%d %H:%M:%S'),
                'cutoff': cutoff_time.strftime('%Y-%m-%d %H:%M:%S')
            }
            
        try:
            if not ActiveMarketData.objects.exists():
                return {'needs_refresh': True, 'reason': 'Cache empty'}
        except Exception as db_err:
            import logging
            logging.getLogger(__name__).error(f"Error checking cache status in SQLite: {db_err}")
            return {'needs_refresh': True, 'reason': 'Cache check failed'}
                
    except Exception as e:
        return {'needs_refresh': True, 'reason': f'Error checking files/cache: {str(e)}'}

    return {'needs_refresh': False}


def generate_temp_password(length=8):
    import random
    import string
    chars = string.ascii_letters + string.digits
    return ''.join(random.choice(chars) for _ in range(length))


def send_password_change_confirmation_email(user):
    """Send confirmation email when password is changed successfully"""
    from ..models import SMTPSettings
    settings_obj = SMTPSettings.get_settings()
    if not user.email or not settings_obj.host:
        return
        
    try:
        from django.core.mail import get_connection
        connection = get_connection(
            host=settings_obj.host,
            port=settings_obj.port,
            username=settings_obj.host_user,
            password=settings_obj.get_decrypted_password(),
            use_tls=settings_obj.use_tls
        )
        email_msg = settings_obj.send_html_email(
            subject_template=settings_obj.password_changed_subject,
            body_template=settings_obj.password_changed_template,
            context_dict={'username': user.username},
            to_emails=[user.email],
            connection=connection
        )
        email_msg.send(fail_silently=False)
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f"Error sending password confirmation email: {e}")

