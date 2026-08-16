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
from .helpers import _check_scrip_status_logic

logger = logging.getLogger(__name__)

_scrip_refresh_lock = threading.Lock()


def _quote_sql_string(value):
    return "'" + value.replace("'", "''") + "'"


def get_p_group_description(exch, group):
    if not group or group == 'XX':
        return ""
    
    exch_lower = exch.lower() if exch else ""
    if exch_lower not in ('nse_cm', 'bse_cm'):
        return ""
    
    meaning = SCRIP_GROUP_MEANINGS.get(exch_lower, {}).get(group)
    if meaning:
        return meaning
    
    # Pattern matching for NSE series
    if exch_lower == 'nse_cm':
        if group.startswith(('N', 'Y', 'Z')) and len(group) == 2:
            return "NCD/Debt"
        if group.startswith('A') and len(group) == 2:
            return "Debt"
            
    return group # Fallback to code itself


def _get_scrip_data_files():
    scrip_dir = os.path.join(settings.BASE_DIR, 'trades', 'scrip_data')
    if not os.path.isdir(scrip_dir):
        raise FileNotFoundError(f"Scrip data folder not found: {scrip_dir}")

    csv_files = sorted(glob.glob(os.path.join(scrip_dir, '*.csv')))
    if not csv_files:
        return []

    target_keywords = ['nse_fo', 'bse_fo', 'nse_cm', 'bse_cm']
    matched_files = [path for path in csv_files if any(keyword in os.path.basename(path).lower() for keyword in target_keywords)]
    if matched_files:
        return matched_files

    if len(csv_files) <= 4:
        return csv_files

    return []


def _perform_scrip_cache_refresh():
    """Internal function to refresh scrip cache in SQLite"""
    try:
        csv_files = _get_scrip_data_files()
        if not csv_files:
            logger.error("No scrip CSV files found for refresh.")
            return False, "No scrip CSV files found."

        connection = connections['scrip_cache']
        
        # Optimize SQLite speed settings for bulk load (Must be done OUTSIDE transaction)
        with connection.cursor() as cursor:
            cursor.execute("PRAGMA synchronous = OFF")
            cursor.execute("PRAGMA journal_mode = MEMORY")
            cursor.execute("PRAGMA temp_store = MEMORY")

        # Stream, process and load into SQLite
        with transaction.atomic('scrip_cache'):
            with connection.cursor() as cursor:
                cursor.execute("DROP TABLE IF EXISTS active_market_data")
                cursor.execute("""
                    CREATE TABLE active_market_data (
                        pSymbol TEXT,
                        pExchSeg TEXT,
                        pSymbolName TEXT,
                        pTrdSymbol TEXT,
                        pOptionType TEXT,
                        pInstType TEXT,
                        dStrikePrice REAL,
                        pScripRefKey TEXT,
                        pDesc TEXT,
                        pGroup TEXT,
                        pAssetCode TEXT,
                        dTickSize REAL,
                        lLotSize INTEGER,
                        expire_date TEXT,
                        has_option_chain INTEGER
                    )
                """)

                date_pat = re.compile(r'(\d{2})([A-Z]{3})(\d{2})')
                month_map = {
                    'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MAY': 5, 'JUN': 6,
                    'JUL': 7, 'AUG': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12
                }
                
                today_str = datetime.date.today().strftime('%Y-%m-%d')
                
                insert_sql = """
                    INSERT INTO active_market_data (
                        pSymbol, pExchSeg, pSymbolName, pTrdSymbol, pOptionType, pInstType,
                        dStrikePrice, pScripRefKey, pDesc, pGroup, pAssetCode,
                        dTickSize, lLotSize, expire_date, has_option_chain
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """

                for csv_file in csv_files:
                    try:
                        with open(csv_file, 'r', encoding='utf-8', errors='ignore') as f:
                            reader = csv.reader(f)
                            try:
                                headers = next(reader)
                            except StopIteration:
                                continue
                            
                            headers_clean = [h.strip().rstrip(';') for h in headers]
                            
                            indices = {
                                'pSymbol': headers_clean.index('pSymbol') if 'pSymbol' in headers_clean else -1,
                                'pExchSeg': headers_clean.index('pExchSeg') if 'pExchSeg' in headers_clean else -1,
                                'pSymbolName': headers_clean.index('pSymbolName') if 'pSymbolName' in headers_clean else -1,
                                'pTrdSymbol': headers_clean.index('pTrdSymbol') if 'pTrdSymbol' in headers_clean else -1,
                                'pOptionType': headers_clean.index('pOptionType') if 'pOptionType' in headers_clean else -1,
                                'pInstType': headers_clean.index('pInstType') if 'pInstType' in headers_clean else -1,
                                'dStrikePrice': headers_clean.index('dStrikePrice') if 'dStrikePrice' in headers_clean else -1,
                                'pScripRefKey': headers_clean.index('pScripRefKey') if 'pScripRefKey' in headers_clean else -1,
                                'pDesc': headers_clean.index('pDesc') if 'pDesc' in headers_clean else -1,
                                'pGroup': headers_clean.index('pGroup') if 'pGroup' in headers_clean else -1,
                                'pAssetCode': headers_clean.index('pAssetCode') if 'pAssetCode' in headers_clean else -1,
                                'dTickSize': headers_clean.index('dTickSize') if 'dTickSize' in headers_clean else -1,
                                'lLotSize': headers_clean.index('lLotSize') if 'lLotSize' in headers_clean else -1,
                            }
                            
                            batch = []
                            for row in reader:
                                row_len = len(row)
                                
                                def get_val(col_name):
                                    idx = indices[col_name]
                                    if idx >= 0 and idx < row_len:
                                        return row[idx].strip()
                                    return None
                                
                                pSymbol = get_val('pSymbol')
                                pExchSeg = get_val('pExchSeg')
                                pSymbolName = get_val('pSymbolName')
                                pTrdSymbol = get_val('pTrdSymbol')
                                pOptionType = get_val('pOptionType')
                                pInstType = get_val('pInstType')
                                pScripRefKey = get_val('pScripRefKey')
                                pDesc = get_val('pDesc')
                                pGroup = get_val('pGroup')
                                pAssetCode = get_val('pAssetCode')
                                
                                strike_val = get_val('dStrikePrice')
                                try:
                                    dStrikePrice = float(strike_val) if strike_val else 0.0
                                except ValueError:
                                    dStrikePrice = 0.0

                                tick_val = get_val('dTickSize')
                                try:
                                    dTickSize = float(tick_val) if tick_val else 0.0
                                except ValueError:
                                    dTickSize = 0.0

                                lot_val = get_val('lLotSize')
                                try:
                                    lLotSize = int(lot_val) if lot_val else 0
                                except ValueError:
                                    lLotSize = 0

                                expire_date = None
                                if pScripRefKey:
                                    m = date_pat.search(pScripRefKey)
                                    if m:
                                        day, mon, yr = m.groups()
                                        mon_num = month_map.get(mon.upper())
                                        if mon_num:
                                            try:
                                                year = 2000 + int(yr)
                                                expire_date = f"{year:04d}-{mon_num:02d}-{int(day):02d}"
                                            except Exception:
                                                pass

                                if expire_date and expire_date < today_str:
                                    continue

                                batch.append((
                                    pSymbol, pExchSeg, pSymbolName, pTrdSymbol, pOptionType, pInstType,
                                    dStrikePrice, pScripRefKey, pDesc, pGroup, pAssetCode,
                                    dTickSize, lLotSize, expire_date, 0
                                ))

                                if len(batch) >= 5000:
                                    cursor.executemany(insert_sql, batch)
                                    batch = []
                            
                            if batch:
                                cursor.executemany(insert_sql, batch)

                    except Exception as fe:
                        logger.error(f"Error in Pass 2 for file {csv_file}: {fe}")

                cursor.execute("CREATE INDEX IF NOT EXISTS idx_amd_symbol_exch ON active_market_data (pSymbol, pExchSeg)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_amd_asset_code ON active_market_data (pAssetCode)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_amd_name ON active_market_data (pSymbolName)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_amd_ref_key ON active_market_data (pScripRefKey)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_amd_expiry ON active_market_data (expire_date)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_amd_inst_type ON active_market_data (pInstType)")
                
                # Fast update of option chain presence
                cursor.execute("""
                    UPDATE active_market_data
                    SET has_option_chain = 1
                    WHERE pSymbol IN (
                        SELECT DISTINCT pAssetCode
                        FROM active_market_data
                        WHERE pInstType IN ('OPTIDX', 'OPTSTK', 'IO', 'SO')
                    )
                """)

                cursor.execute("SELECT COUNT(*) FROM active_market_data")
                row_count = cursor.fetchone()[0]
                logger.info(f"Refreshed SQLite active_market_data with {row_count} active scrips.")
                return True, f"Refreshed with {row_count} scrips from {len(csv_files)} files.", row_count

    except Exception as e:
        logger.error(f"Error performing scrip cache refresh: {e}")
        return False, str(e), 0


def ensure_scrip_cache():
    """Ensure active_market_data is loaded in SQLite using Django ORM"""
    try:
        if ActiveMarketData.objects.exists():
            return True
    except Exception:
        pass
    
    success, _, _ = _perform_scrip_cache_refresh()
    return success


# ==================== Authentication Views ====================


@ajax_login_required
def refresh_scrip_master(request):
    force = request.GET.get('force', 'false').lower() == 'true'
    
    # Try to acquire lock without blocking to avoid hanging the request
    acquired = _scrip_refresh_lock.acquire(blocking=False)
    if not acquired:
        return JsonResponse({'status': 'error', 'message': 'A refresh is already in progress. Please wait.'}, status=429)
    
    try:
        if not force:
            status = _check_scrip_status_logic()
            if not status.get('needs_refresh') or status.get('reason') == 'Cache empty':
                return JsonResponse({'status': 'success', 'message': 'Scrip master files are already up-to-date.'})

        try:
            session_id = getattr(request.session, 'session_key', None) or get_api_session_id(request.user)
            api = KotakNeoAPI(user=request.user, session_id=session_id)
        except Exception as e:
            return JsonResponse({'status': 'error', 'message': str(e)}, status=400)
        
        try:
            result = api.download_scrip_master()
            if result.get('status') == 'success':
                return JsonResponse({'status': 'success', 'message': f"Scrip master data downloaded successfully."})
            else:
                return JsonResponse({'status': 'error', 'message': result.get('error', 'An unknown error occurred.')}, status=400)
        except Exception as e:
            return JsonResponse({'status': 'error', 'message': str(e)}, status=500)
    finally:
        _scrip_refresh_lock.release()


@ajax_login_required
def refresh_scrip_cache(request):
    if request.method != 'GET':
        return JsonResponse({'status': 'error', 'message': 'Only GET requests are allowed.'}, status=405)

    force = request.GET.get('force', 'false').lower() == 'true'
    
    # Try to acquire lock without blocking
    acquired = _scrip_refresh_lock.acquire(blocking=False)
    if not acquired:
        return JsonResponse({'status': 'error', 'message': 'A refresh is already in progress. Please wait.'}, status=429)

    try:
        if not force:
            status = _check_scrip_status_logic()
            if not status.get('needs_refresh'):
                try:
                    connection = connections['scrip_cache']
                    with connection.cursor() as cursor:
                        cursor.execute('SELECT COUNT(*) FROM active_market_data')
                        current_rows = cursor.fetchone()[0]
                except Exception:
                    current_rows = 0
                return JsonResponse({'status': 'success', 'message': 'Scrip cache is already up-to-date.', 'row_count': current_rows})

        success, message, row_count = _perform_scrip_cache_refresh()
        if success:
            return JsonResponse({'status': 'success', 'message': message, 'row_count': row_count})
        else:
            return JsonResponse({'status': 'error', 'message': message}, status=500)
    finally:
        _scrip_refresh_lock.release()


def perform_market_search_cache(search_term, exchange='all', inst_type='all'):
    """
    Core search logic for active_market_data.
    Returns: (list of dicts, error_message)
    """
    from ..models import ActiveMarketData
    search_term = search_term.strip()
    if not search_term or len(search_term) < 2:
        return None, 'Search term must be at least 2 characters.'

    try:
        if not ActiveMarketData.objects.exists():
            return None, 'Scrip cache is empty. Please refresh the scrip cache and try again.'
    except Exception:
        return None, 'Scrip cache table not found. Please refresh the scrip cache and try again.'

    # Build filter conditions
    filters = []
    params = []
    if exchange != 'all':
        filters.append("pExchSeg = ?")
        params.append(exchange)

    if inst_type != 'all':
        if inst_type == 'stock':
            filters.append("(pInstType IS NULL OR pInstType = '')")
        elif inst_type == 'option':
            filters.append("(pInstType IN ('OPTSTK', 'OPTIDX'))")
        elif inst_type == 'future':
            filters.append("(pInstType IN ('FUTIDX', 'FUTSTK'))")

    where_clause = " AND ".join(filters) if filters else "1=1"

    # Build elastic search: make it tighter by requiring more matches
    search_terms = search_term.lower().split()
    
    # Build conditions for options/futures: search only in pScripRefKey (AND logic)
    fno_conditions = []
    fno_params = []
    for term in search_terms:
        fno_conditions.append("LOWER(COALESCE(pScripRefKey, '')) LIKE ?")
        fno_params.append(f"%{term}%")
    fno_search = " AND ".join(fno_conditions) if fno_conditions else "1=1"
    
    # Build conditions for stocks: search in pScripRefKey OR pDesc (OR logic for terms)
    stock_conditions = []
    stock_params = []
    for term in search_terms:
        stock_conditions.append(
            "(LOWER(COALESCE(pScripRefKey, '')) LIKE ? OR LOWER(COALESCE(pDesc, '')) LIKE ?)"
        )
        stock_params.append(f"%{term}%")
        stock_params.append(f"%{term}%")
    stock_search = " OR ".join(stock_conditions) if stock_conditions else "1=1"
    
    # Combine: prioritize F&O search if looking for options/futures or when exchange is F&O, otherwise use stock search
    if inst_type in ('option', 'future') or exchange in ('nse_fo', 'bse_fo'):
        final_search = f"({fno_search})"
        params.extend(fno_params)
    else:
        # For stocks or non-F&O search, use stock search (looser)
        final_search = f"({stock_search})"
        params.extend(stock_params)

    first_term = search_terms[0] if search_terms else ''
    first_term_pat = f"{first_term}%"
    params.append(first_term_pat)
    params.append(first_term_pat)
    
    # Check if user likely wants F&O based on month abbreviations in search
    months = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']
    has_date_term = any(m in term for term in search_terms for m in months)
    wants_fo = inst_type in ('option', 'future') or exchange in ('nse_fo', 'bse_fo') or has_date_term
    
    stock_priority_clause = "expire_date IS NOT NULL ASC," if not wants_fo else ""
    
    order_by_clause = f"""
        ORDER BY 
            -- 1. Exact prefix match gets top priority
            CASE 
                WHEN LOWER(COALESCE(pSymbolName, '')) LIKE ? THEN 0 
                WHEN LOWER(COALESCE(pScripRefKey, '')) LIKE ? THEN 1
                ELSE 2 
            END ASC,
            -- 2. Equity prioritization if no FO intent
            {stock_priority_clause}
            -- 3. Sort by Nearest Expiry Date (Options/Futures)
            expire_date ASC NULLS LAST,
            -- 4. Strike price sorting (closest to 0 or sequential)
            dStrikePrice ASC,
            -- 5. Finally alphabetical
            pSymbolName ASC,
            pScripRefKey ASC
    """

    query = f"""
        SELECT 
            pSymbol,
            pExchSeg,
            pSymbolName,
            pTrdSymbol,
            pOptionType,
            pInstType,
            CAST(COALESCE(dStrikePrice, 0) AS REAL) / 100 as dStrikePrice,
            pScripRefKey,
            pDesc,
            COALESCE(pGroup, '') as pGroup,
            COALESCE(CAST(pAssetCode AS TEXT), '') as pAssetCode,
            has_option_chain,
            CAST(COALESCE(dTickSize, 0) AS REAL) / 100 as dTickSize,
            CAST(COALESCE(lLotSize, 0) AS INTEGER) as lLotSize
        FROM active_market_data
        WHERE {where_clause} AND {final_search}
        {order_by_clause}
        LIMIT 50
    """

    results = ActiveMarketData.objects.raw(query, params)
    data = []
    for scrip in results:
        data.append({
            'pSymbol': scrip.symbol,
            'pExchSeg': scrip.exch_seg,
            'pSymbolName': scrip.symbol_name,
            'pTrdSymbol': scrip.trd_symbol,
            'pOptionType': scrip.option_type,
            'pInstType': scrip.inst_type,
            'dStrikePrice': float(scrip.strike_price or 0.0),
            'pScripRefKey': scrip.scrip_ref_key,
            'pDesc': scrip.desc,
            'pGroup': scrip.group or '',
            'pAssetCode': scrip.asset_code or '',
            'has_option_chain': scrip.has_option_chain,
            'dTickSize': float(scrip.tick_size or 0.0),
            'lLotSize': int(scrip.lot_size or 0)
        })
    
    # Add pGroup description
    for item in data:
        item['pGroupDesc'] = get_p_group_description(item.get('pExchSeg'), item.get('pGroup'))
    
    return data, None


@login_required_with_session_check
def search_scrip_cache(request):
    if request.method != 'GET':
        return JsonResponse({'error': 'Only GET requests are allowed'}, status=405)

    try:
        search_term = request.GET.get('q', '').strip()
        exchange = request.GET.get('exchange', 'all')
        inst_type = request.GET.get('inst_type', 'all')

        data, err = perform_market_search_cache(search_term, exchange, inst_type)
        if err:
            return JsonResponse({'error': err}, status=400)

        logger.info(f"SQLite DB Scrip search execution for '{search_term}' using filters (exchange: {exchange}, inst_type: {inst_type}) returned {len(data)} results.")

        return JsonResponse({
            'results': data,
            'count': len(data),
            'total_available': min(50, len(data))
        })
    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)


@login_required_with_session_check
def search_scrips_ajax(request):
    if request.method != 'GET':
        return JsonResponse({'error': 'Only GET requests are allowed'}, status=405)

    symbol = request.GET.get('symbol', '')
    exchange_segment = request.GET.get('exchange_segment', 'nse_cm')

    if not symbol:
        return JsonResponse({'error': 'Symbol is required.'}, status=400)

    try:
        api = KotakNeoAPI(user=request.user, session_id=request.session.session_key)
    except Exception as e:
        return JsonResponse({'error': str(e)}, status=400)
    
    logger.debug(f"User '{request.user.username}' searching scrips for symbol '{symbol}' in {exchange_segment}.")
    results = api.search_scrip(exchange_segment=exchange_segment, symbol=symbol)

    if 'error' in results:
        return JsonResponse({'error': results['error']}, status=400)

    return JsonResponse(results, safe=False)


@login_required_with_session_check
def check_scrip_status(request):
    """Check if scrip master files exist and are up to date (from today)."""
    return JsonResponse(_check_scrip_status_logic())


@login_required_with_session_check
def get_depth(request):
    if request.method != 'GET':
        return JsonResponse({'error': 'Only GET requests are allowed'}, status=405)

    p_symbol = request.GET.get('p_symbol', '')
    p_exch_seg = request.GET.get('p_exch_seg', '')

    if not p_symbol or not p_exch_seg:
        return JsonResponse({'error': 'p_symbol and p_exch_seg are required.'}, status=400)

    try:
        api = KotakNeoAPI(user=request.user, session_id=request.session.session_key)
    except Exception as e:
        return JsonResponse({'error': str(e)}, status=400)
    
    instrument_tokens = [{"instrument_token": p_symbol, "exchange_segment": p_exch_seg}]
    result = api.quotes(instrument_tokens=instrument_tokens, quote_type="all")

    if 'error' in result:
        return JsonResponse({'error': result['error']}, status=400)

    # The result is a list with one item
    if isinstance(result, list) and len(result) > 0:
        quote = result[0]
        depth_data = {
            'ltp': quote.get('ltp'),
            'buy_depth': quote.get('depth', {}).get('buy', []),
            'sell_depth': quote.get('depth', {}).get('sell', [])
        }
        logger.info(f"Successfully retrieved static depth and LTP ({quote.get('ltp')}) from SDK for target {p_symbol} ({p_exch_seg}).")
        return JsonResponse(depth_data)
    else:
        return JsonResponse({'error': 'No depth data received'}, status=400)


@login_required_with_session_check
def get_ltp(request):
    if request.method != 'GET':
        return JsonResponse({'error': 'Only GET requests are allowed'}, status=405)

    p_symbol = request.GET.get('p_symbol', '')
    p_exch_seg = request.GET.get('p_exch_seg', '')

    if not p_symbol or not p_exch_seg:
        return JsonResponse({'error': 'p_symbol and p_exch_seg are required.'}, status=400)

    try:
        api = KotakNeoAPI(user=request.user, session_id=request.session.session_key)
    except Exception as e:
        return JsonResponse({'error': str(e)}, status=400)

    instrument_tokens = [{"instrument_token": p_symbol, "exchange_segment": p_exch_seg}]
    result = api.quotes(instrument_tokens=instrument_tokens, quote_type="all")

    if 'error' in result:
        return JsonResponse({'error': result['error']}, status=400)

    if isinstance(result, list) and len(result) > 0:
        quote = result[0]
        return JsonResponse({
            'ltp': quote.get('ltp'),
            'lower_circuit': quote.get('low_price_range'),
            'upper_circuit': quote.get('high_price_range')
        })

    return JsonResponse({'error': 'No quote data received'}, status=400)


@login_required_with_session_check
def get_scrip_info_ajax(request):
    token = request.GET.get('token')
    exch = request.GET.get('exch')
    if not token or not exch:
        return JsonResponse({'error': 'Missing token or exchange'}, status=400)

    try:
        scrip = ActiveMarketData.objects.filter(symbol=token, exch_seg=exch).first()
        if not scrip:
            return JsonResponse({'error': 'Scrip not found in cache'}, status=404)
        
        data = {
            'pSymbol': scrip.symbol,
            'pExchSeg': scrip.exch_seg,
            'pSymbolName': scrip.symbol_name,
            'pTrdSymbol': scrip.trd_symbol,
            'pOptionType': scrip.option_type,
            'pInstType': scrip.inst_type,
            'dStrikePrice': float(scrip.strike_price or 0.0) / 100,
            'pScripRefKey': scrip.scrip_ref_key,
            'pDesc': scrip.desc,
            'pGroup': scrip.group or '',
            'pAssetCode': scrip.asset_code or '',
            'has_option_chain': scrip.has_option_chain,
            'dTickSize': float(scrip.tick_size or 0.0) / 100,
            'lLotSize': int(scrip.lot_size or 0),
            'pGroupDesc': get_p_group_description(scrip.exch_seg, scrip.group)
        }
        return JsonResponse({'status': 'success', 'data': data})
    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)


@login_required_with_session_check
def get_option_chain_ajax(request):
    p_symbol = request.GET.get('p_symbol')
    if not p_symbol:
        return JsonResponse({'error': 'Missing p_symbol'}, status=400)

    try:
        results = ActiveMarketData.objects.filter(
            asset_code=p_symbol,
            inst_type__in=['OPTIDX', 'OPTSTK', 'IO', 'SO']
        ).order_by('expire_date', 'strike_price')
        
        raw_data = []
        for scrip in results:
            raw_data.append({
                'pSymbol': scrip.symbol,
                'pExchSeg': scrip.exch_seg,
                'pSymbolName': scrip.symbol_name,
                'pTrdSymbol': scrip.trd_symbol,
                'pOptionType': scrip.option_type,
                'pInstType': scrip.inst_type,
                'dStrikePrice': float(scrip.strike_price or 0.0) / 100,
                'pScripRefKey': scrip.scrip_ref_key,
                'pDesc': scrip.desc,
                'dTickSize': float(scrip.tick_size or 0.0) / 100,
                'lLotSize': int(scrip.lot_size or 0),
                'expire_date_str': scrip.expire_date
            })
        
        # Group by expiry and strike
        chain_data = {}
        expiries = []
        
        for row in raw_data:
            exp = row['expire_date_str']
            strike = row['dStrikePrice']
            opt_type = row['pOptionType']
            
            if exp not in chain_data:
                chain_data[exp] = {}
                expiries.append(exp)
            
            if strike not in chain_data[exp]:
                chain_data[exp][strike] = {'CE': None, 'PE': None}
            
            if opt_type == 'CE':
                chain_data[exp][strike]['CE'] = row
            elif opt_type == 'PE':
                chain_data[exp][strike]['PE'] = row
        
        # Convert strikes to sorted list for each expiry
        final_chain = {}
        for exp in expiries:
            sorted_strikes = []
            for strike in sorted(chain_data[exp].keys()):
                strike_row = chain_data[exp][strike]
                strike_row['strike'] = float(strike)
                sorted_strikes.append(strike_row)
            final_chain[exp] = sorted_strikes

        return JsonResponse({
            'status': 'success',
            'expiries': expiries,
            'chain': final_chain
        })
    except Exception as e:
        logger.error(f"Error fetching option chain: {e}")
        return JsonResponse({'error': str(e)}, status=500)


SCRIP_GROUP_MEANINGS = {
    'bse_cm': {
        'A': 'Active',
        'B': 'Large/Mid Cap',
        'E': 'ETF',
        'F': 'Debt Market',
        'G': 'G-Sec',
        'IF': 'Inst-Debt',
        'M': 'SME',
        'MS': 'SME',
        'MT': 'SME-T2T',
        'P': 'Penny/Surv',
        'R': 'Rights',
        'T': 'T2T',
        'TS': 'SME-T2T',
        'X': 'BSE-Only',
        'XT': 'BSE-Only T2T',
        'Y': 'Debt',
        'Z': 'Non-Compliant',
        'ZP': 'Non-Compliant'
    },
    'nse_cm': {
        'EQ': 'Equity',
        'BE': 'T2T',
        'BL': 'Block Deal',
        'BT': 'Physical',
        'MF': 'Mutual Fund',
        'SM': 'SME',
        'ST': 'SME-T2T',
        'TB': 'T-Bill',
        'GB': 'G-Bond',
        'GS': 'G-Sec',
        'IV': 'InvIT',
        'RR': 'Rights',
        'DR': 'Depository Rec',
        'W1': 'Warrant',
        'SF': 'Security Rec',
        'SG': 'Gold Bond',
        'BZ': 'Surveillance',
        'SZ': 'SME-Surv',
        'IT': 'InvIT',
        'D1': 'Debt',
        'E1': 'ETF',
        'P1': 'Preference'
    }
}


