from django.shortcuts import render
from .kotak_neo_api import KotakNeoAPI
from django.contrib import messages
from django.http import JsonResponse
from django.conf import settings
import json
import duckdb
import glob
import os
import threading

_duckdb_connection = duckdb.connect(database=':memory:')
_duckdb_lock = threading.Lock()

def _quote_sql_string(value):
    return "'" + value.replace("'", "''") + "'"


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


def refresh_scrip_master(request):
    api = KotakNeoAPI()
    try:
        result = api.download_scrip_master()
        if result.get('status') == 'success':
            return JsonResponse({'status': 'success', 'message': f"Scrip master data downloaded successfully to {result.get('downloaded_files')}"})
        else:
            return JsonResponse({'status': 'error', 'message': result.get('error', 'An unknown error occurred.')})
    except Exception as e:
        return JsonResponse({'status': 'error', 'message': str(e)})


def refresh_scrip_cache(request):
    if request.method != 'GET':
        return JsonResponse({'status': 'error', 'message': 'Only GET requests are allowed.'}, status=405)

    try:
        csv_files = _get_scrip_data_files()
        if not csv_files:
            return JsonResponse({
                'status': 'error',
                'message': 'Could not find matching scrip CSV files in trades/scrip_data. Expected files containing nse_fo, bse_fo, nse_cm, or bse_cm.'
            }, status=404)

        with _duckdb_lock:
            _duckdb_connection.execute('DROP TABLE IF EXISTS all_market_data')
            file_list_sql = ', '.join(_quote_sql_string(path) for path in csv_files)
            _duckdb_connection.execute(
                f"CREATE TABLE all_market_data AS SELECT * FROM read_csv([{file_list_sql}], union_by_name=True)"
            )
            row_count = _duckdb_connection.execute('SELECT COUNT(*) FROM all_market_data').fetchone()[0]

        return JsonResponse({
            'status': 'success',
            'message': f'Refreshed all_market_data with {row_count} rows from {len(csv_files)} file(s).',
            'loaded_files': [os.path.basename(path) for path in csv_files],
            'row_count': row_count,
        })
    except Exception as e:
        return JsonResponse({'status': 'error', 'message': str(e)}, status=500)

def search_scrip_cache(request):
    if request.method != 'GET':
        return JsonResponse({'error': 'Only GET requests are allowed'}, status=405)

    try:
        search_term = request.GET.get('q', '').strip()
        exchange = request.GET.get('exchange', 'all')  # all, nse_cm, bse_cm, nse_fo, bse_fo
        inst_type = request.GET.get('inst_type', 'all')  # all, stock, option, future

        if not search_term or len(search_term) < 2:
            return JsonResponse({'error': 'Search term must be at least 2 characters.'}, status=400)

        with _duckdb_lock:
            try:
                row_count = _duckdb_connection.execute('SELECT COUNT(*) FROM all_market_data').fetchone()[0]
                if row_count == 0:
                    return JsonResponse({
                        'error': 'Scrip cache is empty. Please refresh the scrip cache and try again.',
                        'action': 'refresh_cache'
                    }, status=400)
            except Exception:
                return JsonResponse({
                    'error': 'Scrip cache table not found. Please refresh the scrip cache and try again.',
                    'action': 'refresh_cache'
                }, status=400)

            # Build filter conditions
            filters = []
            if exchange != 'all':
                filters.append(f"pExchSeg = '{exchange}'")

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
            
            # Escape single quotes in search terms
            safe_terms = [term.replace("'", "''") for term in search_terms]
            
            # Build conditions for options/futures: search only in pScripRefKey (AND logic)
            fno_conditions = []
            for term in safe_terms:
                fno_conditions.append(f"LOWER(COALESCE(pScripRefKey, '')) LIKE '%{term}%'")
            fno_search = " AND ".join(fno_conditions) if fno_conditions else "1=1"
            
            # Build conditions for stocks: search in pScripRefKey OR pDesc (OR logic for terms)
            stock_conditions = []
            for term in safe_terms:
                stock_conditions.append(f"""
                    (LOWER(COALESCE(pScripRefKey, '')) LIKE '%{term}%'
                    OR LOWER(COALESCE(pDesc, '')) LIKE '%{term}%')
                """)
            stock_search = " OR ".join(stock_conditions) if stock_conditions else "1=1"
            
            # Combine: prioritize F&O search if looking for options/futures or when exchange is F&O, otherwise use stock search
            if inst_type in ('option', 'future') or exchange in ('nse_fo', 'bse_fo'):
                final_search = f"({fno_search})"
            else:
                # For stocks or non-F&O search, use stock search (looser)
                final_search = f"({stock_search})"

            query = f"""
                SELECT 
                    pSymbol,
                    pExchSeg,
                    pSymbolName,
                    pTrdSymbol,
                    pOptionType,
                    pInstType,
                    CAST(COALESCE("dStrikePrice;", 0) AS DECIMAL) / 100 as dStrikePrice,
                    pScripRefKey,
                    pDesc,
                    CAST(COALESCE(dTickSize, dTickSize, 0) AS DECIMAL) / 100 as dTickSize,
                    CAST(COALESCE(lLotSize, 0) AS INTEGER) as lLotSize
                FROM all_market_data
                WHERE {where_clause} AND {final_search}
                LIMIT 50
            """

            results = _duckdb_connection.execute(query).fetchall()
            columns = ['pSymbol', 'pExchSeg', 'pSymbolName', 'pTrdSymbol', 'pOptionType', 'pInstType', 'dStrikePrice', 'pScripRefKey', 'pDesc', 'dTickSize', 'lLotSize']
            
            data = [dict(zip(columns, row)) for row in results]
            
            return JsonResponse({
                'results': data,
                'count': len(data),
                'total_available': min(50, len(data))
            })

    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)

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

        if order_type == 'MKT':
            price = 0
        elif price is None or price == '':
            return JsonResponse({'error': 'Price is required for limit orders.'}, status=400)

        api = KotakNeoAPI()
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
            return JsonResponse({'status': 'error', 'message': f"Margin check failed: {margin_response['error']}"}, status=400)

        margin_data = margin_response.get('data', {}) if isinstance(margin_response, dict) else {}
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

        if 'errMsg' in api_response:
            return JsonResponse({'status': 'error', 'message': api_response['errMsg']}, status=400)
        
        order_id = api_response.get('nOrdNo', 'N/A')
        return JsonResponse({'status': 'success', 'message': f"Trade placed successfully! Order ID: {order_id}", 'data': api_response})

    except (json.JSONDecodeError, ValueError, TypeError) as e:
        return JsonResponse({'error': f"Invalid request data: {e}"}, status=400)
    except Exception as e:
        return JsonResponse({'error': f"An unexpected error occurred: {e}"}, status=500)


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

        api = KotakNeoAPI()
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
            return JsonResponse({'error': f"Margin check failed: {margin_response['error']}"}, status=400)

        return JsonResponse({'status': 'success', 'data': margin_response.get('data', margin_response)})

    except (json.JSONDecodeError, ValueError, TypeError) as e:
        return JsonResponse({'error': f"Invalid request data: {e}"}, status=400)
    except Exception as e:
        return JsonResponse({'error': f"An unexpected error occurred: {e}"}, status=500)


def cancel_order_ajax(request):
    if request.method != 'POST':
        return JsonResponse({'error': 'Only POST requests are allowed'}, status=405)

    try:
        data = json.loads(request.body)
        order_id = data.get('order_id')

        if not order_id:
            return JsonResponse({'error': 'Order ID is required.'}, status=400)

        api = KotakNeoAPI()
        api_response = api.cancel_order(order_id)
        
        if 'errMsg' in api_response:
            return JsonResponse({'status': 'error', 'message': api_response['errMsg']}, status=400)
        
        return JsonResponse({'status': 'success', 'message': f"Order cancellation requested: {api_response.get('result', 'Success')} - {api_response.get('stat', 'Success')}"})

    except (json.JSONDecodeError, ValueError, TypeError) as e:
        return JsonResponse({'error': f"Invalid request data: {e}"}, status=400)
    except Exception as e:
        return JsonResponse({'error': f"An unexpected error occurred: {e}"}, status=500)


def search_scrips_ajax(request):
    if request.method != 'GET':
        return JsonResponse({'error': 'Only GET requests are allowed'}, status=405)

    symbol = request.GET.get('symbol', '')
    exchange_segment = request.GET.get('exchange_segment', 'nse_cm')

    if not symbol:
        return JsonResponse({'error': 'Symbol is required.'}, status=400)

    api = KotakNeoAPI()
    results = api.search_scrip(exchange_segment=exchange_segment, symbol=symbol)

    if 'error' in results:
        return JsonResponse({'error': results['error']}, status=400)

    return JsonResponse(results, safe=False)


def get_depth(request):
    if request.method != 'GET':
        return JsonResponse({'error': 'Only GET requests are allowed'}, status=405)

    p_symbol = request.GET.get('p_symbol', '')
    p_exch_seg = request.GET.get('p_exch_seg', '')

    if not p_symbol or not p_exch_seg:
        return JsonResponse({'error': 'p_symbol and p_exch_seg are required.'}, status=400)

    api = KotakNeoAPI()
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
        return JsonResponse(depth_data)
    else:
        return JsonResponse({'error': 'No depth data received'}, status=400)


def index(request):
    api_response = None
    api = KotakNeoAPI()

    if request.method == 'POST':
        if 'cancel_order_id' in request.POST:
            order_id = request.POST.get('cancel_order_id')
            api_response = api.cancel_order(order_id)
            if 'error' in api_response:
                messages.error(request, f"Cancellation failed: {api_response['error']}")
            else:
                messages.success(request, f"Order cancellation requested: {api_response.get('result', 'Success')}")


    # Fetch account information, holdings, limits, and order book for display.
    # These methods will trigger authentication on the first call if not already authenticated.
    account_info = api.get_account_info()
    holdings = api.get_holdings()
    raw_limits = api.get_limits()
    order_book = api.get_order_book()
    positions = api.get_positions()

    # Handle potential errors from the API calls to prevent page crashes
    if 'error' in account_info:
        messages.warning(request, f"Could not fetch account info: {account_info['error']}")
        account_info = {} # Reset to avoid template errors
    if isinstance(holdings, dict) and 'error' in holdings:
        messages.warning(request, f"Could not fetch holdings: {holdings['error']}")
        holdings = [] # Reset to avoid template errors
    if isinstance(positions, dict) and 'error' in positions:
        messages.warning(request, f"Could not fetch positions: {positions['error']}")
        positions = [] # Reset to avoid template errors
    
    limits = {}
    debug_limits = None
    if 'error' in raw_limits:
        messages.warning(request, f"Could not fetch limits: {raw_limits['error']}")
    else:
        # The limits response is a flat dictionary. Let's parse it directly.
        if isinstance(raw_limits, dict) and raw_limits.get('stat') == 'Ok':
            limits = {
            'available_trade': raw_limits.get('Net', '0.00'),
                'margin_used': raw_limits.get('MarginUsed', '0.00'),
                'collateral': raw_limits.get('CollateralValue', '0.00'),
                'total_cash': raw_limits.get('RmsPayInAmt', '0.00'),
                'unsettled_credit': raw_limits.get('CncSellcrdPresent', '0.00'), 
            }
        
        # If we couldn't parse it for any reason, show the debug info
        debug_limits = raw_limits


    if isinstance(order_book, dict) and 'error' in order_book:
        messages.warning(request, f"Could not fetch order book: {order_book['error']}")
        order_book = [] # Reset to avoid template errors

    # Process holdings and calculate portfolio summary
    processed_holdings = []
    portfolio_summary = {
        'total_invested': 0,
        'current_value': 0,
        'total_pnl': 0,
        'pnl_percentage': 0
    }
    
    if isinstance(holdings, list):
        for h in holdings:
            if not isinstance(h, dict):
                continue # Skip items that are not dictionaries

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
                    'tradingsymbol': h.get('displaySymbol', h.get('symbol', 'N/A')),
                    'quantity': qty,
                    'average_price': avg_price,
                    'last_price': last_price,
                    'pnl': pnl,
                })
            except (ValueError, TypeError):
                # Skip holding if data is malformed
                continue
        
        portfolio_summary['total_pnl'] = portfolio_summary['current_value'] - portfolio_summary['total_invested']
        if portfolio_summary['total_invested'] > 0:
            portfolio_summary['pnl_percentage'] = (portfolio_summary['total_pnl'] / portfolio_summary['total_invested']) * 100

    context = {
        'api_response': api_response,
        'account_info': account_info,
        'holdings': processed_holdings,
        'positions': positions,
        'limits': limits,
        'order_book': order_book,
        'portfolio_summary': portfolio_summary,
        'debug_limits': debug_limits,
        'is_connected': True if account_info and 'error' not in account_info else False,
    }

    return render(request, 'trades/index.html', context)
