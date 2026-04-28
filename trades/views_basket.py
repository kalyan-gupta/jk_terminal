from django.http import JsonResponse
from django.db.models import Max
from django.db import transaction
from .models import BasketOrder
from .decorators import ajax_login_required
from .kotak_neo_api import KotakNeoAPI
import json
import logging
import re

logger = logging.getLogger(__name__)

@ajax_login_required
def add_to_basket_ajax(request):
    if request.method != 'POST':
        return JsonResponse({'error': 'Only POST requests are allowed'}, status=405)
    
    try:
        data = json.loads(request.body)
        
        token = data.get('instrument_token')
        exch = data.get('exchange_segment')
        qty = int(data.get('quantity'))
        price = float(data.get('price', 0))
        ttype = data.get('transaction_type')
        ptype = data.get('product_type')
        otype = data.get('order_type', 'L')

        # Aggregation logic: Check for identical order
        existing = BasketOrder.objects.filter(
            user=request.user,
            instrument_token=token,
            exchange_segment=exch,
            transaction_type=ttype,
            product_type=ptype,
            order_type=otype,
            price=price
        ).first()

        if existing:
            existing.quantity += qty
            existing.save()
            logger.info(f"Updated quantity for {existing.trading_symbol} in basket.")
            return JsonResponse({'status': 'success', 'message': f"Updated quantity for {existing.trading_symbol}.", 'item_id': existing.id})
        
        # Get max sort_order for user
        max_order = BasketOrder.objects.filter(user=request.user).aggregate(Max('sort_order'))['sort_order__max']
        next_order = (max_order or 0) + 1
        
        basket_item = BasketOrder.objects.create(
            user=request.user,
            instrument_token=token,
            exchange_segment=exch,
            trading_symbol=data.get('trading_symbol'),
            quantity=qty,
            price=price,
            transaction_type=ttype,
            product_type=ptype,
            order_type=otype,
            sort_order=next_order
        )
        
        logger.info(f"User '{request.user.username}' added {basket_item.trading_symbol} to basket.")
        return JsonResponse({
            'status': 'success',
            'message': f"Added {basket_item.trading_symbol} to basket.",
            'item_id': basket_item.id
        })
    except Exception as e:
        logger.error(f"Error adding to basket: {e}")
        return JsonResponse({'error': str(e)}, status=400)

@ajax_login_required
def get_basket_ajax(request):
    orders = BasketOrder.objects.filter(user=request.user).order_by('sort_order', 'created_at')
    basket_data = []
    
    if orders.exists():
        # Get metadata for all tokens in basket from DuckDB shared memory connection
        from .views import _duckdb_connection, _duckdb_lock
        tokens = [o.instrument_token for o in orders]
        token_str = ", ".join([f"'{t}'" for t in tokens])
        
        try:
            with _duckdb_lock:
                # Fetch lot_size, tick_size, pDesc etc.
                metadata = _duckdb_connection.execute(f"""
                    SELECT CAST(pSymbol AS VARCHAR) as pSymbol, pSymbolName, pTrdSymbol, pInstType, pDesc, 
                    CAST(COALESCE(dTickSize, 0) AS DECIMAL) / 100 as dTickSize, lLotSize, pScripRefKey, pOptionType,
                    CAST(COALESCE("dStrikePrice;", 0) AS DECIMAL) / 100 as dStrikePrice
                    FROM active_market_data 
                    WHERE CAST(pSymbol AS VARCHAR) IN ({token_str})
                """).df().set_index('pSymbol').to_dict('index')
        except Exception as e:
            logger.error(f"DuckDB error in get_basket: {e}")
            metadata = {}

        # Fetch real-time circuit limits if SDK is active
        quotes_data = {}
        try:
            api = KotakNeoAPI(user=request.user, session_id=request.session.session_key)
            if api.get_cached_session():
                instrument_tokens = [{"instrument_token": o.instrument_token, "exchange_segment": o.exchange_segment} for o in orders]
                quotes = api.quotes(instrument_tokens=instrument_tokens, quote_type="all")
                if isinstance(quotes, dict) and 'data' in quotes:
                    quotes = quotes['data']

                if isinstance(quotes, list):
                    if len(quotes) > 0:
                        logger.debug(f"Basket quote sample: {quotes[0]}")
                    for q in quotes:
                        # Try all common token keys in Kotak API responses
                        tkn = str(q.get('instrumentToken') or q.get('instToken') or q.get('instrument_token') or q.get('pSymbol') or '')
                        if not tkn:
                            continue
                        quotes_data[tkn] = {
                            'lower_circuit': q.get('low_price_range') or q.get('lower_circuit') or q.get('lPriceRange'),
                            'upper_circuit': q.get('high_price_range') or q.get('upper_circuit') or q.get('hPriceRange'),
                            'ltp': q.get('ltp') or q.get('last_price') or q.get('lastPrice')
                        }
        except Exception as e:
            logger.warning(f"Failed to fetch quotes for basket: {e}")

        for o in orders:
            item = {
                'id': o.id,
                'instrument_token': o.instrument_token,
                'exchange_segment': o.exchange_segment,
                'trading_symbol': o.trading_symbol,
                'quantity': o.quantity,
                'price': o.price,
                'transaction_type': o.transaction_type,
                'product_type': o.product_type,
                'order_type': o.order_type,
                'sort_order': o.sort_order,
                'created_at': o.created_at.isoformat(),
            }
            # Add metadata if found
            meta = metadata.get(o.instrument_token, {})
            p_inst_type = meta.get('pInstType', '')
            
            # Use pScripRefKey as the primary display name as requested (more complete)
            item['display_name'] = meta.get('pScripRefKey') or meta.get('pSymbolName') or o.trading_symbol
            
            item['pInstType'] = p_inst_type
            item['desc'] = meta.get('pDesc', '')
            item['tick_size'] = float(meta.get('dTickSize', 0.05))
            item['lot_size'] = int(meta.get('lLotSize', 1))
            item['pScripRefKey'] = meta.get('pScripRefKey', '')
            item['pOptionType'] = meta.get('pOptionType', '')
            item['strike_price'] = float(meta.get('dStrikePrice') or 0)

            # Add quote data (circuits)
            q_data = quotes_data.get(str(o.instrument_token), {})
            item['lower_circuit'] = q_data.get('lower_circuit')
            item['upper_circuit'] = q_data.get('upper_circuit')
            item['last_price'] = q_data.get('ltp')
            
            basket_data.append(item)
            
    return JsonResponse({'status': 'success', 'basket': basket_data})

@ajax_login_required
def update_basket_item_ajax(request):
    if request.method != 'POST':
        return JsonResponse({'error': 'Only POST requests are allowed'}, status=405)
    
    try:
        data = json.loads(request.body)
        order_id = data.get('order_id')
        
        BasketOrder.objects.filter(user=request.user, id=order_id).update(
            quantity=int(data.get('quantity')),
            price=float(data.get('price', 0)),
            transaction_type=data.get('transaction_type'),
            product_type=data.get('product_type'),
            order_type=data.get('order_type')
        )
        return JsonResponse({'status': 'success', 'message': 'Basket item updated.'})
    except Exception as e:
        logger.error(f"Error updating basket item: {e}")
        return JsonResponse({'error': str(e)}, status=400)

@ajax_login_required
def remove_from_basket_ajax(request):
    if request.method != 'POST':
        return JsonResponse({'error': 'Only POST requests are allowed'}, status=405)
    
    try:
        data = json.loads(request.body)
        order_id = data.get('order_id')
        BasketOrder.objects.filter(user=request.user, id=order_id).delete()
        return JsonResponse({'status': 'success', 'message': 'Item removed from basket.'})
    except Exception as e:
        return JsonResponse({'error': str(e)}, status=400)

@ajax_login_required
def clear_basket_ajax(request):
    if request.method != 'POST':
        return JsonResponse({'error': 'Only POST requests are allowed'}, status=405)
    
    try:
        BasketOrder.objects.filter(user=request.user).delete()
        return JsonResponse({'status': 'success', 'message': 'Basket cleared successfully.'})
    except Exception as e:
        return JsonResponse({'error': str(e)}, status=400)

@ajax_login_required
def update_basket_sequence_ajax(request):
    if request.method != 'POST':
        return JsonResponse({'error': 'Only POST requests are allowed'}, status=405)
    
    try:
        data = json.loads(request.body)
        sequence = data.get('sequence', []) # List of {id, sort_order}
        
        for item in sequence:
            BasketOrder.objects.filter(user=request.user, id=item['id']).update(sort_order=item['sort_order'])
            
        return JsonResponse({'status': 'success', 'message': 'Basket sequence updated.'})
    except Exception as e:
        return JsonResponse({'error': str(e)}, status=400)

@ajax_login_required
def execute_basket_ajax(request):
    if request.method != 'POST':
        return JsonResponse({'error': 'Only POST requests are allowed'}, status=405)
    
    orders = BasketOrder.objects.filter(user=request.user).order_by('sort_order', 'created_at')
    if not orders.exists():
        return JsonResponse({'error': 'Basket is empty.'}, status=400)
    
    try:
        api = KotakNeoAPI(user=request.user, session_id=request.session.session_key)
    except Exception as e:
        return JsonResponse({'error': f"Failed to initialize API: {str(e)}"}, status=400)
    
    results = []
    failed = False
    error_message = ""
    
    for order in orders:
        logger.info(f"Executing basket order: {order}")
        
        try:
            # Place the trade
            api_response = api.place_trade(
                trading_symbol=order.trading_symbol,
                quantity=order.quantity,
                price=order.price if order.order_type == 'L' else 0,
                transaction_type=order.transaction_type,
                exchange_segment=order.exchange_segment,
                product=order.product_type,
                order_type=order.order_type
            )
            
            if isinstance(api_response, dict) and 'error' in api_response:
                failed = True
                error_message = api_response['error']
                results.append({'id': order.id, 'status': 'error', 'message': error_message})
                break
            
            if 'errMsg' in api_response:
                failed = True
                error_message = api_response['errMsg']
                results.append({'id': order.id, 'status': 'error', 'message': error_message})
                break
            
            # Success: Remove from basket and continue
            results.append({'id': order.id, 'status': 'success', 'order_id': api_response.get('nOrdNo', 'N/A')})
            order.delete()
            
        except Exception as e:
            failed = True
            error_message = str(e)
            results.append({'id': order.id, 'status': 'error', 'message': error_message})
            break
            
    if failed:
        return JsonResponse({
            'status': 'partial_failure',
            'message': f"Execution stopped at {order.trading_symbol}: {error_message}",
            'results': results
        }, status=400)
    
    return JsonResponse({
        'status': 'success',
        'message': f"All {len(results)} orders in basket executed successfully.",
        'results': results
    })

@ajax_login_required
def check_basket_margin_ajax(request):
    """Calculate required margin for all items in the basket."""
    orders = BasketOrder.objects.filter(user=request.user).order_by('sort_order')
    if not orders.exists():
        return JsonResponse({'error': 'Basket is empty.'}, status=400)
    
    try:
        api = KotakNeoAPI(user=request.user, session_id=request.session.session_key)
    except Exception as e:
        return JsonResponse({'error': f"Failed to initialize API: {str(e)}"}, status=400)
    
    margins = {}
    total_margin = 0
    
    for order in orders:
        try:
            # Note: We use price=0 for MKT orders as per check_margin_ajax in views.py
            margin_response = api.margin_required(
                instrument_token=order.instrument_token,
                quantity=order.quantity,
                price=order.price if order.order_type == 'L' else 0,
                transaction_type=order.transaction_type,
                exchange_segment=order.exchange_segment,
                product=order.product_type,
                order_type=order.order_type
            )
            
            if isinstance(margin_response, dict) and 'data' in margin_response:
                data = margin_response.get('data')
                # Data can be a list or a dict depending on the exact response
                if isinstance(data, list) and len(data) > 0:
                    item_data = data[0]
                elif isinstance(data, dict):
                    item_data = data
                else:
                    item_data = {}
                
                # reqdMrgn or ordMrgn are typical fields in Neo API for required margin
                m_val = float(item_data.get('reqdMrgn') or item_data.get('ordMrgn') or 0)
                margins[order.id] = m_val
                total_margin += m_val
            else:
                margins[order.id] = "Error"
        except Exception as e:
            logger.error(f"Error checking margin for basket item {order.id}: {e}")
            margins[order.id] = "Error"
            
    return JsonResponse({
        'status': 'success', 
        'margins': margins, 
        'total_margin': total_margin
    })

@ajax_login_required
def reorder_basket_ajax(request):
    """Automatically reorder basket items: Buy before Sell for hedges."""
    if request.method != 'POST':
        return JsonResponse({'error': 'Only POST requests are allowed'}, status=405)
    
    orders = list(BasketOrder.objects.filter(user=request.user))
    if not orders:
        return JsonResponse({'status': 'success', 'message': 'Basket is empty.'})

    # Fetch metadata for sorting (expiry, strike, etc.)
    from .views import _duckdb_connection, _duckdb_lock
    tokens = [o.instrument_token for o in orders]
    token_str = ", ".join([f"'{t}'" for t in tokens])
    
    try:
        with _duckdb_lock:
            # Extract underlying from pScripRefKey or pSymbolName
            metadata = _duckdb_connection.execute(f"""
                SELECT CAST(pSymbol AS VARCHAR) as pSymbol, pScripRefKey, pSymbolName, pOptionType,
                CAST(COALESCE("dStrikePrice;", 0) AS DECIMAL) / 100 as dStrikePrice,
                try_strptime(regexp_extract(COALESCE(pScripRefKey, ''), '(\\d{{2}}[A-Z]{{3}}\\d{{2}})', 1), '%d%b%y') as expire_date
                FROM active_market_data 
                WHERE CAST(pSymbol AS VARCHAR) IN ({token_str})
            """).df().set_index('pSymbol').to_dict('index')
    except Exception as e:
        logger.error(f"DuckDB error in reorder_basket: {e}")
        metadata = {}

    def sort_key(order):
        meta = metadata.get(order.instrument_token, {})
        
        # 1. Underlying (e.g. NIFTY, BANKNIFTY)
        # We can try to extract the first few non-digit characters from pScripRefKey
        ref_key = meta.get('pScripRefKey', '')
        # Simple heuristic: take characters before the first digit
        match = re.match(r'^([A-Z]+)', ref_key)
        underlying = match.group(1) if match else (meta.get('pSymbolName') or order.trading_symbol)
        
        # 2. Expiry date
        expiry = meta.get('expire_date')
        expiry_val = expiry.to_pydatetime() if hasattr(expiry, 'to_pydatetime') else (expiry or "")
        
        # 3. Transaction type (Buy 'B' = 0, Sell 'S' = 1)
        side_priority = 0 if order.transaction_type == 'B' else 1
        
        # 4. Strike Price
        strike = float(meta.get('dStrikePrice') or 0)
        
        # 5. Option Type (CE before PE if same strike?)
        opt_type = meta.get('pOptionType', '')
        opt_priority = 0 if opt_type == 'CE' else 1
        
        return (underlying, expiry_val, side_priority, strike, opt_priority)

    orders.sort(key=sort_key)
    
    # Update sort_order in DB
    try:
        with transaction.atomic():
            for i, order in enumerate(orders):
                order.sort_order = i + 1
                order.save()
    except Exception as e:
        return JsonResponse({'error': f"Failed to save new sequence: {str(e)}"}, status=500)
        
    return JsonResponse({'status': 'success', 'message': 'Basket reordered successfully (Buy before Sell).'})
