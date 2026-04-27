from django.http import JsonResponse
from django.db.models import Max
from .models import BasketOrder
from .decorators import ajax_login_required
from .kotak_neo_api import KotakNeoAPI
import json
import logging

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
                    SELECT CAST(pSymbol AS VARCHAR) as pSymbol, pSymbolName, pTrdSymbol, pInstType, pDesc, dTickSize, lLotSize, pScripRefKey, pOptionType,
                    CAST(COALESCE("dStrikePrice;", 0) AS DECIMAL) / 100 as dStrikePrice
                    FROM active_market_data 
                    WHERE CAST(pSymbol AS VARCHAR) IN ({token_str})
                """).df().set_index('pSymbol').to_dict('index')
        except Exception as e:
            logger.error(f"DuckDB error in get_basket: {e}")
            metadata = {}

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
