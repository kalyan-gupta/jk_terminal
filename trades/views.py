from django.shortcuts import render
from .kotak_neo_api import KotakNeoAPI
from django.contrib import messages
from django.http import JsonResponse
import json

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


def place_trade_ajax(request):
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

        if not all([instrument_token, quantity, price, transaction_type, exchange_segment, product_type]):
            return JsonResponse({'error': 'All trade fields are required.'}, status=400)

        api = KotakNeoAPI()
        api_response = api.place_trade(
            trading_symbol=instrument_token,
            quantity=int(quantity),
            price=float(price),
            transaction_type=transaction_type,
            exchange_segment=exchange_segment,
            product=product_type
        )

        if 'error' in api_response:
            return JsonResponse({'status': 'error', 'message': api_response['error']}, status=400)
        
        order_id = api_response.get('norenordno', 'N/A')
        return JsonResponse({'status': 'success', 'message': f"Trade placed successfully! Order ID: {order_id}", 'data': api_response})

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
