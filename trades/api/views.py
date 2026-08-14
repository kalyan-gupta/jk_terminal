from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status, permissions, viewsets
from rest_framework_simplejwt.tokens import RefreshToken
from django.contrib.auth import authenticate
from django.contrib.auth.models import User
from django.utils import timezone
import logging

from ..models import UserNeoCredentials, SessionActivity, BasketOrder, TrackedOrder, ActiveMarketData
from ..kotak_neo_api import KotakNeoAPI
from ..views.helpers import _process_holdings_data, _process_positions_data, _process_limits_data
from ..views.helpers import _check_scrip_status_logic
from ..views.market import get_p_group_description
from .serializers import (

    UserSerializer, RegisterSerializer, UserNeoCredentialsSerializer,
    BasketOrderSerializer, TrackedOrderSerializer, ActiveMarketDataSerializer
)

logger = logging.getLogger(__name__)

def get_api_session_id(user):
    return f"api_{user.username}"

def get_or_create_session_activity(user, session_id, request=None):
    ip_address = '0.0.0.0'
    if request:
        from .permissions import IsSessionValidAndPasswordChangeNotRequired
        ip_address = IsSessionValidAndPasswordChangeNotRequired.get_client_ip(request)
        
    activity, created = SessionActivity.objects.get_or_create(
        user=user,
        session_key=session_id,
        defaults={'ip_address': ip_address}
    )
    if not created:
        activity.last_activity = timezone.now()
        if request and ip_address != '0.0.0.0':
            activity.ip_address = ip_address
            activity.save(update_fields=['last_activity', 'ip_address'])
        else:
            activity.save(update_fields=['last_activity'])
    return activity

def get_neo_api_instance(user, request=None):
    session_id = get_api_session_id(user)
    get_or_create_session_activity(user, session_id, request=request)
    return KotakNeoAPI(user=user, session_id=session_id)


class RegisterAPIView(APIView):
    permission_classes = [permissions.AllowAny]

    def post(self, request):
        serializer = RegisterSerializer(data=request.data)
        if serializer.is_valid():
            user = serializer.save()
            refresh = RefreshToken.for_user(user)
            return Response({
                'user': UserSerializer(user).data,
                'refresh': str(refresh),
                'access': str(refresh.access_token),
            }, status=status.HTTP_201_CREATED)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)


class UserProfileAPIView(APIView):
    def get(self, request):
        serializer = UserSerializer(request.user)
        return Response(serializer.data)

    def put(self, request):
        serializer = UserSerializer(request.user, data=request.data, partial=True)
        if serializer.is_valid():
            serializer.save()
            return Response(serializer.data)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)


class UserNeoCredentialsAPIView(APIView):
    def get(self, request):
        try:
            creds = UserNeoCredentials.objects.get(user=request.user, is_active=True)
            serializer = UserNeoCredentialsSerializer(creds)
            return Response(serializer.data)
        except UserNeoCredentials.DoesNotExist:
            return Response({'detail': 'No credentials configured.'}, status=status.HTTP_404_NOT_FOUND)

    def post(self, request):
        try:
            creds = UserNeoCredentials.objects.get(user=request.user)
            serializer = UserNeoCredentialsSerializer(creds, data=request.data, partial=True)
        except UserNeoCredentials.DoesNotExist:
            serializer = UserNeoCredentialsSerializer(data=request.data)
        
        if serializer.is_valid():
            serializer.save(user=request.user, is_active=True)
            return Response(serializer.data, status=status.HTTP_200_OK if 'creds' in locals() else status.HTTP_201_CREATED)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)


class CheckSDKStatusAPIView(APIView):
    def get(self, request):
        session_id = get_api_session_id(request.user)
        try:
            activity = SessionActivity.objects.get(session_key=session_id)
            is_valid = activity.is_sdk_session_valid()
            expires_at = activity.sdk_session_expires_at.isoformat() if activity.sdk_session_expires_at else None
            return Response({
                'sdk_session_active': is_valid,
                'sdk_session_expires_at': expires_at
            })
        except SessionActivity.DoesNotExist:
            return Response({
                'sdk_session_active': False,
                'sdk_session_expires_at': None
            })


class AuthenticateSDKAPIView(APIView):
    def post(self, request):
        totp = request.data.get('totp')
        force_refresh = request.data.get('force_refresh', False)
        try:
            api = get_neo_api_instance(request.user)
            auth_response = api.authenticate(totp=totp, force_refresh=force_refresh)
            if 'error' in auth_response:
                return Response(auth_response, status=status.HTTP_400_BAD_REQUEST)
            return Response(auth_response)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_400_BAD_REQUEST)


class ExtendSDKSessionAPIView(APIView):
    def post(self, request):
        try:
            api = get_neo_api_instance(request.user)
            success, message = api.extend_session()
            if not success:
                return Response({'error': message}, status=status.HTTP_400_BAD_REQUEST)
            return Response({'status': 'success', 'expires_at': message})
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_400_BAD_REQUEST)


class LogoutSDKAPIView(APIView):
    def post(self, request):
        try:
            api = get_neo_api_instance(request.user)
            api.clear_cached_session()
            session_id = get_api_session_id(request.user)
            try:
                activity = SessionActivity.objects.get(session_key=session_id)
                activity.deactivate_sdk_session()
            except SessionActivity.DoesNotExist:
                pass
            return Response({'status': 'success', 'message': 'SDK session logged out.'})
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_400_BAD_REQUEST)


class OrderBookAPIView(APIView):
    def get(self, request):
        try:
            api = get_neo_api_instance(request.user)
            orders_response = api.get_order_report()
            if isinstance(orders_response, dict) and 'error' in orders_response:
                if 'One-time TOTP code is required' in orders_response['error']:
                    return Response({'error': 'Trade session expired.', 'status': 'reauth_required'}, status=status.HTTP_401_UNAUTHORIZED)
                return Response({'error': orders_response['error']}, status=status.HTTP_400_BAD_REQUEST)
            
            return Response(orders_response)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_400_BAD_REQUEST)

    def post(self, request):
        data = request.data
        instrument_token = data.get('instrument_token')
        trading_symbol = data.get('trading_symbol')
        quantity = data.get('quantity')
        price = data.get('price')
        transaction_type = data.get('transaction_type')
        exchange_segment = data.get('exchange_segment')
        product_type = data.get('product_type')
        order_type = data.get('order_type', 'L')

        if not all([instrument_token, trading_symbol, quantity, transaction_type, exchange_segment, product_type]):
            return Response({'error': 'Required fields are missing.'}, status=status.HTTP_400_BAD_REQUEST)

        try:
            api = get_neo_api_instance(request.user)
            if order_type == 'MKT':
                price = 0
            
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
                    return Response({'error': 'Trade session expired.', 'status': 'reauth_required'}, status=status.HTTP_401_UNAUTHORIZED)
                return Response({'error': api_response['error']}, status=status.HTTP_400_BAD_REQUEST)

            if 'errMsg' in api_response:
                return Response({'error': api_response['errMsg']}, status=status.HTTP_400_BAD_REQUEST)

            order_id = api_response.get('nOrdNo', 'N/A')
            return Response({'status': 'success', 'order_id': order_id, 'data': api_response})
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_400_BAD_REQUEST)

    def put(self, request):
        data = request.data
        order_id = data.get('order_id')
        trading_symbol = data.get('trading_symbol')
        quantity = data.get('quantity')
        price = data.get('price')
        transaction_type = data.get('transaction_type')
        exchange_segment = data.get('exchange_segment')
        product_type = data.get('product_type')
        order_type = data.get('order_type', 'L')

        if not all([order_id, trading_symbol, quantity, price, transaction_type, exchange_segment, product_type]):
            return Response({'error': 'Required fields are missing.'}, status=status.HTTP_400_BAD_REQUEST)

        try:
            api = get_neo_api_instance(request.user)
            api_response = api.modify_order(
                order_id=order_id,
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
                    return Response({'error': 'Trade session expired.', 'status': 'reauth_required'}, status=status.HTTP_401_UNAUTHORIZED)
                return Response({'error': api_response['error']}, status=status.HTTP_400_BAD_REQUEST)

            if 'errMsg' in api_response:
                return Response({'error': api_response['errMsg']}, status=status.HTTP_400_BAD_REQUEST)

            return Response({'status': 'success', 'data': api_response})
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_400_BAD_REQUEST)

    def delete(self, request):
        order_id = request.query_params.get('order_id')
        if not order_id:
            return Response({'error': 'order_id query parameter is required.'}, status=status.HTTP_400_BAD_REQUEST)

        try:
            api = get_neo_api_instance(request.user)
            api_response = api.cancel_order(order_id=order_id)
            if isinstance(api_response, dict) and 'error' in api_response:
                if 'One-time TOTP code is required' in api_response['error']:
                    return Response({'error': 'Trade session expired.', 'status': 'reauth_required'}, status=status.HTTP_401_UNAUTHORIZED)
                return Response({'error': api_response['error']}, status=status.HTTP_400_BAD_REQUEST)

            if 'errMsg' in api_response:
                return Response({'error': api_response['errMsg']}, status=status.HTTP_400_BAD_REQUEST)

            return Response({'status': 'success', 'data': api_response})
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_400_BAD_REQUEST)


class CheckMarginAPIView(APIView):
    def post(self, request):
        data = request.data
        instrument_token = data.get('instrument_token')
        quantity = data.get('quantity')
        price = data.get('price', 0)
        transaction_type = data.get('transaction_type')
        exchange_segment = data.get('exchange_segment')
        product_type = data.get('product_type')
        order_type = data.get('order_type', 'L')

        if not all([instrument_token, quantity, transaction_type, exchange_segment, product_type]):
            return Response({'error': 'Required fields are missing.'}, status=status.HTTP_400_BAD_REQUEST)

        try:
            api = get_neo_api_instance(request.user)
            margin_response = api.margin_required(
                instrument_token=instrument_token,
                quantity=quantity,
                price=0 if order_type == 'MKT' else price,
                transaction_type=transaction_type,
                exchange_segment=exchange_segment,
                product=product_type,
                order_type=order_type
            )
            return Response(margin_response)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_400_BAD_REQUEST)


class HoldingsAPIView(APIView):
    def get(self, request):
        try:
            api = get_neo_api_instance(request.user)
            raw_holdings = api.get_holdings()
            if isinstance(raw_holdings, dict) and 'error' in raw_holdings:
                return Response({'error': raw_holdings['error']}, status=status.HTTP_400_BAD_REQUEST)
            processed = _process_holdings_data(raw_holdings)
            return Response({'holdings': processed})
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_400_BAD_REQUEST)


class PositionsAPIView(APIView):
    def get(self, request):
        try:
            api = get_neo_api_instance(request.user)
            raw_positions = api.get_positions()
            if isinstance(raw_positions, dict) and 'error' in raw_positions:
                return Response({'error': raw_positions['error']}, status=status.HTTP_400_BAD_REQUEST)
            processed = _process_positions_data(raw_positions)
            return Response({'positions': processed})
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_400_BAD_REQUEST)


class LimitsAPIView(APIView):
    def get(self, request):
        try:
            api = get_neo_api_instance(request.user)
            raw_limits = api.get_limits()
            if isinstance(raw_limits, dict) and 'error' in raw_limits:
                return Response({'error': raw_limits['error']}, status=status.HTTP_400_BAD_REQUEST)
            processed = _process_limits_data(raw_limits)
            return Response({'limits': processed})
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_400_BAD_REQUEST)


class BasketOrderViewSet(viewsets.ModelViewSet):
    serializer_class = BasketOrderSerializer

    def get_queryset(self):
        return BasketOrder.objects.filter(user=self.request.user)

    def perform_create(self, serializer):
        serializer.save(user=self.request.user)


class ExecuteBasketAPIView(APIView):
    def post(self, request):
        basket_ids = request.data.get('basket_ids', [])
        if not basket_ids:
            return Response({'error': 'No basket item IDs provided.'}, status=status.HTTP_400_BAD_REQUEST)

        try:
            api = get_neo_api_instance(request.user)
            results = []
            for item_id in basket_ids:
                try:
                    item = BasketOrder.objects.get(id=item_id, user=request.user)
                    res = api.place_trade(
                        trading_symbol=item.trading_symbol,
                        quantity=item.quantity,
                        price=item.price,
                        transaction_type=item.transaction_type,
                        exchange_segment=item.exchange_segment,
                        product=item.product_type,
                        order_type=item.order_type
                    )
                    results.append({'id': item_id, 'status': 'success', 'data': res})
                except BasketOrder.DoesNotExist:
                    results.append({'id': item_id, 'status': 'error', 'message': 'Basket item not found'})
                except Exception as ex:
                    results.append({'id': item_id, 'status': 'error', 'message': str(ex)})

            return Response({'results': results})
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_400_BAD_REQUEST)


class MarketSearchAPIView(APIView):
    def get(self, request):
        query = request.query_params.get('q', '')
        exchange = request.query_params.get('exchange', 'all')
        inst_type = request.query_params.get('inst_type', 'all')
        if not query:
            return Response({'error': 'Search query parameter "q" is required.'}, status=status.HTTP_400_BAD_REQUEST)
        
        from ..views.market import perform_market_search_cache
        data, err = perform_market_search_cache(query, exchange, inst_type)
        if err:
            return Response({'error': err}, status=status.HTTP_400_BAD_REQUEST)
            
        return Response({
            'results': data,
            'count': len(data),
            'total_available': min(50, len(data))
        })


class LTPAPIView(APIView):
    def get(self, request):
        p_symbol = request.query_params.get('p_symbol')
        p_exch_seg = request.query_params.get('p_exch_seg')
        if not all([p_symbol, p_exch_seg]):
            return Response({'error': 'p_symbol and p_exch_seg parameters are required.'}, status=status.HTTP_400_BAD_REQUEST)
        
        try:
            api = get_neo_api_instance(request.user)
            instrument_tokens = [{"instrument_token": p_symbol, "exchange_segment": p_exch_seg}]
            result = api.quotes(instrument_tokens=instrument_tokens, quote_type="all")
            
            if 'error' in result:
                return Response({'error': result['error']}, status=status.HTTP_400_BAD_REQUEST)

            if isinstance(result, list) and len(result) > 0:
                quote = result[0]
                return Response({
                    'ltp': quote.get('ltp'),
                    'lower_circuit': quote.get('low_price_range'),
                    'upper_circuit': quote.get('high_price_range')
                })
            return Response({'error': 'No quote data received'}, status=status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_400_BAD_REQUEST)


class DepthAPIView(APIView):
    def get(self, request):
        p_symbol = request.query_params.get('p_symbol')
        p_exch_seg = request.query_params.get('p_exch_seg')
        if not all([p_symbol, p_exch_seg]):
            return Response({'error': 'p_symbol and p_exch_seg parameters are required.'}, status=status.HTTP_400_BAD_REQUEST)
        
        try:
            api = get_neo_api_instance(request.user)
            instrument_tokens = [{"instrument_token": p_symbol, "exchange_segment": p_exch_seg}]
            result = api.quotes(instrument_tokens=instrument_tokens, quote_type="all")

            if 'error' in result:
                return Response({'error': result['error']}, status=status.HTTP_400_BAD_REQUEST)

            if isinstance(result, list) and len(result) > 0:
                quote = result[0]
                depth_data = {
                    'ltp': quote.get('ltp'),
                    'buy_depth': quote.get('depth', {}).get('buy', []),
                    'sell_depth': quote.get('depth', {}).get('sell', [])
                }
                return Response(depth_data)
            return Response({'error': 'No depth data received'}, status=status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_400_BAD_REQUEST)


class OptionChainAPIView(APIView):
    def get(self, request):
        p_symbol = request.query_params.get('p_symbol')
        if not p_symbol:
            return Response({'error': 'Missing p_symbol parameter.'}, status=status.HTTP_400_BAD_REQUEST)

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
            
            final_chain = {}
            for exp in expiries:
                sorted_strikes = []
                for strike in sorted(chain_data[exp].keys()):
                    strike_row = chain_data[exp][strike]
                    strike_row['strike'] = float(strike)
                    sorted_strikes.append(strike_row)
                final_chain[exp] = sorted_strikes

            return Response({
                'status': 'success',
                'expiries': expiries,
                'chain': final_chain
            })
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class ScripInfoAPIView(APIView):
    def get(self, request):
        token = request.query_params.get('token')
        exch = request.query_params.get('exch')
        if not token or not exch:
            return Response({'error': 'Missing token or exch parameter.'}, status=status.HTTP_400_BAD_REQUEST)

        try:
            scrip = ActiveMarketData.objects.filter(symbol=token, exch_seg=exch).first()
            if not scrip:
                return Response({'error': 'Scrip not found in cache'}, status=status.HTTP_404_NOT_FOUND)
            
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
            return Response({'status': 'success', 'data': data})
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class RefreshScripMasterAPIView(APIView):
    def get(self, request):
        from ..views.market import refresh_scrip_master
        return refresh_scrip_master(request)


class RefreshScripCacheAPIView(APIView):
    def get(self, request):
        from ..views.market import refresh_scrip_cache
        return refresh_scrip_cache(request)

