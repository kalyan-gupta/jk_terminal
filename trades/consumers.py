import json
import logging
import uuid
from channels.generic.websocket import WebsocketConsumer
from django.contrib.auth.models import AnonymousUser
from .kotak_neo_api import KotakNeoAPI
from trading_platform.logging_utils import request_id_var, request_user_var

logger = logging.getLogger(__name__)

class LiveQuotesConsumer(WebsocketConsumer):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.api = None
        self.quote_cache = {} # Store last known values per instrument token

    def connect(self):
        # Assign a unique session ID to this websocket connection for tracing
        self.ws_session_id = f"WS-{str(uuid.uuid4())[:8]}"
        request_id_var.set(self.ws_session_id)
        
        user = self.scope.get('user', None)
        user_name = user.username if user and user.is_authenticated else "Anonymous"
        request_user_var.set(user_name)
        
        if not user or not hasattr(user, 'is_authenticated') or not user.is_authenticated:
            logger.warning(f"WebSocket connection rejected: Unauthenticated user.")
            self.close(code=4001)
            return

        try:
            session_key = self.scope.get('session').session_key if self.scope.get('session') else None
            self.api = KotakNeoAPI(user=user, session_id=session_key)
        except Exception as e:
            logger.error(f"WebSocket connection error: {e}")
            self.close(code=4002)
            return

        self.accept()
        auth_response = self.api.authenticate()
        if 'error' in auth_response:
            logger.error(f"WebSocket auth failure: {auth_response['error']}")
            self.send(text_data=json.dumps({'error': auth_response['error']}))
            self.close()
        else:
            logger.info(f"WebSocket connected and authenticated for user '{user_name}'")
            self.send(text_data=json.dumps({'message': 'Connected and authenticated'}))

    def disconnect(self, close_code):
        if hasattr(self.api, 'unsubscribe'):
            self.api.unsubscribe() # Assuming there's a method to clean up the subscription

    def receive(self, text_data):
        # Ensure context variables are set for this thread
        request_id_var.set(self.ws_session_id)
        user = self.scope.get('user')
        request_user_var.set(user.username if user and user.is_authenticated else "Anonymous")
        
        try:
            text_data_json = json.loads(text_data)
            action = text_data_json.get('action')
            params = text_data_json.get('params', {})
            
            if action == 'subscribe':
                instruments = params.get('instrument_tokens')
                isIndex = params.get('isIndex', False)
                isDepth = params.get('isDepth', False)
                if instruments:
                    logger.info(f"WebSocket action 'subscribe' for user '{self.scope.get('user')}': {instruments} (Depth: {isDepth})")
                    # To get both LTP (from 'sf' feed) and Depth (from 'dp' feed), 
                    # we perform a dual subscription when depth is requested.
                    self.api.subscribe(instruments, on_message=self.on_quote, isIndex=isIndex, isDepth=False)
                    if isDepth:
                        self.api.subscribe(instruments, on_message=self.on_quote, isIndex=isIndex, isDepth=True)
            elif action == 'unsubscribe':
                instruments = params.get('instrument_tokens')
                isIndex = params.get('isIndex', False)
                isDepth = params.get('isDepth', False)
                if instruments:
                    logger.info(f"WebSocket action 'unsubscribe' for user '{self.scope.get('user')}': {instruments}")
                    self.api.unsubscribe(instruments, isIndex=isIndex, isDepth=False)
                    if isDepth:
                        self.api.unsubscribe(instruments, isIndex=isIndex, isDepth=True)
            else:
                logger.warning(f"Unknown message type received: {action}")

        except json.JSONDecodeError:
            logger.error("Received non-JSON message")
        except Exception as e:
            logger.error(f"Error in receive method: {e}", exc_info=True)

    def on_quote(self, quote):
        """Callback function to handle incoming quotes from the API."""
        # Ensure context variables are set (SDK callbacks might be in different threads)
        request_id_var.set(self.ws_session_id)
        user = self.scope.get('user')
        request_user_var.set(user.username if user and user.is_authenticated else "Anonymous")
        
        try:
            # Flatten and normalize the payload
            # Kotak SDK usually wraps in {'type': 'stock_feed', 'data': [...]}
            normalized_list = []
            
            raw_data = []
            if isinstance(quote, dict):
                if quote.get('type') in ['stock_feed', 'depth_feed', 'index_feed'] and 'data' in quote:
                    raw_data = quote['data']
                else:
                    raw_data = [quote]
            elif isinstance(quote, list):
                raw_data = quote

            for item in raw_data:
                if not isinstance(item, dict): continue
                
                token = item.get('tk')
                if not token:
                    # If no token, we can't reliably cache it, but we still try to normalize it
                    # This might happen for some generic non-stock messages
                    continue
                
                # Initialize cache entry for this token if it doesn't exist
                if token not in self.quote_cache:
                    self.quote_cache[token] = {
                        'instrument_token': token,
                        'exchange_segment': item.get('e'),
                        'symbol': item.get('ts'),
                        'ltp': None,
                        'volume': None,
                        'open': None,
                        'high': None,
                        'low': None,
                        'close': None,
                        'atp': None,
                        'percent_change': None,
                        'depth': {
                            'buy': [{'price': None, 'quantity': None, 'orders': None} for _ in range(5)],
                            'sell': [{'price': None, 'quantity': None, 'orders': None} for _ in range(5)]
                        }
                    }
                
                cache = self.quote_cache[token]
                
                # Update basic fields if they are present in the current message
                field_mappings = {
                    'ltp': ['lp', 'ltp', 'last_traded_price'],
                    'volume': ['v', 'volume'],
                    'open': ['o', 'open'],
                    'high': ['h', 'high'],
                    'low': ['lo', 'low'],
                    'close': ['c', 'close'],
                    'atp': ['ap', 'average_price'],
                    'percent_change': ['pc', 'net_change_percentage'],
                    'symbol': ['ts'],
                    'exchange_segment': ['e']
                }
                
                for canonical_field, raw_keys in field_mappings.items():
                    val = None
                    for k in raw_keys:
                        if k in item:
                            val = item[k]
                            break
                    if val is not None:
                        cache[canonical_field] = val
                
                # Handle Depth Levels discretely
                depth_keys = [
                    ('buy', 0, 'bp', 'bq', 'bno1'), ('buy', 1, 'bp1', 'bq1', 'bno2'),
                    ('buy', 2, 'bp2', 'bq2', 'bno3'), ('buy', 3, 'bp3', 'bq3', 'bno4'),
                    ('buy', 4, 'bp4', 'bq4', 'bno5'),
                    ('sell', 0, 'sp', 'bs', 'sno1'), ('sell', 1, 'sp1', 'bs1', 'sno2'),
                    ('sell', 2, 'sp2', 'bs2', 'sno3'), ('sell', 3, 'sp3', 'bs3', 'sno4'),
                    ('sell', 4, 'sp4', 'bs4', 'sno5'),
                ]
                
                for side, idx, p_key, q_key, o_key in depth_keys:
                    p, q, o = item.get(p_key), item.get(q_key), item.get(o_key)
                    if p is not None: cache['depth'][side][idx]['price'] = p
                    if q is not None: cache['depth'][side][idx]['quantity'] = q
                    if o is not None: cache['depth'][side][idx]['orders'] = o

                # Also handle SDK-normalized depth if present
                if 'depth' in item:
                    d = item['depth']
                    for side in ['buy', 'sell']:
                        if side in d:
                            for idx, d_item in enumerate(d[side][:5]):
                                if d_item.get('price') is not None: cache['depth'][side][idx]['price'] = d_item.get('price')
                                if d_item.get('quantity') is not None: cache['depth'][side][idx]['quantity'] = d_item.get('quantity')
                                if d_item.get('orders') is not None: cache['depth'][side][idx]['orders'] = d_item.get('orders')
                
                # Add request_type if present (usually not cached as it varies by message)
                quote_to_send = cache.copy()
                quote_to_send['request_type'] = item.get('request_type')
                
                normalized_list.append(quote_to_send)

            if normalized_list:
                # Log a summary of the quote
                first = normalized_list[0]
                logger.debug(f"Quote received for {first.get('instrument_token')} ({first.get('symbol')}): LTP={first.get('ltp')}")
                
                # Forward the normalized quote to the connected client
                self.send(text_data=json.dumps({
                    'type': 'quote',
                    'data': normalized_list
                }))
        except Exception as e:
            logger.error(f"Error processing/sending quote to client: {e}", exc_info=True)

