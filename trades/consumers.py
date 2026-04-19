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
            self.api = KotakNeoAPI(user=user)
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
                    self.api.subscribe(instruments, on_message=self.on_quote, isIndex=isIndex, isDepth=isDepth)
            elif action == 'unsubscribe':
                instruments = params.get('instrument_tokens')
                isIndex = params.get('isIndex', False)
                isDepth = params.get('isDepth', False)
                if instruments:
                    logger.info(f"WebSocket action 'unsubscribe' for user '{self.scope.get('user')}': {instruments}")
                    self.api.unsubscribe(instruments, isIndex=isIndex, isDepth=isDepth)
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
                
                normalizeditem = {
                    'instrument_token': item.get('tk'),
                    'exchange_segment': item.get('e'),
                    'symbol': item.get('ts'),
                    'ltp': item.get('lp') or item.get('ltp') or item.get('last_traded_price'),
                    'volume': item.get('v') or item.get('volume'),
                    'open': item.get('o') or item.get('open'),
                    'high': item.get('h') or item.get('high'),
                    'low': item.get('lo') or item.get('low'),
                    'close': item.get('c') or item.get('close'),
                    'atp': item.get('ap') or item.get('average_price'),
                    'percent_change': item.get('pc') or item.get('net_change_percentage'),
                    'request_type': item.get('request_type')
                }
                
                # Handle Depth (using exact keys from NeoWebSocket.py depth_resp_mapping)
                if any(k in item for k in ['bp', 'sp', 'bq', 'bs']):
                    normalizeditem['depth'] = {
                        'buy': [
                            {'price': item.get('bp'), 'quantity': item.get('bq'), 'orders': item.get('bno1')},
                            {'price': item.get('bp1'), 'quantity': item.get('bq1'), 'orders': item.get('bno2')},
                            {'price': item.get('bp2'), 'quantity': item.get('bq2'), 'orders': item.get('bno3')},
                            {'price': item.get('bp3'), 'quantity': item.get('bq3'), 'orders': item.get('bno4')},
                            {'price': item.get('bp4'), 'quantity': item.get('bq4'), 'orders': item.get('bno5')},
                        ],
                        'sell': [
                            {'price': item.get('sp'), 'quantity': item.get('bs'), 'orders': item.get('sno1')},
                            {'price': item.get('sp1'), 'quantity': item.get('bs1'), 'orders': item.get('sno2')},
                            {'price': item.get('sp2'), 'quantity': item.get('bs2'), 'orders': item.get('sno3')},
                            {'price': item.get('sp3'), 'quantity': item.get('bs3'), 'orders': item.get('sno4')},
                            {'price': item.get('sp4'), 'quantity': item.get('bs4'), 'orders': item.get('sno5')},
                        ]
                    }
                elif 'depth' in item:
                    # If it's already mapped by the SDK (e.g. in SNAP messages)
                    d = item['depth']
                    normalizeditem['depth'] = {
                        'buy': [{'price': b.get('price'), 'quantity': b.get('quantity'), 'orders': b.get('orders')} for b in d.get('buy', [])],
                        'sell': [{'price': s.get('price'), 'quantity': s.get('quantity'), 'orders': s.get('orders')} for s in d.get('sell', [])]
                    }
                
                normalized_list.append(normalizeditem)

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

