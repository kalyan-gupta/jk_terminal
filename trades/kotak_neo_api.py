from neo_api_client import NeoAPI
from django.conf import settings
from django.utils import timezone
from datetime import timedelta
import logging
import requests
import os

logger = logging.getLogger(__name__)

class KotakNeoAPI:
    """
    Kotak Neo API handler - now supports per-user credentials.
    Each user has their own instance with their credentials.
    """

    _session_cache = {}
    
    def __init__(self, user=None, session_id=None, credentials=None):
        """
        Initialize the API handler with user credentials and session context.
        
        Args:
            user: Django User instance
            session_id: Unique Django session key to isolate SDK sessions per browser
            credentials: Dict with MPIN, CONSUMER_KEY, etc.
        """
        self.user = user
        self.user_id = user.id if user else None
        self.session_id = session_id or "global" # Fallback to global for background tasks
        self.cache_key = (self.user_id, self.session_id)
        self.is_authenticated = False
        self.login_data = None
        self.client = None
        
        # Get credentials from database if user provided, else use passed credentials
        if user:
            from trades.models import UserNeoCredentials
            try:
                user_creds = UserNeoCredentials.objects.get(user=user, is_active=True)
                self.credentials = user_creds.get_decrypted_credentials()
                self.user_credentials_obj = user_creds
            except UserNeoCredentials.DoesNotExist:
                raise Exception(f"No active Neo API credentials found for user {user.username}. Please configure your credentials.")
        elif credentials:
            self.credentials = credentials
            self.user_credentials_obj = None
        else:
            # Fallback to settings (for backward compatibility)
            self.credentials = settings.KOTAK_NEO_API_CREDENTIALS
            self.user_credentials_obj = None
        
        # Initialize the NeoAPI client
        if self.credentials and 'CONSUMER_KEY' in self.credentials:
            self.client = NeoAPI(environment='prod', consumer_key=self.credentials['CONSUMER_KEY'])
    
    def get_cached_session(self):
        """Return cached authenticated session data if still valid."""
        if not self.user_id:
            return None
        session_info = KotakNeoAPI._session_cache.get(self.cache_key)
        if not session_info:
            return None
        if session_info.get('expires_at') and timezone.now() < session_info['expires_at']:
            return session_info
        self.clear_cached_session()
        return None

    def cache_session(self, login_data, duration_seconds=1800):
        """Cache the authenticated client for the session duration."""
        if not self.user_id:
            return
        expires_at = timezone.now() + timedelta(seconds=duration_seconds)
        KotakNeoAPI._session_cache[self.cache_key] = {
            'client': self.client,
            'login_data': login_data,
            'expires_at': expires_at,
        }

    def clear_cached_session(self):
        """Remove any cached SDK session for this user session."""
        if not self.user_id:
            return
        KotakNeoAPI._session_cache.pop(self.cache_key, None)

    def authenticate(self, totp=None, force_refresh=False):
        """Authenticate with Kotak Neo API using a one-time TOTP code."""
        if self.user and not force_refresh:
            cached = self.get_cached_session()
            if cached:
                self.client = cached['client']
                self.login_data = cached['login_data']
                self.is_authenticated = True
                return {"status": "success", "message": "Already authenticated"}

        if not self.client:
            return {"error": "API client not initialized. Please configure your credentials."}

        if not totp:
            return {"error": "One-time TOTP code is required to authenticate the Neo SDK session."}

        try:
            logger.info(f"Attempting Kotak Neo API authentication for user {self.user.username if self.user else 'unknown'}...")

            login_response = self.client.totp_login(
                mobile_number=self.credentials['MOBILE_NUMBER'],
                ucc=self.credentials['UCC'],
                totp=totp
            )

            if isinstance(login_response, dict) and ('error' in login_response or 'Error Message' in login_response):
                return {"error": f"Login failed: {login_response}"}

            validate_response = self.client.totp_validate(mpin=self.credentials['MPIN'])

            if isinstance(validate_response, dict) and ('error' in validate_response or 'Error Message' in validate_response):
                return {"error": f"Validation failed: {validate_response}"}

            self.is_authenticated = True
            self.login_data = validate_response
            self.cache_session(login_data=validate_response)

            if self.user_credentials_obj:
                self.user_credentials_obj.mark_sdk_session_active(duration_seconds=1800)
                self.user_credentials_obj.last_used = timezone.now()
                self.user_credentials_obj.save()

            logger.info(f"Authentication successful for {self.user.username if self.user else 'user'}.")
            return {"status": "success", "message": "Authenticated successfully"}
        except Exception as e:
            logger.error(f"An error occurred during authentication: {e}", exc_info=True)
            self.is_authenticated = False
            return {"error": f"An error occurred during authentication: {e}"}

    def get_account_info(self):
        auth_response = self.authenticate()
        if 'error' in auth_response:
            return auth_response

        # The positions method seems to be the closest to getting account info
        try:
            positions = self.client.positions()
            if isinstance(positions, dict) and ('error' in positions or 'Error Message' in positions):
                return {"error": f"Could not fetch positions: {positions}"}

            # Try to get account name from credentials
            account_name = self.credentials.get('ACCOUNT_NAME', 'Your Account')
            if account_name == 'Your Account' and hasattr(self, 'login_data') and isinstance(self.login_data, dict):
                account_name = self.login_data.get('userName', self.login_data.get('clientName', account_name))

            logger.info(f"Fetched account info for '{account_name}' (UCC: {self.credentials.get('UCC', 'unknown')}).")
            return {"account_name": account_name, "account_id": self.credentials['UCC'], "positions": positions}
        except Exception as e:
            logger.error(f"Error fetching account info: {e}", exc_info=True)
            return {"error": f"An error occurred while fetching account info: {e}"}

    def get_holdings(self):
        auth_response = self.authenticate()
        if 'error' in auth_response:
            return auth_response

        try:
            holdings_response = self.client.holdings()
            logger.debug(f"Raw holdings response from SDK: {holdings_response}")
            if isinstance(holdings_response, dict):
                if 'error' in holdings_response or 'Error Message' in holdings_response:
                    return {"error": f"Could not fetch holdings: {holdings_response}"}
                # The holdings are in the 'data' key
                logger.debug("Successfully fetched holdings data (dict format).")
                return holdings_response.get('data', [])
            elif isinstance(holdings_response, list):
                # The client might have already extracted the list
                logger.debug("Successfully fetched holdings data (list format).")
                return holdings_response
            else:
                return []
        except Exception as e:
            logger.error(f"Error fetching holdings: {e}", exc_info=True)
            return {"error": f"An error occurred while fetching holdings: {e}"}

    def get_positions(self):
        auth_response = self.authenticate()
        if 'error' in auth_response:
            return auth_response
        try:
            positions = self.client.positions()
            if isinstance(positions, dict):
                if 'error' in positions or 'Error Message' in positions:
                    return {"error": f"Could not fetch positions: {positions}"}
                # The positions are in the 'data' key
                logger.debug("Successfully fetched positions data (dict format).")
                return positions.get('data', [])
            elif isinstance(positions, list):
                # The client might have already extracted the list
                logger.debug("Successfully fetched positions data (list format).")
                return positions
            else:
                return []
        except Exception as e:
            logger.error(f"Error fetching positions: {e}", exc_info=True)
            return {"error": f"An error occurred while fetching positions: {e}"}

    def get_limits(self):
        auth_response = self.authenticate()
        if 'error' in auth_response:
            return auth_response

        try:
            limits = self.client.limits(segment="ALL", exchange="ALL", product="ALL")
            if isinstance(limits, dict) and ('error' in limits or 'Error Message' in limits):
                return {"error": f"Could not fetch limits: {limits}"}

            logger.info("Successfully fetched limit information.")
            return limits
        except Exception as e:
            logger.error(f"Error fetching limits: {e}", exc_info=True)
            return {"error": f"An error occurred while fetching limits: {e}"}

    def get_order_book(self):
        auth_response = self.authenticate()
        if 'error' in auth_response:
            return auth_response

        try:
            report = self.client.order_report()
            if isinstance(report, dict) and ('error' in report or 'Error Message' in report):
                return {"error": f"Could not fetch order book: {report}"}

            logger.info("Successfully fetched order book report.")
            return report.get('data', [])
        except Exception as e:
            logger.error(f"Error fetching order book: {e}", exc_info=True)
            return {"error": f"An error occurred while fetching order book: {e}"}

    def cancel_order(self, order_id, is_verify=True):
        auth_response = self.authenticate()
        if 'error' in auth_response:
            return auth_response

        try:
            logger.info(f"Attempting to cancel order ID: {order_id}")
            result = self.client.cancel_order(order_id=str(order_id), isVerify=is_verify)
            logger.info(f"Cancel order successful: {result}")
            return result
        except Exception as e:
            logger.error(f"Error cancelling order: {e}", exc_info=True)
            return {"error": f"An error occurred while cancelling order: {e}"}

    def place_trade(self, trading_symbol, quantity, price, transaction_type,
                        exchange_segment='nse_cm', product='MIS', order_type='L', validity='DAY', amo='NO'):
        auth_response = self.authenticate()
        if 'error' in auth_response:
            return auth_response

        try:
            order_params = {
                'exchange_segment': exchange_segment,
                'product': product,
                'order_type': order_type,
                'quantity': str(quantity),
                'validity': validity,
                'trading_symbol': trading_symbol,
                'transaction_type': transaction_type[0].upper(),  # 'B' or 'S'
                'amo': amo
            }
            
            # Always include price, set to 0 for market orders
            order_params['price'] = str(price) if price is not None else '0'
            
            logger.info(f"Attempting to place trade: {order_params}")
            order = self.client.place_order(**order_params)
            logger.info(f"Place trade API returned: {order}")
            return order
        except Exception as e:
            logger.error(f"Error placing trade: {e}", exc_info=True)
            return {"error": f"An error occurred while placing trade: {e}"}

    def margin_required(self, instrument_token, quantity, price, transaction_type,
                        exchange_segment='nse_cm', product='MIS', order_type='L'):
        auth_response = self.authenticate()
        if 'error' in auth_response:
            return auth_response

        try:
            params = {
                'exchange_segment': exchange_segment,
                'product': product,
                'order_type': order_type,
                'quantity': str(quantity),
                'instrument_token': instrument_token,
                'transaction_type': transaction_type[0].upper(),
                'price': str(price if price is not None else 0)
            }
            logger.debug(f"Checking margin requirements: {params}")
            margin_result = self.client.margin_required(**params)
            logger.debug(f"Margin check returned: {margin_result}")
            return margin_result
        except Exception as e:
            logger.error(f"Error checking margin: {e}", exc_info=True)
            return {"error": f"An error occurred while checking margin: {e}"}

    def subscribe(self, instrument_tokens, on_message, isIndex=False, isDepth=False):
        auth_response = self.authenticate()
        if 'error' in auth_response:
            return auth_response
            
        try:
            self.client.on_message = on_message
            self.client.subscribe(instrument_tokens=instrument_tokens, isIndex=isIndex, isDepth=isDepth)
            logger.info(f"Subscribed to instruments: {instrument_tokens} (Index: {isIndex}, Depth: {isDepth})")
        except Exception as e:
            logger.error(f"Error subscribing to instruments: {e}", exc_info=True)
            return {"error": f"An error occurred during subscription: {e}"}

    def unsubscribe(self, instrument_tokens=[], isIndex=False, isDepth=False):
        try:
            self.client.un_subscribe(instrument_tokens=instrument_tokens, isIndex=isIndex, isDepth=isDepth)
            logger.info("Unsubscribed from all instruments.")
        except Exception as e:
            logger.error(f"Error unsubscribing: {e}", exc_info=True)

    def logout(self):
        """Logout the SDK session and clear the cached session."""
        try:
            if self.client and hasattr(self.client, 'logout'):
                self.client.logout()
        except Exception as e:
            logger.warning(f"SDK logout call failed: {e}", exc_info=True)

        if self.user_credentials_obj:
            self.user_credentials_obj.deactivate_sdk_session()

        self.clear_cached_session()
        self.is_authenticated = False
        self.login_data = None
        return {"status": "success", "message": "SDK session cleared."}

    def search_scrip(self, exchange_segment, symbol):
        auth_response = self.authenticate()
        if 'error' in auth_response:
            return auth_response
        
        try:
            return self.client.search_scrip(exchange_segment=exchange_segment, symbol=symbol)
        except Exception as e:
            logger.error(f"Error searching scrip: {e}", exc_info=True)
            return {"error": f"An error occurred while searching for scrips: {e}"}

    def quotes(self, instrument_tokens, quote_type=""):
        auth_response = self.authenticate()
        if 'error' in auth_response:
            return auth_response
        
        try:
            return self.client.quotes(instrument_tokens=instrument_tokens, quote_type=quote_type)
        except Exception as e:
            logger.error(f"Error fetching quotes: {e}", exc_info=True)
            return {"error": f"An error occurred while fetching quotes: {e}"}

    def scrip_master(self, exchange_segment=None):
        auth_response = self.authenticate()
        if 'error' in auth_response:
            return auth_response
        try:
            if exchange_segment:
                return self.client.scrip_master(exchange_segment=exchange_segment)
            return self.client.scrip_master()
        except Exception as e:
            logger.error(f"Error fetching scrip master: {e}", exc_info=True)
            return {"error": f"An error occurred while fetching scrip master: {e}"}

    def download_scrip_master(self, exchange_segment=None):
        scrip_master_data = self.scrip_master(exchange_segment)
        if 'error' in scrip_master_data:
            return scrip_master_data

        if 'filesPaths' not in scrip_master_data:
            return {"error": "No file paths found in scrip master data."}

        base_dir = os.path.join('trades', 'scrip_data')
        os.makedirs(base_dir, exist_ok=True)

        downloaded_files = []
        for file_url in scrip_master_data['filesPaths']:
            try:
                response = requests.get(file_url, stream=True)
                response.raise_for_status()  # Raise an exception for bad status codes

                file_name = os.path.join(base_dir, file_url.split('/')[-1])
                with open(file_name, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                downloaded_files.append(file_name)
            except requests.exceptions.RequestException as e:
                logger.error(f"Error downloading file {file_url}: {e}")
                return {"error": f"Failed to download file: {file_url}"}

        return {"status": "success", "downloaded_files": downloaded_files}


def logout_sdk_session_for_user(user, session_id=None):
    """Helper to clear any SDK session for the given user and session."""
    try:
        api = KotakNeoAPI(user=user, session_id=session_id)
        api.logout()
    except Exception as e:
        logger.warning(f"Failed to logout SDK session for user {user.username if user else 'unknown'}: {e}", exc_info=True)

    return {"status": "success", "message": "SDK session cleared."}
