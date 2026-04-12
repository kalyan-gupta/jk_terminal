from neo_api_client import NeoAPI
from django.conf import settings
import pyotp
import logging
import requests
import os

logger = logging.getLogger(__name__)

class KotakNeoAPI:
    def __init__(self):
        self.credentials = settings.KOTAK_NEO_API_CREDENTIALS
        self.client = NeoAPI(environment='prod', consumer_key=self.credentials['CONSUMER_KEY'])
        self.is_authenticated = False

    def generate_totp(self):
        totp = pyotp.TOTP(self.credentials['TOTP_SECRET'])
        return totp.now()

    def authenticate(self):
        if self.is_authenticated:
            return {"status": "success", "message": "Already authenticated"}
        try:
            logger.info("Attempting Kotak Neo API authentication...")
            login_response = self.client.totp_login(mobile_number=self.credentials['MOBILE_NUMBER'], 
                                   ucc=self.credentials['UCC'], 
                                   totp=self.generate_totp())
            
            if isinstance(login_response, dict) and ('error' in login_response or 'Error Message' in login_response):
                return {"error": f"Login failed: {login_response}"}

            validate_response = self.client.totp_validate(mpin=self.credentials['MPIN'])
            
            if isinstance(validate_response, dict) and ('error' in validate_response or 'Error Message' in validate_response):
                return {"error": f"Validation failed: {validate_response}"}

            self.is_authenticated = True
            self.login_data = validate_response
            logger.info("Authentication successful.")
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

            # Try to get account name from settings or login data
            account_name = self.credentials.get('ACCOUNT_NAME', 'Your Account')
            if account_name == 'Your Account' and hasattr(self, 'login_data') and isinstance(self.login_data, dict):
                account_name = self.login_data.get('userName', self.login_data.get('clientName', account_name))

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
            if isinstance(holdings_response, dict):
                if 'error' in holdings_response or 'Error Message' in holdings_response:
                    return {"error": f"Could not fetch holdings: {holdings_response}"}
                # The holdings are in the 'data' key
                return holdings_response.get('data', [])
            elif isinstance(holdings_response, list):
                # The client might have already extracted the list
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
                return positions.get('data', [])
            elif isinstance(positions, list):
                # The client might have already extracted the list
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

            return report.get('data', [])
        except Exception as e:
            logger.error(f"Error fetching order book: {e}", exc_info=True)
            return {"error": f"An error occurred while fetching order book: {e}"}

    def cancel_order(self, order_id, is_verify=True):
        auth_response = self.authenticate()
        if 'error' in auth_response:
            return auth_response

        try:
            return self.client.cancel_order(order_id=str(order_id), isVerify=is_verify)
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
            
            order = self.client.place_order(**order_params)
            return order
        except Exception as e:
            logger.error(f"Error placing trade: {e}", exc_info=True)
            return {"error": f"An error occurred while placing trade: {e}"}

    def subscribe(self, instrument_tokens, on_message):
        auth_response = self.authenticate()
        if 'error' in auth_response:
            return auth_response
            
        try:
            self.client.on_message = on_message
            self.client.subscribe(instrument_tokens=instrument_tokens)
            logger.info(f"Subscribed to instruments: {instrument_tokens}")
        except Exception as e:
            logger.error(f"Error subscribing to instruments: {e}", exc_info=True)
            return {"error": f"An error occurred during subscription: {e}"}

    def unsubscribe(self, instrument_tokens=[], isIndex=False, isDepth=False):
        try:
            self.client.un_subscribe(instrument_tokens=instrument_tokens, isIndex=isIndex, isDepth=isDepth)
            logger.info("Unsubscribed from all instruments.")
        except Exception as e:
            logger.error(f"Error unsubscribing: {e}", exc_info=True)

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
