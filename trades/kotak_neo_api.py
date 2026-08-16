from django.conf import settings
from django.utils import timezone
from datetime import timedelta
import logging
import requests
import os
import urllib.parse
import pandas as pd
import io
import json

logger = logging.getLogger(__name__)

class DirectNeoClient:
    """
    Direct lightweight implementation of Kotak Neo API REST and WebSocket client
    to avoid high-level dependency on the neo_api_client SDK.
    """
    def __init__(self, environment='prod', consumer_key=None):
        self.environment = environment
        self.consumer_key = consumer_key  # Access token from dashboard
        self.view_token = None
        self.sid = None
        self.edit_token = None
        self.edit_sid = None
        self.edit_rid = None
        self.serverId = None
        self.data_center = None
        self.baseUrl = "https://mis.kotaksecurities.com"
        self.session = requests.Session()
        
        # Callbacks for WebSocket
        self.on_message = None
        self.on_error = None
        self.on_close = None
        self.on_open = None
        self.NeoWebSocket = None

    def totp_login(self, mobile_number, ucc, totp):
        headers = {
            'Authorization': self.consumer_key,
            'neo-fin-key': 'neotradeapi',
            'Content-Type': 'application/json'
        }
        url = "https://mis.kotaksecurities.com/login/1.0/tradeApiLogin"
        payload = {
            "mobileNumber": mobile_number,
            "ucc": ucc,
            "totp": totp
        }
        response = self.session.post(url, headers=headers, json=payload, timeout=30)
        try:
            data = response.json()
        except Exception:
            return {"error": "Invalid JSON response from server"}
        if response.status_code == 200 and data.get("status") != "error":
            self.view_token = data.get("data", {}).get("token")
            self.sid = data.get("data", {}).get("sid")
        return data

    def totp_validate(self, mpin):
        headers = {
            'Authorization': self.consumer_key,
            'sid': self.sid,
            'Auth': self.view_token,
            'neo-fin-key': 'neotradeapi',
            'Content-Type': 'application/json'
        }
        url = "https://mis.kotaksecurities.com/login/1.0/tradeApiValidate"
        payload = {"mpin": mpin}
        response = self.session.post(url, headers=headers, json=payload, timeout=30)
        try:
            data = response.json()
        except Exception:
            return {"error": "Invalid JSON response from server"}
        if response.status_code == 200 and data.get("status") != "error":
            self.edit_token = data.get("data", {}).get("token")
            self.edit_sid = data.get("data", {}).get("sid")
            self.edit_rid = data.get("data", {}).get("rid")
            self.serverId = data.get("data", {}).get("hsServerId")
            self.data_center = data.get("data", {}).get("dataCenter")
            self.baseUrl = data.get("data", {}).get("baseUrl")
        return data

    def request(self, method, path, query_params=None, body_params=None, urlencoded=False, timeout=30):
        url = f"{self.baseUrl.rstrip('/')}/{path.lstrip('/')}"
        if query_params:
            url = f"{url}?{urllib.parse.urlencode(query_params)}"
        headers = {
            "Sid": self.edit_sid,
            "Auth": self.edit_token,
            "neo-fin-key": "neotradeapi"
        }
        if urlencoded:
            headers["Content-Type"] = "application/x-www-form-urlencoded"
            payload = {}
            if body_params:
                payload["jData"] = json.dumps(body_params)
            response = self.session.post(url, headers=headers, data=payload, timeout=timeout)
        else:
            headers["Content-Type"] = "application/json"
            response = self.session.request(method, url, headers=headers, json=body_params, timeout=timeout)
        
        try:
            return response.json()
        except Exception:
            return {"error": "Invalid JSON response from server", "status_code": response.status_code}

    def positions(self):
        return self.request("GET", "quick/user/positions")

    def holdings(self):
        return self.request("GET", "portfolio/v1/holdings")

    def limits(self, segment="ALL", exchange="ALL", product="ALL"):
        body = {
            "seg": segment,
            "exch": exchange,
            "prod": product
        }
        return self.request("POST", "quick/user/limits", body_params=body, urlencoded=True)

    def order_report(self):
        return self.request("GET", "quick/user/orders")

    def order_history(self, order_id):
        body = {"nOrdNo": str(order_id)}
        return self.request("POST", "quick/order/history", body_params=body, urlencoded=True)

    def cancel_order(self, order_id, isVerify=False, amo="NO"):
        body = {"on": str(order_id), "am": amo}
        return self.request("POST", "quick/order/cancel", body_params=body, urlencoded=True)

    def place_order(self, **kwargs):
        body = {
            "am": kwargs.get("amo", "NO"),
            "dq": kwargs.get("disclosed_quantity", "0"),
            "es": kwargs.get("exchange_segment"),
            "mp": kwargs.get("market_protection", "0"),
            "pc": kwargs.get("product"),
            "pf": kwargs.get("pf", "N"),
            "pr": kwargs.get("price", "0"),
            "pt": kwargs.get("order_type"),
            "qt": kwargs.get("quantity"),
            "rt": kwargs.get("validity", "DAY"),
            "tp": kwargs.get("trigger_price", "0"),
            "ts": kwargs.get("trading_symbol"),
            "tt": kwargs.get("transaction_type"),
            "sot": kwargs.get("square_off_type", ""),
            "slt": kwargs.get("stop_loss_type", ""),
            "slv": kwargs.get("stop_loss_value", ""),
            "sov": kwargs.get("square_off_value", ""),
            "tlt": kwargs.get("trailing_stop_loss", ""),
            "tsv": kwargs.get("trailing_sl_value", ""),
            "os": "NEOTRADEAPI"
        }
        query_params = {"sId": self.serverId} if self.serverId else None
        return self.request("POST", "quick/order/rule/ms/place", query_params=query_params, body_params=body, urlencoded=True)

    def modify_order(self, **kwargs):
        body = {
            "am": kwargs.get("amo", "NO"),
            "dq": kwargs.get("disclosed_quantity", "0"),
            "es": kwargs.get("exchange_segment"),
            "mp": kwargs.get("market_protection", "0"),
            "pc": kwargs.get("product"),
            "pf": kwargs.get("pf", "N"),
            "pr": kwargs.get("price", "0"),
            "pt": kwargs.get("order_type"),
            "qt": kwargs.get("quantity"),
            "vd": kwargs.get("validity", "DAY"),
            "dd": kwargs.get("dd", "NA"),
            "tp": kwargs.get("trigger_price", "0"),
            "ts": kwargs.get("trading_symbol"),
            "tt": kwargs.get("transaction_type"),
            "no": kwargs.get("order_id")
        }
        return self.request("POST", "quick/order/vr/modify", body_params=body, urlencoded=True)

    def margin_required(self, **kwargs):
        body = {
            "brkName": kwargs.get("broker_name", "KOTAK"),
            "brnchId": kwargs.get("branch_id", "ONLINE"),
            "exSeg": kwargs.get("exchange_segment"),
            "prc": str(kwargs.get("price", 0)),
            "prcTp": kwargs.get("order_type"),
            "prod": kwargs.get("product"),
            "qty": str(kwargs.get("quantity")),
            "tok": kwargs.get("instrument_token"),
            "trnsTp": kwargs.get("transaction_type"),
            "slAbsOrTks": kwargs.get("stop_loss_type", ""),
            "slVal": kwargs.get("stop_loss_value", ""),
            "sqrOffAbsOrTks": kwargs.get("square_off_type", ""),
            "sqrOffVal": kwargs.get("square_off_value", ""),
            "trailSL": kwargs.get("trailing_stop_loss", ""),
            "tSLTks": kwargs.get("trailing_sl_value", "")
        }
        return self.request("POST", "quick/user/check-margin", body_params=body, urlencoded=True)

    def quotes(self, instrument_tokens, quote_type="all"):
        if not quote_type:
            quote_type = 'all'
        neo_symbol_str = ",".join(f"{item['exchange_segment']}|{item['instrument_token']}" for item in instrument_tokens)
        encoded_neo_symbol_str = urllib.parse.quote(neo_symbol_str)
        headers = {
            "Authorization": self.consumer_key,
            "Content-Type": "application/json"
        }
        url = f"{self.baseUrl.rstrip('/')}/script-details/1.0/quotes/neosymbol/{encoded_neo_symbol_str}/{quote_type}"
        response = self.session.get(url, headers=headers, timeout=30)
        try:
            return response.json()
        except Exception:
            return {"error": "Invalid JSON response from server"}

    def scrip_master(self, exchange_segment=None):
        headers = {
            "Authorization": self.consumer_key,
            "Content-Type": "application/json"
        }
        url = f"{self.baseUrl.rstrip('/')}/script-details/1.0/masterscrip/file-paths"
        response = self.session.get(url, headers=headers, timeout=30)
        try:
            data = response.json()
        except Exception:
            return {"error": "Invalid JSON response from server"}
        
        if response.status_code != 200:
            return data
        
        scrip_report = data.get("data", {})
        if exchange_segment:
            exchange_segment_mapped = exchange_segment
            exchange_segment_csv = [file for file in scrip_report.get("filesPaths", []) if exchange_segment_mapped.lower() in file.lower()]
            if exchange_segment_csv:
                return exchange_segment_csv[0]
            else:
                return {"Error": "Exchange segment not found"}
        return scrip_report

    def search_scrip(self, exchange_segment, symbol="", expiry=None, option_type=None, strike_price=None, ignore_50multiple=True):
        scrip_report = self.scrip_master(exchange_segment)
        if isinstance(scrip_report, dict) and "error" in scrip_report:
            return scrip_report
        
        url = scrip_report
        if not isinstance(url, str):
            return {"message": "Exchange Segment is not available"}
        
        response = requests.get(url, timeout=30)
        if response.status_code != 200:
            return {"message": "Failed to retrieve scrip master CSV"}
        
        df = pd.read_csv(io.StringIO(response.text))
        df = df.rename(columns=lambda x: x.strip())
        
        if symbol != '':
            mask = df["pSymbolName"].str.lower().str.strip().str.contains(symbol.lower())
            df = df[mask]
            
        if option_type:
            option_type = str(option_type).lower()
            df["pOptionType"] = df["pOptionType"].str.lower()
            mask = df["pOptionType"] == option_type
            df = df[mask]

        if expiry:
            df['pExpiryDate'] = pd.to_datetime(df['pExpiryDate'], unit='s')
            df['pExpiryDate'] = df['pExpiryDate'].dt.strftime('%d%b%Y')
            df = df[df['pExpiryDate'] == expiry]

        if strike_price:
            df['dStrikePrice;'] = df['dStrikePrice;'].astype(float)
            df = df[df['dStrikePrice;'] == float(strike_price) * 100]

        df = df.dropna(how='all')
        if len(df) > 0:
            df = df.to_json(orient='records')
            return json.loads(df)
        return {"message": "No data found with the given search information."}

    def subscribe(self, instrument_tokens, isIndex=False, isDepth=False):
        if not self.NeoWebSocket:
            from .kotak_websocket import NeoWebSocket
            self.NeoWebSocket = NeoWebSocket(
                sid=self.edit_sid,
                token=self.edit_token,
                server_id=self.serverId,
                data_center=self.data_center
            )
            self.NeoWebSocket.on_message = self.on_message
            self.NeoWebSocket.on_error = self.on_error
            self.NeoWebSocket.on_open = self.on_open
            self.NeoWebSocket.on_close = self.on_close
            
        self.NeoWebSocket.get_live_feed(instrument_tokens=instrument_tokens, isIndex=isIndex, isDepth=isDepth)
        if isDepth:
            self.NeoWebSocket.get_live_feed(instrument_tokens=instrument_tokens, isIndex=isIndex, isDepth=False)

    def un_subscribe(self, instrument_tokens, isIndex=False, isDepth=False):
        if self.NeoWebSocket:
            self.NeoWebSocket.un_subscribe_list(instrument_tokens=instrument_tokens, isIndex=isIndex, isDepth=isDepth)
            if isDepth:
                self.NeoWebSocket.un_subscribe_list(instrument_tokens=instrument_tokens, isIndex=isIndex, isDepth=False)


class KotakNeoAPI:
    """
    Kotak Neo API handler - now supports per-user credentials.
    Each user has their own instance with their credentials.
    """

    _session_cache = {}
    
    def __init__(self, user=None, session_id=None, credentials=None):
        """
        Initialize the API handler with user credentials and session context.
        """
        self.user = user
        self.user_id = user.id if user else None
        self.session_id = session_id or "global"
        self.cache_key = (self.user_id, self.session_id)
        self.is_authenticated = False
        self.login_data = None
        self.client = None
        
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
            self.credentials = settings.KOTAK_NEO_API_CREDENTIALS
            self.user_credentials_obj = None
        
        if self.credentials and 'CONSUMER_KEY' in self.credentials:
            self.client = DirectNeoClient(environment='prod', consumer_key=self.credentials['CONSUMER_KEY'])
    
    def get_cached_session(self):
        """Return cached authenticated session data if still valid."""
        if not self.user_id:
            return None
        
        from trades.models import PlatformSettings, SessionActivity
        import pickle
        
        plat_settings = PlatformSettings.get_settings()
        session_info = KotakNeoAPI._session_cache.get(self.cache_key)
        
        if not session_info and plat_settings.allow_session_restore:
            try:
                session_activity = SessionActivity.objects.get(session_key=self.session_id)
                if session_activity.sdk_session_data:
                    decrypted_data = session_activity.decrypt_data(session_activity.sdk_session_data)
                    session_info = pickle.loads(decrypted_data)
                    session_info['restored'] = True
                    KotakNeoAPI._session_cache[self.cache_key] = session_info
            except Exception as e:
                logger.warning(f"Failed to restore SDK session from database: {e}")

        if not session_info:
            return None
            
        if not plat_settings.sdk_timeout_enabled:
            return session_info
            
        if session_info.get('expires_at') and timezone.now() < session_info['expires_at']:
            return session_info
        self.clear_cached_session()
        return None

    def cache_session(self, login_data, duration_seconds=None):
        """Cache the authenticated client for the session duration."""
        if not self.user_id:
            return
            
        if duration_seconds is None:
            from trades.models import PlatformSettings
            plat_settings = PlatformSettings.get_settings()
            duration_seconds = plat_settings.sdk_timeout_seconds if plat_settings.sdk_timeout_enabled else 86400
            
        expires_at = timezone.now() + timedelta(seconds=duration_seconds)
        KotakNeoAPI._session_cache[self.cache_key] = {
            'client': self.client,
            'login_data': login_data,
            'expires_at': expires_at,
        }
        
        try:
            from trades.models import SessionActivity
            import pickle
            
            session_activity = SessionActivity.objects.get(session_key=self.session_id)
            data_to_pickle = {
                'client': self.client,
                'login_data': login_data,
                'expires_at': expires_at
            }
            pickled_data = pickle.dumps(data_to_pickle)
            encrypted_data = session_activity.encrypt_data(pickled_data)
            session_activity.sdk_session_data = encrypted_data
            session_activity.save(update_fields=['sdk_session_data'])
        except Exception as e:
            logger.warning(f"Failed to securely store SDK session to database: {e}")

    def clear_cached_session(self):
        """Remove any cached SDK session for this user session."""
        if not self.user_id:
            return
        KotakNeoAPI._session_cache.pop(self.cache_key, None)
        
        try:
            from trades.models import SessionActivity
            session_activity = SessionActivity.objects.get(session_key=self.session_id)
            session_activity.sdk_session_data = None
            session_activity.save(update_fields=['sdk_session_data'])
        except Exception:
            pass

    def extend_session(self):
        """Extend the current SDK session expiry and save it to the database."""
        session_info = self.get_cached_session()
        if not session_info:
            return False, "No active SDK session to extend."
            
        self.client = session_info['client']
        
        from trades.models import PlatformSettings
        plat_settings = PlatformSettings.get_settings()
        duration_seconds = plat_settings.sdk_timeout_seconds if plat_settings.sdk_timeout_enabled else 86400
        
        self.cache_session(session_info['login_data'], duration_seconds)
        
        try:
            from trades.models import SessionActivity
            session_activity = SessionActivity.objects.get(session_key=self.session_id)
            session_activity.sdk_session_expires_at = timezone.now() + timezone.timedelta(seconds=duration_seconds)
            session_activity.save(update_fields=['sdk_session_expires_at'])
        except Exception:
            pass
            
        return True, (timezone.now() + timezone.timedelta(seconds=duration_seconds)).isoformat()

    def generate_totp_code(self, secret):
        """Generate a 6-digit TOTP code from a base32 encoded secret key (zero-dependency)."""
        import time
        import hmac
        import hashlib
        import base64
        import struct
        
        try:
            # Clean and normalize secret
            secret = secret.replace(" ", "").strip()
            missing_padding = len(secret) % 8
            if missing_padding:
                secret += '=' * (8 - missing_padding)
            key = base64.b32decode(secret, casefold=True)
            counter = int(time.time() // 30)
            msg = struct.pack(">Q", counter)
            hs = hmac.new(key, msg, hashlib.sha1).digest()
            offset = hs[-1] & 0x0f
            bin_code = struct.unpack(">I", hs[offset:offset+4])[0] & 0x7fffffff
            otp = bin_code % 1000000
            return f"{otp:06d}"
        except Exception as e:
            logger.error(f"Error generating TOTP code from secret: {e}")
            return None

    def authenticate(self, totp=None, force_refresh=False):
        """Authenticate with Kotak Neo API using a one-time TOTP code."""
        if self.user and not force_refresh:
            cached = self.get_cached_session()
            if cached:
                self.client = cached['client']
                self.login_data = cached['login_data']
                self.is_authenticated = True
                return {
                    "status": "success", 
                    "message": "Already authenticated",
                    "restored": cached.get('restored', False)
                }

        if not self.client:
            return {"error": "API client not initialized. Please configure your credentials."}

        if not totp:
            from trades.models import PlatformSettings
            plat_settings = PlatformSettings.get_settings()
            if self.user_credentials_obj and self.user_credentials_obj.auth_mode == 'secret' and plat_settings.allow_direct_secret_auth:
                secret = self.credentials.get('TOTP_SECRET')
                if secret:
                    totp = self.generate_totp_code(secret)

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
            from trades.models import PlatformSettings
            plat_settings = PlatformSettings.get_settings()
            sdk_duration = plat_settings.sdk_timeout_seconds if plat_settings.sdk_timeout_enabled else 86400

            self.cache_session(login_data=validate_response, duration_seconds=sdk_duration)

            if self.user_credentials_obj:
                self.user_credentials_obj.last_used = timezone.now()
                self.user_credentials_obj.save()
            
            from trades.models import SessionActivity
            try:
                session_activity = SessionActivity.objects.get(session_key=self.session_id)
                session_activity.mark_sdk_session_active(duration_seconds=sdk_duration)
            except SessionActivity.DoesNotExist:
                logger.warning(f"SessionActivity not found for session_id {self.session_id}. SDK state not saved.")

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

        try:
            positions = self.client.positions()
            if isinstance(positions, dict) and ('error' in positions or 'Error Message' in positions):
                return {"error": f"Could not fetch positions: {positions}"}

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
            if isinstance(holdings_response, dict):
                if 'error' in holdings_response or 'Error Message' in holdings_response:
                    return {"error": f"Could not fetch holdings: {holdings_response}"}
                return holdings_response.get('data', [])
            elif isinstance(holdings_response, list):
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
                return positions.get('data', [])
            elif isinstance(positions, list):
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

    def modify_order(self, order_id, quantity, price, trading_symbol, transaction_type, exchange_segment='nse_cm', product='MIS', order_type='L', validity='DAY', amo='NO'):
        auth_response = self.authenticate()
        if 'error' in auth_response:
            return auth_response

        try:
            order_params = {
                'order_id': str(order_id),
                'exchange_segment': exchange_segment,
                'product': product,
                'order_type': order_type,
                'quantity': str(quantity),
                'validity': validity,
                'trading_symbol': trading_symbol,
                'transaction_type': transaction_type[0].upper(),
                'amo': amo
            }
            order_params['price'] = str(price) if price is not None else '0'
            
            logger.info(f"Attempting to modify order {order_id}: {order_params}")
            result = self.client.modify_order(**order_params)
            logger.info(f"Modify order successful: {result}")
            return result
        except Exception as e:
            logger.error(f"Error modifying order: {e}", exc_info=True)
            return {"error": f"An error occurred while modifying order: {e}"}

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
                'transaction_type': transaction_type[0].upper(),
                'amo': amo
            }
            order_params['price'] = str(price) if price is not None else '0'
            
            logger.info(f"Attempting to place trade: {order_params}")
            order = self.client.place_order(**order_params)
            logger.info(f"Place trade API returned: {order}")

            # Track successfully placed orders
            if isinstance(order, dict) and order.get('status') != 'error' and 'nOrdNo' in order:
                order_id = order['nOrdNo']
                try:
                    from trades.models import TrackedOrder, SMTPSettings
                    TrackedOrder.objects.create(
                        user=self.user,
                        order_id=order_id,
                        trading_symbol=trading_symbol,
                        transaction_type=transaction_type[0].upper(),
                        quantity=quantity,
                        price=price if price is not None else 0.0,
                        order_type=order_type,
                        product_type=product,
                        exchange_segment=exchange_segment,
                        last_status='placed'
                    )

                    # Send initial email notification
                    smtp_settings = SMTPSettings.get_settings()
                    if smtp_settings.enable_order_notifications and smtp_settings.host and smtp_settings.host_user and self.user.email:
                        from django.core.mail import get_connection
                        connection = get_connection(
                            host=smtp_settings.host,
                            port=smtp_settings.port,
                            username=smtp_settings.host_user,
                            password=smtp_settings.get_decrypted_password(),
                            use_tls=smtp_settings.use_tls,
                            timeout=5
                        )
                        email_msg = smtp_settings.send_html_email(
                            subject_template=smtp_settings.order_placed_subject,
                            body_template=smtp_settings.order_placed_template,
                            context_dict={
                                'username': self.user.username,
                                'trading_symbol': trading_symbol,
                                'transaction_type': 'BUY' if transaction_type[0].upper() == 'B' else 'SELL',
                                'quantity': quantity,
                                'price': price if price is not None else 0.0,
                                'order_type': order_type,
                                'product_type': product,
                                'exchange_segment': exchange_segment,
                                'order_id': order_id,
                            },
                            to_emails=[self.user.email],
                            connection=connection
                        )

                        import threading
                        def send_email_async(msg):
                            try:
                                msg.send(fail_silently=False)
                                logger.info(f"Trade notification email sent successfully to {self.user.email} for order {order_id}")
                            except Exception as ex:
                                logger.error(f"Failed to send trade email async: {ex}")

                        threading.Thread(target=send_email_async, args=(email_msg,)).start()
                except Exception as ex:
                    logger.error(f"Failed to track placed order: {ex}", exc_info=True)

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
            self.client.edit_token = None
            self.client.edit_sid = None
        except Exception as e:
            logger.warning(f"SDK logout call failed: {e}", exc_info=True)

        from trades.models import SessionActivity
        try:
            session_activity = SessionActivity.objects.get(session_key=self.session_id)
            session_activity.deactivate_sdk_session()
        except SessionActivity.DoesNotExist:
            pass

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

        if isinstance(scrip_master_data, str):
            file_url = scrip_master_data
            files_paths = [file_url]
        else:
            if 'filesPaths' not in scrip_master_data:
                return {"error": "No file paths found in scrip master data."}
            files_paths = scrip_master_data['filesPaths']

        base_dir = os.path.join(settings.BASE_DIR, 'trades', 'scrip_data')
        os.makedirs(base_dir, exist_ok=True)

        downloaded_files = []
        for file_url in files_paths:
            try:
                response = requests.get(file_url, stream=True, timeout=30)
                response.raise_for_status()

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
