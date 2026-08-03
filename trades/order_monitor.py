import time
import logging
import threading
from django.contrib.auth.models import User
from django.utils import timezone

logger = logging.getLogger(__name__)

def _order_monitor_loop():
    logger.info("Order monitor background thread started.")
    
    # Give Django a few seconds to boot up completely
    time.sleep(10)
    
    while True:
        try:
            from trades.models import TrackedOrder, SMTPSettings, PlatformSettings, UserNeoCredentials
            from trades.kotak_neo_api import KotakNeoAPI
            
            # Fetch all active (non-terminal) orders
            active_orders = TrackedOrder.objects.filter(is_terminal=False)
            if active_orders.exists():
                plat_settings = PlatformSettings.get_settings()
                
                # Group active orders by user to minimize API authentication calls
                orders_by_user = {}
                for order in active_orders:
                    orders_by_user.setdefault(order.user_id, []).append(order)
                    
                for user_id, user_orders in orders_by_user.items():
                    try:
                        user = User.objects.get(id=user_id)
                        
                        # Verify user credentials are active and set to secret auth mode
                        try:
                            user_creds = UserNeoCredentials.objects.get(user=user, is_active=True)
                        except UserNeoCredentials.DoesNotExist:
                            logger.debug(f"User {user.username} has no active Neo credentials. Skipping.")
                            continue
                            
                        if user_creds.auth_mode != 'secret' or not plat_settings.allow_direct_secret_auth:
                            logger.debug(f"User {user.username} is not using secret auth or direct auth is disabled. Skipping.")
                            continue
                        
                        # Initialize API handler for the user
                        api = KotakNeoAPI(user=user, session_id="background_monitor")
                        
                        # Authenticate using stored TOTP secret
                        auth_result = api.authenticate(force_refresh=False)
                        if auth_result.get('status') != 'success':
                            logger.warning(f"Background auth failed for user {user.username}: {auth_result.get('error')}")
                            continue
                            
                        # Retrieve order book
                        order_book = api.get_order_book()
                        if isinstance(order_book, dict) and 'error' in order_book:
                            logger.warning(f"Failed to fetch order book for user {user.username}: {order_book['error']}")
                            continue
                            
                        if not isinstance(order_book, list):
                            logger.debug(f"Invalid order book format for user {user.username}")
                            continue
                            
                        # Map order book by order ID for fast lookup
                        order_book_map = {str(item.get('nOrdNo')): item for item in order_book if item.get('nOrdNo')}
                        
                        for tracked_order in user_orders:
                            order_id_str = str(tracked_order.order_id)
                            if order_id_str in order_book_map:
                                order_data = order_book_map[order_id_str]
                                current_status = order_data.get('stat') or order_data.get('ordSt') or 'unknown'
                                
                                # Sync quantity and price if they differ from Kotak's response
                                params_modified = False
                                try:
                                    kotak_qty = int(order_data.get('qty', tracked_order.quantity))
                                    kotak_price = float(order_data.get('prc', tracked_order.price))
                                    if kotak_qty != tracked_order.quantity or abs(kotak_price - tracked_order.price) > 0.001:
                                        logger.info(f"Order {tracked_order.order_id} parameters modified on broker: Qty {tracked_order.quantity} -> {kotak_qty}, Price {tracked_order.price} -> {kotak_price}")
                                        tracked_order.quantity = kotak_qty
                                        tracked_order.price = kotak_price
                                        tracked_order.save()
                                        params_modified = True
                                except Exception as e:
                                    logger.error(f"Error syncing tracked order parameters: {e}")
                                
                                # Check if status changed or parameters modified
                                status_changed = current_status.strip().lower() != tracked_order.last_status.strip().lower()
                                if status_changed or params_modified:
                                    old_status = tracked_order.last_status
                                    tracked_order.last_status = current_status
                                    
                                    # Determine if terminal state reached
                                    terminal_statuses = ['complete', 'rejected', 'cancelled']
                                    if current_status.strip().lower() in terminal_statuses:
                                        tracked_order.is_terminal = True
                                        
                                    tracked_order.save()
                                    
                                    # Determine the status to display in the email update
                                    display_status = current_status
                                    if params_modified and not status_changed:
                                        display_status = "modified"
                                    
                                    logger.info(f"Order {tracked_order.order_id} ({tracked_order.trading_symbol}) changed: {old_status} -> {display_status}")
                                    
                                    # Send status change email
                                    try:
                                        smtp_settings = SMTPSettings.get_settings()
                                        if smtp_settings.enable_order_notifications and smtp_settings.host and smtp_settings.host_user and user.email:
                                            from django.core.mail import get_connection
                                            connection = get_connection(
                                                host=smtp_settings.host,
                                                port=smtp_settings.port,
                                                username=smtp_settings.host_user,
                                                password=smtp_settings.get_decrypted_password(),
                                                use_tls=smtp_settings.use_tls,
                                                timeout=5
                                            )
                                            subj_temp = smtp_settings.order_modified_subject if display_status == "modified" else smtp_settings.order_status_subject
                                            body_temp = smtp_settings.order_modified_template if display_status == "modified" else smtp_settings.order_status_template
                                            email_msg = smtp_settings.send_html_email(
                                                subject_template=subj_temp,
                                                body_template=body_temp,
                                                context_dict={
                                                    'username': user.username,
                                                    'trading_symbol': tracked_order.trading_symbol,
                                                    'transaction_type': 'BUY' if str(tracked_order.transaction_type).upper().startswith('B') else 'SELL',
                                                    'quantity': tracked_order.quantity,
                                                    'price': tracked_order.price,
                                                    'order_id': tracked_order.order_id,
                                                    'last_status': display_status,
                                                },
                                                to_emails=[user.email],
                                                connection=connection
                                            )
                                            email_msg.send(fail_silently=False)
                                            logger.info(f"Status update email sent successfully to {user.email} for order {tracked_order.order_id} (Status: {display_status})")
                                    except Exception as mail_ex:
                                        logger.error(f"Failed to send order status email for {tracked_order.order_id}: {mail_ex}")
                                        
                    except Exception as user_ex:
                        logger.error(f"Error processing background orders for user ID {user_id}: {user_ex}", exc_info=True)
                        
        except Exception as loop_ex:
            logger.error(f"Error in background order monitor loop: {loop_ex}", exc_info=True)
            
        # Poll every 10 seconds
        time.sleep(10)

def start_order_monitor():
    """Start the order status monitoring loop in a background daemon thread."""
    thread = threading.Thread(target=_order_monitor_loop, name="OrderMonitorDaemon", daemon=True)
    thread.start()
