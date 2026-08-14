from django.test import TestCase
from unittest.mock import patch, MagicMock
from trades.kotak_neo_api import KotakNeoAPI

class KotakNeoAPITestCase(TestCase):
    @patch('trades.kotak_neo_api.DirectNeoClient')
    def test_unsubscribe_calls_un_subscribe(self, MockDirectNeoClient):
        # Arrange
        mock_client = MagicMock()
        MockDirectNeoClient.return_value = mock_client
        
        # Instantiate the API wrapper
        api = KotakNeoAPI()
        
        # Act
        api.unsubscribe(instrument_tokens=['some_token'], isIndex=True, isDepth=True)
        
        # Assert
        mock_client.un_subscribe.assert_called_once_with(instrument_tokens=['some_token'], isIndex=True, isDepth=True)


class SMTPSettingsTestCase(TestCase):
    def test_send_html_email_renders_context_and_includes_from_name(self):
        from trades.models import SMTPSettings
        
        smtp = SMTPSettings(
            host="smtp.example.com",
            port=587,
            host_user="test@example.com",
            from_address="noreply@example.com",
            from_name="JK Test Display Name"
        )
        
        email_msg = smtp.send_html_email(
            subject_template="OTP for {{ username }}",
            body_template="Your OTP code is {{ otp }}",
            context_dict={"username": "alice", "otp": "999999"},
            to_emails=["recipient@example.com"]
        )
        
        self.assertEqual(email_msg.subject, "OTP for alice")
        self.assertIn("Your OTP code is 999999", email_msg.alternatives[0][0])
        self.assertEqual(email_msg.from_email, "JK Test Display Name <noreply@example.com>")
        self.assertEqual(email_msg.to, ["recipient@example.com"])


class KotakNeoAPIExtendedTestCase(TestCase):
    def setUp(self):
        from django.contrib.auth.models import User
        from trades.models import PlatformSettings, UserNeoCredentials
        # Clean up any existing records
        User.objects.filter(username='testuser').delete()
        self.user = User.objects.create_user(username='testuser', password='password')
        self.creds = UserNeoCredentials.objects.create(
            user=self.user,
            mpin='1234',
            consumer_key='testkey',
            mobile_number='+919999999999',
            ucc='UCC123',
            account_name='Test Account',
            totp_secret='MZXW6YTBOI======',
            auth_mode='secret'
        )
        self.platform_settings = PlatformSettings.get_settings()
        self.platform_settings.allow_direct_secret_auth = True
        self.platform_settings.save()

    def test_totp_generator_format(self):
        api = KotakNeoAPI(user=self.user)
        code = api.generate_totp_code('MZXW6YTBOI======')
        self.assertEqual(len(code), 6)
        self.assertTrue(code.isdigit())

    @patch('trades.kotak_neo_api.DirectNeoClient')
    def test_authenticate_with_secret(self, MockDirectNeoClient):
        mock_client = MagicMock()
        MockDirectNeoClient.return_value = mock_client
        mock_client.totp_login.return_value = {"status": "success", "data": {"token": "t", "sid": "s"}}
        mock_client.totp_validate.return_value = {"status": "success", "data": {"token": "t2", "sid": "s2"}}
        
        api = KotakNeoAPI(user=self.user)
        # Call authenticate without passing totp explicitly, should use totp_secret
        result = api.authenticate(force_refresh=True)
        
        self.assertEqual(result.get('status'), 'success')
        self.assertTrue(api.is_authenticated)
        # Verify totp_login was called with generated 6-digit TOTP
        self.assertTrue(mock_client.totp_login.called)
        args, kwargs = mock_client.totp_login.call_args
        self.assertEqual(len(kwargs.get('totp')), 6)


class TrackedOrderTestCase(TestCase):
    def setUp(self):
        from django.contrib.auth.models import User
        from trades.models import PlatformSettings, UserNeoCredentials
        from trades.kotak_neo_api import KotakNeoAPI
        KotakNeoAPI._session_cache.clear()
        
        self.user = User.objects.create_user(username='testuser_tracked', email='recipient@example.com', password='password')
        self.creds = UserNeoCredentials.objects.create(
            user=self.user,
            mpin='1234',
            consumer_key='testkey',
            mobile_number='+919999999999',
            ucc='UCC123',
            account_name='Test Account',
            totp_secret='MZXW6YTBOI======',
            auth_mode='secret'
        )
        self.platform_settings = PlatformSettings.get_settings()
        self.platform_settings.allow_direct_secret_auth = True
        self.platform_settings.save()

    @patch('trades.kotak_neo_api.DirectNeoClient')
    def test_place_trade_creates_tracked_order_and_sends_email(self, MockDirectNeoClient):
        from trades.models import TrackedOrder
        mock_client = MagicMock()
        MockDirectNeoClient.return_value = mock_client
        mock_client.totp_login.return_value = {"status": "success", "data": {"token": "t", "sid": "s"}}
        mock_client.totp_validate.return_value = {"status": "success", "data": {"token": "t2", "sid": "s2"}}
        mock_client.place_order.return_value = {"status": "success", "nOrdNo": "260802999999"}

        api = KotakNeoAPI(user=self.user)
        res = api.place_trade(
            trading_symbol='RELIANCE',
            quantity=10,
            price=2400.0,
            transaction_type='BUY',
            exchange_segment='nse_cm',
            product='MIS',
            order_type='L'
        )

        self.assertEqual(res.get('nOrdNo'), '260802999999')
        # Check if TrackedOrder is created
        tracked = TrackedOrder.objects.get(order_id='260802999999')
        self.assertEqual(tracked.trading_symbol, 'RELIANCE')
        self.assertEqual(tracked.last_status, 'placed')
        self.assertFalse(tracked.is_terminal)

    @patch('trades.kotak_neo_api.DirectNeoClient')
    def test_modify_order_updates_tracked_order(self, MockDirectNeoClient):
        import json
        from django.utils import timezone
        from trades.models import TrackedOrder, SessionActivity
        from django.test import RequestFactory
        from django.contrib.sessions.middleware import SessionMiddleware
        from trades.views.orders import modify_order_ajax

        mock_client = MagicMock()
        MockDirectNeoClient.return_value = mock_client
        mock_client.totp_login.return_value = {"status": "success", "data": {"token": "t", "sid": "s"}}
        mock_client.totp_validate.return_value = {"status": "success", "data": {"token": "t2", "sid": "s2"}}
        
        # Setup initial tracked order
        TrackedOrder.objects.create(
            user=self.user,
            order_id='260802999999',
            trading_symbol='RELIANCE',
            transaction_type='BUY',
            quantity=10,
            price=2400.0,
            order_type='L',
            product_type='MIS',
            exchange_segment='nse_cm',
            last_status='placed'
        )

        # Mock modification API response
        mock_client.modify_order.return_value = {"status": "success", "nOrdNo": "260802999999"}

        # Simulate AJAX request to modify_order_ajax
        factory = RequestFactory()
        
        payload = {
            'order_id': '260802999999',
            'quantity': 15,
            'price': 2450.0,
            'order_type': 'L',
            'trading_symbol': 'RELIANCE',
            'transaction_type': 'BUY',
            'exchange_segment': 'nse_cm',
            'product_type': 'MIS'
        }
        
        request = factory.post('/modify_order_ajax/', data=json.dumps(payload), content_type='application/json')
        request.user = self.user
        
        # Setup session for decorator
        middleware = SessionMiddleware(lambda req: None)
        middleware.process_request(request)
        request.session.save()
        
        # Setup SessionActivity to bypass decorator check
        SessionActivity.objects.create(
            user=self.user, 
            session_key=request.session.session_key, 
            sdk_session_active=True, 
            sdk_session_expires_at=timezone.now() + timezone.timedelta(hours=1)
        )

        response = modify_order_ajax(request)
        
        # Verify response is 200 OK and status success
        self.assertEqual(response.status_code, 200)
        res_data = json.loads(response.content)
        self.assertEqual(res_data.get('status'), 'success')

        # Check that TrackedOrder was updated
        tracked = TrackedOrder.objects.get(order_id='260802999999')
        self.assertEqual(tracked.quantity, 15)
        self.assertEqual(tracked.price, 2450.0)


class RESTAPISecurityTestCase(TestCase):
    def setUp(self):
        from django.contrib.auth.models import User
        from trades.models import UserSecurity, PlatformSettings
        from rest_framework.test import APIRequestFactory
        
        User.objects.filter(username='api_test_user').delete()
        self.user = User.objects.create_user(username='api_test_user', password='password123')
        self.security, _ = UserSecurity.objects.get_or_create(user=self.user)
        self.security.force_password_change = False
        self.security.save()
        
        # Clean up and ensure PlatformSettings is configured
        PlatformSettings.objects.all().delete()
        self.platform_settings = PlatformSettings.objects.create(
            session_timeout_enabled=True,
            session_timeout_seconds=300
        )
        
        from trades.api.permissions import IsSessionValidAndPasswordChangeNotRequired
        self.permission = IsSessionValidAndPasswordChangeNotRequired()
        self.factory = APIRequestFactory()

    def test_permission_granted_for_valid_active_session(self):
        from django.utils import timezone
        from trades.models import SessionActivity
        
        # Clean up any existing activity for test user
        SessionActivity.objects.filter(user=self.user).delete()
        
        # Setup SessionActivity that is active (not expired)
        SessionActivity.objects.create(
            user=self.user,
            session_key=f"api_{self.user.username}",
            last_activity=timezone.now()
        )
        
        request = self.factory.get('/api/profile/')
        request.user = self.user
        
        # Permission should be granted
        self.assertTrue(self.permission.has_permission(request, None))

    def test_permission_denied_when_force_password_change_true(self):
        from rest_framework.exceptions import PermissionDenied
        
        self.security.force_password_change = True
        self.security.save()
        
        request = self.factory.get('/api/profile/')
        request.user = self.user
        
        with self.assertRaises(PermissionDenied) as ctx:
            self.permission.has_permission(request, None)
            
        self.assertEqual(ctx.exception.detail.get('code'), 'password_change_required')

    def test_permission_denied_when_session_expired(self):
        from django.utils import timezone
        from rest_framework.exceptions import AuthenticationFailed
        from trades.models import SessionActivity
        
        # Clean up any existing activity for test user
        SessionActivity.objects.filter(user=self.user).delete()
        
        # Setup SessionActivity that is expired
        activity = SessionActivity.objects.create(
            user=self.user,
            session_key=f"api_{self.user.username}"
        )
        SessionActivity.objects.filter(id=activity.id).update(
            last_activity=timezone.now() - timezone.timedelta(seconds=600)
        )
        
        request = self.factory.get('/api/profile/')
        request.user = self.user
        
        with self.assertRaises(AuthenticationFailed) as ctx:
            self.permission.has_permission(request, None)
            
        self.assertEqual(ctx.exception.detail.get('error'), 'Session expired')

    @patch('trades.models.ActiveMarketData.objects.exists')
    @patch('trades.models.ActiveMarketData.objects.raw')
    def test_market_search_cache_api(self, mock_raw, mock_exists):
        mock_exists.return_value = True
        
        from trades.api.views import MarketSearchAPIView
        from rest_framework import status
        
        # Mock raw SQL query return value
        mock_scrip = MagicMock()
        mock_scrip.symbol = "12345"
        mock_scrip.exch_seg = "nse_cm"
        mock_scrip.symbol_name = "TCS"
        mock_scrip.trd_symbol = "TCS-EQ"
        mock_scrip.option_type = None
        mock_scrip.inst_type = None
        mock_scrip.strike_price = 0
        mock_scrip.scrip_ref_key = "TCS"
        mock_scrip.desc = "TATA CONSULTANCY SERVICES"
        mock_scrip.group = ""
        mock_scrip.asset_code = ""
        mock_scrip.has_option_chain = False
        mock_scrip.tick_size = 5
        mock_scrip.lot_size = 1
        
        mock_raw.return_value = [mock_scrip]
        
        # Perform view test
        from rest_framework.test import force_authenticate
        view = MarketSearchAPIView.as_view()
        request = self.factory.get('/api/v1/market/search/?q=tcs')
        force_authenticate(request, user=self.user)
        
        # Setup active session activity to pass permission check
        from trades.models import SessionActivity
        SessionActivity.objects.update_or_create(
            user=self.user,
            session_key=f"api_{self.user.username}"
        )
        
        response = view(request)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertIn('results', response.data)
        self.assertEqual(len(response.data['results']), 1)
        self.assertEqual(response.data['results'][0]['pSymbolName'], 'TCS')


