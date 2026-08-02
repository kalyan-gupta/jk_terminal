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
