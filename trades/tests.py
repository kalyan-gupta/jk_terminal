from django.test import TestCase
from unittest.mock import patch, MagicMock
from trades.kotak_neo_api import KotakNeoAPI

class KotakNeoAPITestCase(TestCase):
    @patch('trades.kotak_neo_api.NeoAPI')
    def test_unsubscribe_calls_un_subscribe(self, MockNeoAPI):
        # Arrange
        mock_client = MagicMock()
        MockNeoAPI.return_value = mock_client
        
        # Instantiate the API wrapper
        api = KotakNeoAPI()
        
        # Act
        api.unsubscribe(instrument_tokens=['some_token'], isIndex=True, isDepth=True)
        
        # Assert
        mock_client.un_subscribe.assert_called_once_with(instrument_tokens=['some_token'], isIndex=True, isDepth=True)
