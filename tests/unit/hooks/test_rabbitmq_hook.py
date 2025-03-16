import unittest
from unittest import mock

import aio_pika
import pika
from pika.adapters.blocking_connection import BlockingConnection

from airflow.providers.rabbitmq.hooks.rabbitmq_hook import RabbitMQHook


class TestRabbitMQHook(unittest.TestCase):
    """Tests for RabbitMQHook"""

    def setUp(self):
        """Set up test fixtures"""
        self.connection_uri = "amqp://guest:guest@localhost:5672/"
        self.hook = RabbitMQHook(connection_uri=self.connection_uri)

    def test_init(self):
        """Test hook initialization"""
        self.assertEqual(self.hook.connection_uri, self.connection_uri)

    @mock.patch("pika.BlockingConnection")
    @mock.patch("pika.URLParameters")
    def test_get_sync_connection(self, mock_url_parameters, mock_blocking_connection):
        """Test get_sync_connection method"""
        # Setup mocks
        mock_url_parameters.return_value = "mocked_params"
        mock_connection = mock.MagicMock(spec=BlockingConnection)
        mock_blocking_connection.return_value = mock_connection

        # Call the method
        result = self.hook.get_sync_connection()

        # Assertions
        mock_url_parameters.assert_called_once_with(self.connection_uri)
        mock_blocking_connection.assert_called_once_with("mocked_params")
        self.assertEqual(result, mock_connection)

    @mock.patch("aio_pika.connect_robust")
    async def test_get_async_connection(self, mock_connect_robust):
        """Test get_async_connection method"""
        # Setup mock
        mock_connection = mock.MagicMock(spec=aio_pika.abc.AbstractRobustConnection)
        mock_connect_robust.return_value = mock_connection

        # Call the method
        result = await self.hook.get_async_connection()

        # Assertions
        mock_connect_robust.assert_called_once_with(self.connection_uri)
        self.assertEqual(result, mock_connection)

    @mock.patch.object(RabbitMQHook, "get_sync_connection")
    def test_publish_sync(self, mock_get_sync_connection):
        """Test publish_sync method"""
        # Setup mocks
        mock_connection = mock.MagicMock(spec=BlockingConnection)
        mock_channel = mock.MagicMock(spec=pika.channel.Channel)
        mock_connection.channel.return_value = mock_channel
        mock_get_sync_connection.return_value = mock_connection

        # Test data
        message = "test message"
        exchange = "test_exchange"
        routing_key = "test_routing_key"

        # Call the method
        self.hook.publish_sync(message, exchange, routing_key)

        # Assertions
        mock_get_sync_connection.assert_called_once()
        mock_connection.channel.assert_called_once()
        mock_channel.basic_publish.assert_called_once_with(
            exchange=exchange, routing_key=routing_key, body=message
        )
        mock_connection.close.assert_called_once()

    @mock.patch.object(RabbitMQHook, "get_async_connection")
    async def test_publish_async(self, mock_get_async_connection):
        """Test publish_async method"""
        # Setup mocks
        mock_connection = mock.MagicMock(spec=aio_pika.abc.AbstractRobustConnection)
        mock_channel = mock.MagicMock(spec=aio_pika.abc.AbstractChannel)
        mock_exchange = mock.MagicMock()
        mock_channel.default_exchange = mock_exchange
        
        # Setup async context
        mock_connection.channel = mock.AsyncMock(return_value=mock_channel)
        mock_connection.close = mock.AsyncMock()
        mock_exchange.publish = mock.AsyncMock()
        mock_get_async_connection.return_value = mock_connection

        # Test data
        message = "test message"
        exchange = "test_exchange"
        routing_key = "test_routing_key"

        # Call the method
        await self.hook.publish_async(message, exchange, routing_key)

        # Assertions
        mock_get_async_connection.assert_called_once()
        mock_connection.channel.assert_called_once()
        mock_exchange.publish.assert_called_once()
        mock_connection.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()