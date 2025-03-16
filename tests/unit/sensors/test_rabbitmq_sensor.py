import unittest
from unittest import mock
from typing import Dict, Any, Optional

from airflow.sensors.base import BaseSensorOperator
from pika.adapters.blocking_connection import BlockingChannel, BlockingConnection
from pika.frame import Method

from airflow.providers.rabbitmq.hooks.rabbitmq_hook import RabbitMQHook
from airflow.providers.rabbitmq.sensors.rabbitmq_sensor import RabbitMQSensor


class TestRabbitMQSensor(unittest.TestCase):
    """Tests for RabbitMQSensor"""

    def setUp(self):
        """Set up test fixtures"""
        self.connection_uri = "amqp://guest:guest@localhost:5672/"
        self.queue = "test_queue"
        self.task_id = "test_task_id"

    def test_init(self):
        """Test sensor initialization"""
        sensor = RabbitMQSensor(
            task_id=self.task_id,
            connection_uri=self.connection_uri,
            queue=self.queue,
        )

        self.assertEqual(sensor.connection_uri, self.connection_uri)
        self.assertEqual(sensor.queue, self.queue)
        self.assertIsInstance(sensor, BaseSensorOperator)

    @mock.patch.object(RabbitMQHook, "get_sync_connection")
    @mock.patch.object(RabbitMQHook, "__init__")
    def test_poke_with_message(self, mock_hook_init, mock_get_sync_connection):
        """Test poke method when a message is found"""
        # Setup mocks
        mock_hook_init.return_value = None
        mock_connection = mock.MagicMock(spec=BlockingConnection)
        mock_channel = mock.MagicMock(spec=BlockingChannel)
        mock_method_frame = mock.MagicMock(spec=Method)
        
        mock_connection.channel.return_value = mock_channel
        mock_channel.basic_get.return_value = (mock_method_frame, None, b"test message")
        mock_get_sync_connection.return_value = mock_connection

        # Create sensor
        sensor = RabbitMQSensor(
            task_id=self.task_id,
            connection_uri=self.connection_uri,
            queue=self.queue,
        )

        # Call poke
        context: Dict[str, Any] = {}
        result = sensor.poke(context)

        # Assertions
        mock_hook_init.assert_called_once_with(self.connection_uri)
        mock_get_sync_connection.assert_called_once()
        mock_connection.channel.assert_called_once()
        mock_channel.basic_get.assert_called_once_with(self.queue, auto_ack=True)
        mock_connection.close.assert_called_once()
        self.assertTrue(result)

    @mock.patch.object(RabbitMQHook, "get_sync_connection")
    @mock.patch.object(RabbitMQHook, "__init__")
    def test_poke_without_message(self, mock_hook_init, mock_get_sync_connection):
        """Test poke method when no message is found"""
        # Setup mocks
        mock_hook_init.return_value = None
        mock_connection = mock.MagicMock(spec=BlockingConnection)
        mock_channel = mock.MagicMock(spec=BlockingChannel)
        
        mock_connection.channel.return_value = mock_channel
        mock_channel.basic_get.return_value = (None, None, None)
        mock_get_sync_connection.return_value = mock_connection

        # Create sensor
        sensor = RabbitMQSensor(
            task_id=self.task_id,
            connection_uri=self.connection_uri,
            queue=self.queue,
        )

        # Call poke
        context: Dict[str, Any] = {}
        result = sensor.poke(context)

        # Assertions
        mock_hook_init.assert_called_once_with(self.connection_uri)
        mock_get_sync_connection.assert_called_once()
        mock_connection.channel.assert_called_once()
        mock_channel.basic_get.assert_called_once_with(self.queue, auto_ack=True)
        mock_connection.close.assert_called_once()
        self.assertFalse(result)

    @mock.patch.object(RabbitMQHook, "get_sync_connection")
    @mock.patch.object(RabbitMQHook, "__init__")
    def test_poke_with_exception(self, mock_hook_init, mock_get_sync_connection):
        """Test poke method when an exception occurs"""
        # Setup mocks
        mock_hook_init.return_value = None
        mock_get_sync_connection.side_effect = Exception("Test exception")

        # Create sensor
        sensor = RabbitMQSensor(
            task_id=self.task_id,
            connection_uri=self.connection_uri,
            queue=self.queue,
        )

        # Call poke
        context: Dict[str, Any] = {}
        result = sensor.poke(context)

        # Assertions
        mock_hook_init.assert_called_once_with(self.connection_uri)
        mock_get_sync_connection.assert_called_once()
        self.assertFalse(result)


if __name__ == "__main__":
    unittest.main()