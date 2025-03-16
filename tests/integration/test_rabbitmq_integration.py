import unittest
from unittest import mock
import time
from typing import Dict, Any

from airflow.providers.rabbitmq.hooks.rabbitmq_hook import RabbitMQHook
from airflow.providers.rabbitmq.operators.rabbitmq_producer import RabbitMQProducerOperator
from airflow.providers.rabbitmq.sensors.rabbitmq_sensor import RabbitMQSensor


class TestRabbitMQIntegration(unittest.TestCase):
    """Integration tests for RabbitMQ provider components"""

    def setUp(self):
        """Set up test fixtures"""
        self.connection_uri = "amqp://guest:guest@localhost:5672/"
        self.exchange = ""  # Default exchange
        self.queue = "test_integration_queue"
        self.routing_key = self.queue  # For default exchange, routing key is queue name
        self.message = "test integration message"
        self.task_id = "test_task_id"

    @mock.patch.object(RabbitMQHook, "publish_sync")
    @mock.patch.object(RabbitMQHook, "get_sync_connection")
    @mock.patch.object(RabbitMQHook, "__init__")
    def test_operator_sensor_integration(self, mock_hook_init, mock_get_sync_connection, mock_publish_sync):
        """Test integration between operator and sensor"""
        # Setup mocks
        mock_hook_init.return_value = None
        
        # Create and execute the operator
        operator = RabbitMQProducerOperator(
            task_id=self.task_id,
            connection_uri=self.connection_uri,
            message=self.message,
            exchange=self.exchange,
            routing_key=self.routing_key,
            use_async=False,
        )
        
        context: Dict[str, Any] = {}
        operator.execute(context)
        
        # Verify operator published the message
        mock_hook_init.assert_called_with(self.connection_uri)
        mock_publish_sync.assert_called_once_with(self.message, self.exchange, self.routing_key)
        
        # Reset mocks for sensor test
        mock_hook_init.reset_mock()
        mock_get_sync_connection.reset_mock()
        
        # Setup mock for sensor to find the message
        mock_connection = mock.MagicMock()
        mock_channel = mock.MagicMock()
        mock_method_frame = mock.MagicMock()
        
        mock_connection.channel.return_value = mock_channel
        mock_channel.basic_get.return_value = (mock_method_frame, None, self.message.encode())
        mock_get_sync_connection.return_value = mock_connection
        
        # Create and execute the sensor
        sensor = RabbitMQSensor(
            task_id=self.task_id,
            connection_uri=self.connection_uri,
            queue=self.queue,
        )
        
        result = sensor.poke(context)
        
        # Verify sensor found the message
        mock_hook_init.assert_called_with(self.connection_uri)
        mock_get_sync_connection.assert_called_once()
        mock_channel.basic_get.assert_called_once_with(self.queue, auto_ack=True)
        self.assertTrue(result)


if __name__ == "__main__":
    unittest.main()