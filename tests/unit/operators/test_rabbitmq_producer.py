import unittest
from unittest import mock
from typing import Dict, Any

import asyncio
from airflow.models import BaseOperator
from airflow.providers.rabbitmq.hooks.rabbitmq_hook import RabbitMQHook
from airflow.providers.rabbitmq.operators.rabbitmq_producer import RabbitMQProducerOperator


class TestRabbitMQProducerOperator(unittest.TestCase):
    """Tests for RabbitMQProducerOperator"""

    def setUp(self):
        """Set up test fixtures"""
        self.connection_uri = "amqp://guest:guest@localhost:5672/"
        self.message = "test message"
        self.exchange = "test_exchange"
        self.routing_key = "test_routing_key"
        self.task_id = "test_task_id"

    def test_init(self):
        """Test operator initialization"""
        operator = RabbitMQProducerOperator(
            task_id=self.task_id,
            connection_uri=self.connection_uri,
            message=self.message,
            exchange=self.exchange,
            routing_key=self.routing_key,
            use_async=False,
        )

        self.assertEqual(operator.connection_uri, self.connection_uri)
        self.assertEqual(operator.message, self.message)
        self.assertEqual(operator.exchange, self.exchange)
        self.assertEqual(operator.routing_key, self.routing_key)
        self.assertEqual(operator.use_async, False)
        self.assertIsInstance(operator, BaseOperator)

    def test_init_with_async(self):
        """Test operator initialization with async mode"""
        operator = RabbitMQProducerOperator(
            task_id=self.task_id,
            connection_uri=self.connection_uri,
            message=self.message,
            exchange=self.exchange,
            routing_key=self.routing_key,
            use_async=True,
        )

        self.assertEqual(operator.use_async, True)

    @mock.patch.object(RabbitMQHook, "publish_sync")
    @mock.patch.object(RabbitMQHook, "__init__")
    def test_execute_sync(self, mock_hook_init, mock_publish_sync):
        """Test execute method with synchronous mode"""
        # Setup mocks
        mock_hook_init.return_value = None

        # Create operator
        operator = RabbitMQProducerOperator(
            task_id=self.task_id,
            connection_uri=self.connection_uri,
            message=self.message,
            exchange=self.exchange,
            routing_key=self.routing_key,
            use_async=False,
        )

        # Call execute
        context: Dict[str, Any] = {}
        operator.execute(context)

        # Assertions
        mock_hook_init.assert_called_once_with(self.connection_uri)
        mock_publish_sync.assert_called_once_with(self.message, self.exchange, self.routing_key)

    @mock.patch.object(asyncio, "run")
    @mock.patch.object(RabbitMQHook, "__init__")
    def test_execute_async(self, mock_hook_init, mock_asyncio_run):
        """Test execute method with asynchronous mode"""
        # Setup mocks
        mock_hook_init.return_value = None

        # Create operator
        operator = RabbitMQProducerOperator(
            task_id=self.task_id,
            connection_uri=self.connection_uri,
            message=self.message,
            exchange=self.exchange,
            routing_key=self.routing_key,
            use_async=True,
        )

        # Call execute
        context: Dict[str, Any] = {}
        operator.execute(context)

        # Assertions
        mock_hook_init.assert_called_once_with(self.connection_uri)
        mock_asyncio_run.assert_called_once()


if __name__ == "__main__":
    unittest.main()