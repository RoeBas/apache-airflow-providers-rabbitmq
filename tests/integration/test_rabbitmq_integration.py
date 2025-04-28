import unittest
from typing import Any, Dict

import pika
import pytest
from testcontainers.rabbitmq import RabbitMqContainer

from airflow.providers.rabbitmq.operators.rabbitmq_producer import (
    RabbitMQProducerOperator,
)
from airflow.providers.rabbitmq.sensors.rabbitmq_sensor import RabbitMQSensor


class TestRabbitMQIntegration(unittest.TestCase):
    """Integration tests for RabbitMQ provider components"""

    @pytest.fixture(scope="class", autouse=True)
    def rabbitmq_container(self):
        """Start a RabbitMQ container for the test class"""
        with RabbitMqContainer("rabbitmq:4") as container:
            # Allow RabbitMQ to initialize
            params = container.get_connection_params()
            self.connection_uri = f"amqp://{params.DEFAULT_USERNAME}:{params.DEFAULT_PASSWORD}@{params._host}:{params._port}"
            self.exchange = ""
            self.queue = params.DEFAULT_PASSWORD
            self.routing_key = self.queue
            self.message = "test integration message"
            self.task_id = "test_task_id"
            # Manually configure queue using pika
            params = pika.URLParameters(self.connection_uri)
            connection = pika.BlockingConnection(params)
            channel = connection.channel()
            channel.queue_declare(queue=self.queue, durable=False)
            connection.close()

            yield  # continue with tests

    def test_operator_sensor_integration(self):
        """Test integration between RabbitMQProducerOperator and RabbitMQSensor"""

        # Run the RabbitMQProducerOperator
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

        # Run the RabbitMQSensor to verify the message
        sensor = RabbitMQSensor(
            task_id=self.task_id,
            connection_uri=self.connection_uri,
            queue=self.queue,
            timeout=10,  # seconds
            poke_interval=1,
            mode="poke",
        )

        result = sensor.poke(context)
        assert result is True
