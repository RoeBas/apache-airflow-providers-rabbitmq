import time
from typing import Any, Dict, Optional
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from apache_airflow_provider_rabbitmq.hooks.rabbitmq_hook import RabbitMQHook
from pika.adapters.blocking_connection import BlockingChannel, BlockingConnection
from pika.frame import Method


class RabbitMQSensor(BaseSensorOperator):
    """
    Airflow Sensor to wait for messages in a RabbitMQ queue.

    This sensor periodically checks a specified RabbitMQ queue and triggers
    downstream tasks once a message is detected.

    :param connection_uri: The RabbitMQ connection URI (e.g., "amqp://user:password@host:port/vhost").
    :param queue: The name of the RabbitMQ queue to monitor.
    """

    @apply_defaults
    def __init__(self, connection_uri: str, queue: str, **kwargs: Dict[str, Any]) -> None:
        super().__init__(**kwargs)
        self.connection_uri: str = connection_uri
        self.queue: str = queue

    def poke(self, context: Dict[str, Any]) -> bool:
        """
        Checks the RabbitMQ queue for new messages.

        :param context: Airflow's execution context dictionary.
        :return: True if a message is found; otherwise, False.
        """
        hook = RabbitMQHook(self.connection_uri)
        try:
            conn: BlockingConnection = hook.get_sync_connection()
            channel: BlockingChannel = conn.channel()

            # Attempt to retrieve a message without consuming it
            method_frame: Optional[Method]
            method_frame, _, body = channel.basic_get(self.queue, auto_ack=True)

            # Close connection after checking the queue
            conn.close()

            if method_frame:
                self.log.info("Received message: %s", body)
                return True

        except Exception as e:
            self.log.error("Error during RabbitMQ poke: %s", e)

        # Avoid excessive polling
        time.sleep(1)
        return False