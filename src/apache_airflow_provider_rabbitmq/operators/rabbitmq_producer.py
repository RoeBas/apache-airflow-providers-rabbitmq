from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from apache_airflow_provider_rabbitmq.hooks.rabbitmq_hook import RabbitMQHook
import asyncio
from typing import Any, Dict


class RabbitMQProducerOperator(BaseOperator):
    """
    Airflow Operator for publishing messages to RabbitMQ.

    Supports both synchronous (blocking) and asynchronous message publishing.

    :param connection_uri: The RabbitMQ connection URI (e.g., "amqp://user:password@host:port/vhost").
    :param message: The message to be sent to RabbitMQ.
    :param exchange: The RabbitMQ exchange name.
    :param routing_key: The routing key used for routing the message.
    :param use_async: Flag to determine whether to use async messaging. Defaults to False.
    """

    @apply_defaults
    def __init__(
            self,
            connection_uri: str,
            message: str,
            exchange: str,
            routing_key: str,
            use_async: bool = False,
            **kwargs: Dict[str, Any]
    ) -> None:
        super().__init__(**kwargs)
        self.connection_uri: str = connection_uri
        self.message: str = message
        self.exchange: str = exchange
        self.routing_key: str = routing_key
        self.use_async: bool = use_async

    def execute(self, context: Dict[str, Any]) -> None:
        """
        Executes the operator by publishing a message to RabbitMQ.

        Uses either synchronous (`pika`) or asynchronous (`aio-pika`) based on the `use_async` flag.

        :param context: Airflow's execution context dictionary.
        """
        hook = RabbitMQHook(self.connection_uri)

        if self.use_async:
            self.log.info("Publishing message asynchronously to RabbitMQ")
            asyncio.run(hook.publish_async(self.message, self.exchange, self.routing_key))
        else:
            self.log.info("Publishing message synchronously to RabbitMQ")
            hook.publish_sync(self.message, self.exchange, self.routing_key)