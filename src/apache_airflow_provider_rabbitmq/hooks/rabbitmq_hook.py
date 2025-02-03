import pika
import asyncio
import aio_pika
from pika.adapters.blocking_connection import BlockingConnection
from pika.channel import Channel
from aio_pika.abc import AbstractRobustConnection, AbstractChannel


class RabbitMQHook:
    """
    Hook for interacting with RabbitMQ. Supports both synchronous and asynchronous messaging.
    """

    def __init__(self, connection_uri: str) -> None:
        """
        Initialize RabbitMQ connection settings.

        :param connection_uri: RabbitMQ connection string (e.g., "amqp://user:password@host:port/vhost").
        """
        self.connection_uri: str = connection_uri

    def get_sync_connection(self) -> BlockingConnection:
        """
        Establish a synchronous connection to RabbitMQ using pika.

        :return: A BlockingConnection instance.
        """
        params = pika.URLParameters(self.connection_uri)
        return pika.BlockingConnection(params)

    async def get_async_connection(self) -> AbstractRobustConnection:
        """
        Establish an asynchronous connection to RabbitMQ using aio-pika.

        :return: An aio_pika AbstractRobustConnection instance.
        """
        return await aio_pika.connect_robust(self.connection_uri)

    def publish_sync(self, message: str, exchange: str, routing_key: str) -> None:
        """
        Publish a message to RabbitMQ synchronously.

        :param message: The message to be sent.
        :param exchange: The name of the RabbitMQ exchange.
        :param routing_key: The routing key for message delivery.
        """
        conn: BlockingConnection = self.get_sync_connection()
        channel: Channel = conn.channel()

        # Publish the message
        channel.basic_publish(exchange=exchange, routing_key=routing_key, body=message)

        # Close the connection after publishing
        conn.close()

    async def publish_async(self, message: str, exchange: str, routing_key: str) -> None:
        """
        Publish a message to RabbitMQ asynchronously.

        :param message: The message to be sent.
        :param exchange: The name of the RabbitMQ exchange.
        :param routing_key: The routing key for message delivery.
        """
        connection: AbstractRobustConnection = await self.get_async_connection()
        channel: AbstractChannel = await connection.channel()

        # Publish the message asynchronously
        await channel.default_exchange.publish(
            aio_pika.Message(body=message.encode()), routing_key=routing_key
        )

        # Close the connection
        await connection.close()