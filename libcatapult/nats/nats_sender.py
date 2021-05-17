
import asyncio
from nats.aio.client import Client as NATS
from nats.aio.errors import ErrConnectionClosed, ErrTimeout, ErrNoServers


class NATSClient:

    nc = NATS()

    def __init__(self):
        await nc.connect()

    def send(self, topic, message):

