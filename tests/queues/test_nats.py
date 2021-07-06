import pytest

from libcatapult.queues.base_queue import NotConnectedException
from libcatapult.queues.nats import NatsQueue


def test_not_connected():
    nc = NatsQueue("nats://somewhere:12345")
    with pytest.raises(NotConnectedException):
        nc.publish("a channel", "a message")