
from libcatapult.queues.nats import NatsQueue

client = NatsQueue("nats://localhost:4222")
client.connect()
client.publish("test", "payload:payload")
# client.close()
