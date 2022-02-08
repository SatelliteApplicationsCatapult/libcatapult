
from libcatapult.queues.nats import NatsQueue


def wait(n):
    while n > 0:
        print("waiting...")
        n = n - 1

print("starting")
client = NatsQueue("nats://localhost:5555")
client.connect()
print("connected")
client.publish("test", "payload:payload")
# client.close()
print("done")

