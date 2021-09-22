# Rabbit Stream Forwarder

RabbitMQ introduced the concept of `streams` in versions `3.9+`.  Rabbit Streams are very similar to Kafka topics
under the hood.  They are append only logs that can configure TTL.

Rabbit Stream Forwarder is an active "shovel" type process that forwards messages written to a Stream, to an outbound exchange.
In some tech ecosystems that revolve around RabbitMQ, publishers can be bottlenecked due to a complex exchange topology and many queue bindings to the same routing keys.  This can be a problem
for a variety of reasons.  With this, it is possible to increase publishing throughput using a Stream as a buffer queue.


### Getting Started
```
go build && ./rabbit-stream-forwarder --help
```
