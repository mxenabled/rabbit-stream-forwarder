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

## Docker

### Build
```
docker build . -t rabbit-stream-forwarder
```

### Running

#### Environmental Variables

|Var|default|description|
|---|---|---|
|AMQP_URI|amqp://guest:guest@localhost:5672|URI to connect to|
|STREAM_NAME||Stream name to forward|
|CONSUMER_NAME|rabbit-stream-forwarder|name to use on consuming functions in rabbit|
|EXCHANGE|events|name of exchange to forward messages to|
|OVERRIDE_OFFSET|false||
|OFFSET|last||
|OFFSET_MANAGER_TYPE|rabbit||
|OFFSET_FILE_PATH|/tmp/rabbit-stream-forwarder-offset||
|DEBUG|false||
|TLS_CA_FILE||client ca file path|
|TLS_CLIENT_CERT_FILE||client certificate file path|
|TLS_CLIENT_KEY_FILE||client key file path
|USE_STATSD|true||
|STATSD_URI|127.0.0.1:8125||

