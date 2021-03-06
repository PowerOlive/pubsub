# pubsub

pubsub is a simplistic system for subscribing to and publishing messages where
simplicity and low overhead are more important than delivery guarantees.  It
works as follows:

- Clients connect to a broker process using TCP (optionally TLS)

- Clients subscribe to one or more topics. Topics are identified by a byte
  array. Any client can subscribe to any topic, there is no authentication.

- Clients can unsubscribe from any topic to which they previously subscribed.

- Subscriptions do not persist across client connections, so clients are
  responsible for re-subscribing after connecting. Consequently, clients cannot
  receive messages that were published while the client was disconnected.

- Clients publish messages to topics.  Messages have a body that's just a byte
  array.

- The broker delivers messages to all connected clients subscribed to a given
  topic.

- For clients to be allowed to publish messages, they have to authenticate
  first by sending an authentication message containing a secret value.

- In order to deal with slow readers, the server maintains a buffer of outgoing
  messages for each client. If a client's buffer is full, any new messages for
  the client are discarded until there is room in the buffer.

- If the broker encounters any errors on reading from/writing to a client, it
  disconnects. It is the client's responsibility to reconnect if it wants to
  continue receiving messages. Whatever message failed to send will not be
  retried.

- If the broker hasn't read from or written to a client within its idle timeout,
  the broker disconnects. This prevents the broker from needlessly maintaining a
  bunch of idle connections.

- Clients that would otherwise be idle send periodic keep-alive messages to the
  broker in order to keep it, and any intervening servers, from timing out the
  underlying TCP connection.

- The protocol does not account for the broker returning any errors to the
  client. If the broker encounters an abnormal condition, it will disconnect.

- Messages are framed and serialized using
  [MessagePack](http://msgpack.org/index.html). MessagePack was chosen because
  it is schema-less but still achieves a small serialized size and reasonably
  fast performance. See [here](https://github.com/eishay/jvm-serializers/wiki)
  for some useful comparisons.

### Java Client
[pubsub-java](https://github.com/getlantern/pubsub-java) is the Java client for
pubsub.

### Load Testing
To simulate a load, you can use the programs in `perfclient` and `loadclient` to
simulate a large number of clients (analogous to Lantern clients) and a single
client generating a lot of load (analogous to the config server).

```sh
ulimit -n 100000 && ./perfclient -numclients 15000 -stderrthreshold WARNING
```

```sh
./loadclient -authkey <publisher auth key here> -stderrthreshold WARNING
```

You can also generate load using the Java class
`org.getlantern.pubsub.LoadGeneratingClient` to see how the system performs with
the Java client library.

At the moment, with 15,000 connected clients and using a parallelism of 100 on
the `loadclient`, pubsub is able to process around 15 thousand messages per
second which corresponds to about 60 Mbit/s outbound on the wire per
`vnstat -l -i eth0`'s `tx` value.
