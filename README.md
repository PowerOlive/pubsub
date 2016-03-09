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
