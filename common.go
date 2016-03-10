package pubsub

import "gopkg.in/vmihailenco/msgpack.v2"

// MessageType identifies the type of a Message
type MessageType int8

const (
	// Message Types

	// KeepAlive is a MessageType for keeping alive connections
	KeepAlive = 0

	// Authenticate is a MessageType for authenticating clients
	Authenticate = 1

	// Subscribe is a MessageType for subscribing to topics
	Subscribe = 2

	// Unsubscribe is a MessageType for unsubscribing from topics
	Unsubscribe = 3

	// Publish is a MessageType for publishing a message to a topic
	Publish = 4
)

// Message is a message
type Message struct {
	// Type of message (serialized as field "t")
	Type MessageType

	// Topic of message (serialized as field "o")
	Topic []byte

	// Body of message (serialized as field "b")
	Body []byte
}

// Define custom encoding for Message
var (
	_ msgpack.CustomEncoder = &Message{}
	_ msgpack.CustomDecoder = &Message{}
)

func (m *Message) EncodeMsgpack(enc *msgpack.Encoder) error {
	return enc.Encode(int8(m.Type), m.Topic, m.Body)
}

func (m *Message) DecodeMsgpack(dec *msgpack.Decoder) error {
	return dec.Decode(&m.Type, &m.Topic, &m.Body)
}
