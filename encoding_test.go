package pubsub

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"gopkg.in/vmihailenco/msgpack.v2"
)

func TestPublish(t *testing.T) {
	doTest(t, 14, &Message{
		Type:  Publish,
		Topic: []byte("topic"),
		Body:  []byte("body"),
	})
}

func TestKeepAlive(t *testing.T) {
	doTest(t, 1, &Message{
		Type: KeepAlive,
	})
}

func doTest(t *testing.T, expectedEncodedLength int, m *Message) {
	b, err := msgpack.Marshal(m)
	if !assert.NoError(t, err, "Unable to marshal value") {
		return
	}
	assert.Equal(t, expectedEncodedLength, len(b), "Encoded value was of unexpected length")
	m2 := &Message{}
	err = msgpack.Unmarshal(b, m2)
	if !assert.NoError(t, err, "Unable to unmarshal value") {
		return
	}

	assert.Equal(t, m, m2, "Messages did not match")
}
