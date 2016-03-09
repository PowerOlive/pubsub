package pubsub

import (
	"net"
	"testing"
	"time"

	"github.com/getlantern/idletiming"
	"github.com/getlantern/testify/assert"
)

func TestRoundTrip(t *testing.T) {
	authenticationKey := "authentication key"

	l, err := net.Listen("tcp", "localhost:0")
	if !assert.NoError(t, err, "Unable to listen") {
		return
	}
	addr := l.Addr().String()
	broker := NewBroker(&Config{
		AuthenticationKey: authenticationKey,
		IdleTimeout:       50 * time.Millisecond,
	})
	go broker.Serve(l)

	dial := func() (net.Conn, error) {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			return nil, err
		}
		return idletiming.Conn(conn, 40*time.Millisecond, func() {
			conn.Close()
		}), nil
	}

	timeoutClient, err := Connect(&ClientConfig{
		Dial:            dial,
		KeepalivePeriod: 50 * time.Millisecond,
	})
	if !assert.NoError(t, err, "Unable to connect timeoutClient") {
		return
	}

	// Sleep to hit idle condition (should not trigger keepalive)
	time.Sleep(50 * time.Millisecond)
	err = timeoutClient.Subscribe([]byte("a"))
	assert.Error(t, err, "Subscribe should have failed due to connection timeout")

	clientA, err := Connect(&ClientConfig{
		Dial:            dial,
		KeepalivePeriod: 25 * time.Millisecond,
	})
	if !assert.NoError(t, err, "Unable to connect clientA") {
		return
	}

	clientB, err := Connect(&ClientConfig{
		Dial:            dial,
		KeepalivePeriod: 25 * time.Millisecond,
	})
	if !assert.NoError(t, err, "Unable to connect clientB") {
		return
	}

	// Sleep to hit idle condition (should trigger keepalive)
	time.Sleep(50 * time.Millisecond)

	topic := []byte("The Topic")
	body := []byte("The Body")

	err = clientB.Subscribe(topic)
	if !assert.NoError(t, err, "clientB unable to subscribe") {
		return
	}

	// Attempt to publish before authenticating
	err = clientA.Publish(topic, []byte("Bad Body"))
	if !assert.NoError(t, err, "clientA unable to publish unauthenticated") {
		return
	}

	// Authenticate first time, should fail because our previous publish call
	// caused broker to disconnect
	err = clientA.Authenticate(authenticationKey)
	if !assert.NoError(t, err, "clientA unable to authenticate") {
		return
	}

	// Reconnect
	clientA, err = Connect(&ClientConfig{
		Dial:              dial,
		KeepalivePeriod:   25 * time.Millisecond,
		InitialTopics:     [][]byte{topic},
		AuthenticationKey: authenticationKey,
	})
	if !assert.NoError(t, err, "Unable to reconnect clientA") {
		return
	}

	err = clientA.Publish(topic, body)
	if !assert.NoError(t, err, "clientA unable to publish authenticated") {
		return
	}

	msg, err := clientA.Read()
	if !assert.NoError(t, err, "clientA unable to read") {
		return
	}
	assert.Equal(t, topic, msg.Topic, "clientA received wrong topic")
	assert.Equal(t, body, msg.Body, "clientA received wrong body")

	msg, err = clientB.Read()
	if !assert.NoError(t, err, "clientB unable to read") {
		return
	}
	assert.Equal(t, topic, msg.Topic, "clientB received wrong topic")
	assert.Equal(t, body, msg.Body, "clientB received wrong body")

	err = clientA.Unsubscribe(topic)
	if !assert.NoError(t, err, "clientA unable to unsubscribe") {
		return
	}

	body2 := []byte("Body the sequel")
	err = clientA.Publish(topic, body2)
	if !assert.NoError(t, err, "clientA unable to publish again") {
		return
	}

	msg, err = clientA.Read()
	assert.Error(t, err, "clientA should not have been able to read again")

	msg, err = clientB.Read()
	if !assert.NoError(t, err, "clientB unable to read again") {
		return
	}
	assert.Equal(t, topic, msg.Topic, "clientB received wrong topic")
	assert.Equal(t, body2, msg.Body, "clientB received wrong body")
}
