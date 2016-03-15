package pubsub

import (
	"net"
	"testing"
	"time"

	"github.com/getlantern/idletiming"
	"github.com/getlantern/testify/assert"
	"github.com/golang/glog"
)

func TestRoundTrip(t *testing.T) {
	authenticationKey := "authentication key"
	topic := []byte("The Topic")
	body := []byte("The Body")

	l, err := net.Listen("tcp", "localhost:0")
	if !assert.NoError(t, err, "Unable to listen") {
		return
	}
	addr := l.Addr().String()
	broker := NewBroker(&BrokerConfig{
		AuthenticationKey: authenticationKey,
		IdleTimeout:       50 * time.Millisecond,
	})
	go broker.Serve(l)

	dial := func() (net.Conn, error) {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			return nil, err
		}
		return idletiming.Conn(conn, 40*time.Hour, func() {
			conn.Close()
		}), nil
	}

	clientA := Connect(&ClientConfig{
		Dial:              dial,
		BackoffBase:       10 * time.Millisecond,
		KeepalivePeriod:   25 * time.Millisecond,
		AuthenticationKey: authenticationKey,
	})
	glog.Info("Connected clientA")

	clientB := Connect(&ClientConfig{
		Dial:            dial,
		BackoffBase:     10 * time.Millisecond,
		KeepalivePeriod: 25 * time.Millisecond,
	})
	clientB.Subscribe(topic)
	glog.Info("Connected clientB")

	// Sleep to hit idle condition (should trigger keepalive)
	time.Sleep(50 * time.Millisecond)

	// Subscribe to topic
	clientA.Subscribe(topic)

	// Publish with authenticated client
	clientA.Publish(topic, body)

	msg := clientA.Read()
	assert.Equal(t, topic, msg.Topic, "clientA received wrong topic")
	assert.Equal(t, body, msg.Body, "clientA received wrong body")

	msg = clientB.Read()
	assert.Equal(t, topic, msg.Topic, "clientB received wrong topic")
	assert.Equal(t, body, msg.Body, "clientB received wrong body")

	clientA.Unsubscribe(topic)

	body2 := []byte("Body the sequel")
	clientA.Publish(topic, body2)

	msg, ok := clientA.ReadTimeout(75 * time.Millisecond)
	assert.False(t, ok, "clientA should not have been able to read again")

	msg = clientB.Read()
	assert.Equal(t, topic, msg.Topic, "clientB received wrong topic")
	assert.Equal(t, body2, msg.Body, "clientB received wrong body")
}
