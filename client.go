package pubsub

import (
	"fmt"
	"net"
	"time"

	"github.com/golang/glog"

	"gopkg.in/vmihailenco/msgpack.v2"
)

type Client struct {
	keepalivePeriod time.Duration
	keepaliveTimer  *time.Timer
	conn            net.Conn
	enc             *msgpack.Encoder
	dec             *msgpack.Decoder
}

type ClientConfig struct {
	// Dial: function for opening a connection to the broker
	Dial func() (net.Conn, error)

	// KeepalivePeriod: the client will send a message after being idle for
	// keepalivePeriod. Set this to the largest value you can that still keeps the
	// underlying connections from timing out due to idle timeouts on intervening
	// servers.
	KeepalivePeriod time.Duration

	// AuthenticationKey: if supplied, client will immediately authenticate with
	// this key.
	AuthenticationKey string

	// InitialTopics: list of topics to which client will immediately subscribe
	InitialTopics [][]byte
}

// Connect creates a Client
func Connect(cfg *ClientConfig) (*Client, error) {
	// Set sensible defaults
	if cfg.KeepalivePeriod == 0 {
		cfg.KeepalivePeriod = 30 * time.Second
	}

	conn, err := cfg.Dial()
	if err != nil {
		return nil, fmt.Errorf("Unable to dial server: %v", err)
	}
	client := &Client{
		keepalivePeriod: cfg.KeepalivePeriod,
		keepaliveTimer:  time.NewTimer(cfg.KeepalivePeriod),
		conn:            conn,
		enc:             msgpack.NewEncoder(conn),
		dec:             msgpack.NewDecoder(conn),
	}
	go client.keepAlive()

	if cfg.AuthenticationKey != "" {
		err := client.Authenticate(cfg.AuthenticationKey)
		if err != nil {
			client.Close()
			return nil, err
		}
	}

	for _, topic := range cfg.InitialTopics {
		err = client.Subscribe(topic)
		if err != nil {
			client.Close()
			return nil, err
		}
	}

	return client, nil
}

func (c *Client) Close() error {
	return c.conn.Close()
}

func (c *Client) Authenticate(authenticationKey string) error {
	err := c.publish(&Message{Type: Authenticate, Body: []byte(authenticationKey)})
	if err != nil {
		err = fmt.Errorf("Unable to authenticate: %v", err)
	}
	return err
}

func (c *Client) Subscribe(topic []byte) error {
	err := c.publish(&Message{Type: Subscribe, Topic: topic})
	if err != nil {
		fmt.Errorf("Unable to subscribe to topic: %v", err)
	}
	return err
}

func (c *Client) Unsubscribe(topic []byte) error {
	err := c.publish(&Message{Type: Unsubscribe, Topic: topic})
	if err != nil {
		fmt.Errorf("Unable to unsubscribe from topic: %v", err)
	}
	return err
}

func (c *Client) Publish(topic, body []byte) error {
	err := c.publish(&Message{Type: Publish, Topic: topic, Body: body})
	if err != nil {
		fmt.Errorf("Unable to publish: %v", err)
	}
	return err
}

func (c *Client) publish(msg *Message) error {
	c.resetKeepaliveTimer()
	return c.enc.Encode(msg)
}

func (c *Client) Read() (*Message, error) {
	for {
		msg := &Message{}
		c.conn.SetReadDeadline(time.Now().Add(c.keepalivePeriod))
		err := c.dec.Decode(msg)
		if err != nil {
			return nil, err
		}
		c.resetKeepaliveTimer()
		// Ignore KeepAlive messages
		if msg.Type != KeepAlive {
			return msg, nil
		}
	}
}

func (c *Client) keepAlive() {
	for range c.keepaliveTimer.C {
		// Send a KeepAlive message to keep underlying connection open
		err := c.publish(&Message{Type: KeepAlive})
		if err != nil {
			glog.Errorf("Unable to keep alive, disconnecting: %v", err)
			c.conn.Close()
			return
		}
	}
}

func (c *Client) resetKeepaliveTimer() {
	c.keepaliveTimer.Reset(c.keepalivePeriod)
}
