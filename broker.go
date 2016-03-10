package pubsub

import (
	"net"
	"time"

	"github.com/golang/glog"

	"gopkg.in/vmihailenco/msgpack.v2"
)

// BrokerConfig contains configuration for a Broker.
type BrokerConfig struct {
	// AuthenticationKey: the secret key that clients must specify in order to
	// be allowed to publish
	AuthenticationKey string

	// IdleTimeout: how long the server waits before disconnecting an idle connection
	IdleTimeout time.Duration

	// SubscribeBufferDepth: how many subscribe messages to buffer before back-pressuring to client
	SubscribeBufferDepth int

	// UnsubscribeBufferDepth: how many unsubscribe messages to buffer before back-pressuring to client
	UnsubscribeBufferDepth int

	// PublishBufferDepth: how many publish messages to buffer before back-pressuring to client
	PublishBufferDepth int

	// ClientBufferDepth: how many outbound messages to buffer per client before back-pressuring to broker
	ClientBufferDepth int
}

// A Broker handles communication between clients by allowing them to subscribe
// to and publish to topics.
type Broker struct {
	cfg             *BrokerConfig
	highestClientID int64
	clients         map[int64]*client
	subscriptions   map[string]map[int64]*client
	subscribe       chan *subscription
	unsubscribe     chan *subscription
	disconnect      chan *client
	out             chan *Message
}

type client struct {
	broker    *Broker
	id        int64
	conn      net.Conn
	dec       *msgpack.Decoder
	enc       *msgpack.Encoder
	out       chan *Message
	idleTimer *time.Timer
}

type subscription struct {
	topic  string
	client *client
}

// NewBroker creates a Broker from the given BrokerConfig.
func NewBroker(cfg *BrokerConfig) *Broker {
	// Apply sensible defaults
	if cfg.IdleTimeout == 0 {
		cfg.IdleTimeout = 70 * time.Second
	}

	if cfg.SubscribeBufferDepth == 0 {
		cfg.SubscribeBufferDepth = 100
	}

	if cfg.UnsubscribeBufferDepth == 0 {
		cfg.UnsubscribeBufferDepth = 100
	}

	if cfg.PublishBufferDepth == 0 {
		cfg.PublishBufferDepth = 100
	}

	if cfg.ClientBufferDepth == 0 {
		cfg.ClientBufferDepth = 10
	}

	return &Broker{
		cfg:           cfg,
		clients:       make(map[int64]*client),
		subscriptions: make(map[string]map[int64]*client),
		subscribe:     make(chan *subscription, cfg.SubscribeBufferDepth),
		unsubscribe:   make(chan *subscription, cfg.UnsubscribeBufferDepth),
		disconnect:    make(chan *client, cfg.UnsubscribeBufferDepth),
		out:           make(chan *Message, cfg.PublishBufferDepth),
	}
}

// Serve starts serving clients connecting via the given net.Listener.
func (b *Broker) Serve(l net.Listener) {
	go b.handleMessages()
	b.accept(l)
}

func (b *Broker) accept(l net.Listener) {
	for {
		conn, err := l.Accept()
		if err != nil {
			glog.Errorf("Unable to accept connection: %v", err)
		}
		b.newClient(conn)
	}
}

func (b *Broker) handleMessages() {
	for {
		select {
		case sub := <-b.subscribe:
			glog.Info("Subscribing")
			b.clientsFor(sub.topic)[sub.client.id] = sub.client
		case unsub := <-b.unsubscribe:
			glog.Info("Unsubscribing")
			delete(b.clientsFor(unsub.topic), unsub.client.id)
		case client := <-b.disconnect:
			glog.Info("Disconnecting client")
			for _, clients := range b.subscriptions {
				delete(clients, client.id)
			}
			client.conn.Close()
		case msg := <-b.out:
			for _, client := range b.clientsFor(string(msg.Topic)) {
				select {
				case client.out <- msg:
					// message submitted to client
				default:
					glog.Warningf("Client buffer full, discarding message")
				}
			}
		}
	}
}

func (b *Broker) clientsFor(topic string) map[int64]*client {
	clients, found := b.subscriptions[topic]
	if !found {
		clients = make(map[int64]*client)
		b.subscriptions[topic] = clients
	}
	return clients
}

func (b *Broker) newClient(conn net.Conn) *client {
	b.highestClientID++
	client := &client{
		broker:    b,
		id:        b.highestClientID,
		conn:      conn,
		dec:       msgpack.NewDecoder(conn),
		enc:       msgpack.NewEncoder(conn),
		out:       make(chan *Message, b.cfg.ClientBufferDepth),
		idleTimer: time.NewTimer(b.cfg.IdleTimeout),
	}
	go client.read()
	go client.write()
	b.clients[client.id] = client
	return client
}

func (c *client) read() {
	dec := msgpack.NewDecoder(c.conn)
	authenticated := false
	for {
		// Read message
		msg := &Message{}
		err := dec.Decode(msg)
		if err != nil {
			glog.Errorf("Unable to read message, disconnecting: %v", err)
			c.broker.disconnect <- c
			return
		}
		c.resetIdleTimer()

		// Process Message
		switch msg.Type {
		case KeepAlive:
			glog.Info("Sending back KeepAlive")
			// Send back a KeepAlive in case there's a downstream idle timeout on read
			c.out <- &Message{Type: KeepAlive}
		case Authenticate:
			glog.Info("Authenticating")
			if msg.Body == nil {
				glog.Warning("Attempted to authenticate with empty body, disconnecting")
				c.broker.disconnect <- c
				return
			}
			if string(msg.Body) == c.broker.cfg.AuthenticationKey {
				// TODO: this is theoretically vulnerable to replay attacks, fix it
				glog.Info("Authentication successful")
				authenticated = true
			} else {
				glog.Warning("AuthenticationKey did not match expected")
			}
		case Subscribe, Unsubscribe:
			if msg.Topic == nil {
				glog.Warning("Received subscription message with no topic, disconnecting")
				c.broker.disconnect <- c
				return
			}
			sub := &subscription{
				topic:  string(msg.Topic),
				client: c,
			}
			if msg.Type == Subscribe {
				c.broker.subscribe <- sub
			} else {
				c.broker.unsubscribe <- sub
			}
		case Publish:
			if !authenticated {
				glog.Warning("Unauthenticated client attempted to publish message, disconnecting")
				c.broker.disconnect <- c
				return
			}
			glog.Info("Publishing")
			if msg.Topic == nil {
				glog.Warning("Received publish message with no topic, disconnecting")
				c.broker.disconnect <- c
				return
			}
			c.broker.out <- msg
		}
	}
}

func (c *client) write() {
	for {
		select {
		case msg := <-c.out:
			c.resetIdleTimer()
			err := c.enc.Encode(msg)
			if err != nil {
				glog.Errorf("Unable to write message, disconnecting: %v", err)
				c.broker.disconnect <- c
				return
			}
		case <-c.idleTimer.C:
			glog.Info("Nothing heard from client within timeout, disconnecting")
			c.broker.disconnect <- c
			return
		}
	}
}

func (c *client) resetIdleTimer() {
	c.idleTimer.Reset(c.broker.cfg.IdleTimeout)
}
