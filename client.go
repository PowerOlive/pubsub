package pubsub

import (
	"math"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/glog"

	"gopkg.in/vmihailenco/msgpack.v2"
)

// ClientConfig configures a Client.
type ClientConfig struct {
	// Dial: function for opening a connection to the broker
	Dial func() (net.Conn, error)

	// BackoffBase: in case client needs to redial, it will back-off
	// exponentially using the formula BackoffBase^(numFailures). Defaults to 1s.
	BackoffBase time.Duration

	// MaxBackoff: when backing off during redialing, this caps the wait time.
	// Defaults to 1m.
	MaxBackoff time.Duration

	// KeepalivePeriod: the client will send a message after being idle for
	// keepalivePeriod. Set this to the largest value you can that still keeps the
	// underlying connections from timing out due to idle timeouts on intervening
	// servers. Defaults to 30s.
	KeepalivePeriod time.Duration

	// AuthenticationKey: if supplied, client will immediately authenticate with
	// this key.
	AuthenticationKey string
}

// Client is a client that is able to communicate with a Broker over the
// network.
type Client struct {
	cfg                *ClientConfig
	subscriptions      map[string][]byte
	subscriptionsMutex sync.RWMutex
	keepaliveTimer     *time.Timer
	out                chan *Sendable
	in                 chan *Message
	forceReconnect     int32
	connect            chan interface{}
	conn               net.Conn
	enc                *msgpack.Encoder
}

// Sendable represents a Message that can be published to a specific client.
type Sendable struct {
	c   *Client
	msg *Message
}

// Connect creates a Client
func Connect(cfg *ClientConfig) *Client {
	// Set sensible defaults
	if cfg.BackoffBase == 0 {
		cfg.BackoffBase = 1 * time.Second
	}

	if cfg.MaxBackoff == 0 {
		cfg.MaxBackoff = 1 * time.Minute
	}

	if cfg.KeepalivePeriod == 0 {
		cfg.KeepalivePeriod = 30 * time.Second
	}

	client := &Client{
		cfg:            cfg,
		subscriptions:  make(map[string][]byte, 0),
		keepaliveTimer: time.NewTimer(cfg.KeepalivePeriod),
		out:            make(chan *Sendable, 0),
		in:             make(chan *Message, 0),
		connect:        make(chan interface{}, 1),
	}
	go client.process()

	return client
}

// Read reads the next Message from the Broker.
func (c *Client) Read() *Message {
	return <-c.in
}

// ReadTimeout reads the next message from the Broker or returns nil, false if
// no message is read within the given timeout.
func (c *Client) ReadTimeout(timeout time.Duration) (msg *Message, ok bool) {
	select {
	case msg := <-c.in:
		return msg, true
	case <-time.After(timeout):
		return nil, false
	}
}

// Subscribe subscribes this client to the given topic.
func (c *Client) Subscribe(topic []byte) {
	c.subscriptionsMutex.Lock()
	c.subscriptions[string(topic)] = topic
	c.subscriptionsMutex.Unlock()
	c.subscribe(topic).Send()
}

func (c *Client) subscribe(topic []byte) *Sendable {
	return &Sendable{
		c:   c,
		msg: &Message{Type: Subscribe, Topic: topic},
	}
}

// Unsubscribe unsubscribes this client from the given topic.
func (c *Client) Unsubscribe(topic []byte) {
	c.subscriptionsMutex.Lock()
	delete(c.subscriptions, string(topic))
	c.subscriptionsMutex.Unlock()
	c.unsubscribe(topic).Send()
}

func (c *Client) unsubscribe(topic []byte) *Sendable {
	return &Sendable{
		c:   c,
		msg: &Message{Type: Unsubscribe, Topic: topic},
	}
}

// Publish publishes a message with the given body to the given topic.
func (c *Client) Publish(topic, body []byte) {
	s := &Sendable{
		c:   c,
		msg: &Message{Type: Publish, Topic: topic, Body: body},
	}
	s.Send()
}

// Send sends the Sendable eventually (queues behind other Sends and ops).
func (s *Sendable) Send() {
	s.c.out <- s
}

func (s *Sendable) sendImmediate() error {
	s.c.resetKeepaliveTimer()
	err := s.c.enc.Encode(s.msg)
	if err == nil {
		glog.Infof("Sent message %v", s.msg)
	}
	return err
}

func (c *Client) process() {
	c.forceConnect()
	for {
		select {
		case s := <-c.out:
			glog.Info("Sending message")
			c.doWithConnection(s.sendImmediate)
		case <-c.keepaliveTimer.C:
			glog.Info("Sending KeepAlive")
			c.doWithConnection(c.keepAlive().sendImmediate)
		case <-c.connect:
			glog.Info("Ensuring we have a connection")
			c.doWithConnection(func() error { return nil })
		}
	}
}

func (c *Client) doWithConnection(op func() error) {
	for numFailures := 0; numFailures < math.MaxInt32; numFailures++ {
		// Back off if necessary
		if numFailures > 0 {
			backoff := time.Duration(math.Pow(float64(c.cfg.BackoffBase), float64(numFailures)))
			if backoff > c.cfg.MaxBackoff {
				backoff = c.cfg.MaxBackoff
			}
			glog.Infof("%d consecutive failures, waiting %v before retrying", numFailures, backoff)
			time.Sleep(backoff)
		}

		var err error
		if atomic.CompareAndSwapInt32(&c.forceReconnect, 1, 0) || c.conn == nil {
			c.close()
			glog.Info("Dialing new conn")
			c.conn, err = c.cfg.Dial()
			if err != nil {
				c.conn = nil
				glog.Warningf("Unable to dial broker: %v", err)
				continue
			}

			// Set up msgpack
			c.enc = msgpack.NewEncoder(c.conn)

			// Send initial messages
			err = c.sendInitialMessages()
			if err != nil {
				glog.Warningf("Error sending initial messages: %v", err)
				c.close()
				continue
			}

			// Successfully connected, start reading
			go c.readLoop(msgpack.NewDecoder(c.conn))
		}

		err = op()
		if err != nil {
			glog.Warningf("Unable to process op: %v", err)
			c.close()
			continue
		}

		glog.Info("Op succesful")
		return
	}
}

func (c *Client) sendInitialMessages() error {
	glog.Info("Sending initial messages")
	if c.cfg.AuthenticationKey != "" {
		err := c.authenticate(c.cfg.AuthenticationKey).sendImmediate()
		if err != nil {
			return err
		}
	}

	c.subscriptionsMutex.RLock()
	initialTopics := make([][]byte, 0, len(c.subscriptions))
	for _, topic := range c.subscriptions {
		initialTopics = append(initialTopics, topic)
	}
	c.subscriptionsMutex.RUnlock()

	for _, topic := range initialTopics {
		err := c.subscribe(topic).sendImmediate()
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *Client) readLoop(dec *msgpack.Decoder) {
	glog.Info("Start reading")
	for {
		msg := &Message{}
		err := dec.Decode(msg)
		if err != nil {
			glog.Warningf("Unable to read, stopping readLoop: %v", err)
			atomic.StoreInt32(&c.forceReconnect, 1)
			c.forceConnect()
			return
		}
		c.resetKeepaliveTimer()
		if msg.Type != KeepAlive {
			glog.Info("Read message")
			c.in <- msg
		} else {
			glog.Info("Ignoring KeepAlive message")
		}
	}
}

func (c *Client) forceConnect() {
	select {
	case c.connect <- nil:
	default:
		// Already attempting to force connection, don't bother doing it again
	}
}

func (c *Client) authenticate(authenticationKey string) *Sendable {
	return &Sendable{
		c:   c,
		msg: &Message{Type: Authenticate, Body: []byte(authenticationKey)},
	}
}

func (c *Client) keepAlive() *Sendable {
	return &Sendable{
		c:   c,
		msg: &Message{Type: KeepAlive},
	}
}

func (c *Client) resetKeepaliveTimer() {
	c.keepaliveTimer.Reset(c.cfg.KeepalivePeriod)
}

func (c *Client) close() {
	if c.conn != nil {
		glog.Info("Closing connection")
		c.conn.Close()
		c.conn = nil
	}
}
