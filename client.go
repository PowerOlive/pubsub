package pubsub

import (
	"math"
	"net"
	"time"

	"github.com/golang/glog"

	"gopkg.in/vmihailenco/msgpack.v2"
)

type Client struct {
	cfg            *ClientConfig
	keepaliveTimer *time.Timer
	out            chan *Publishable
	in             chan *Message
	connect        chan interface{}
	conn           net.Conn
	enc            *msgpack.Encoder
}

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

	// InitialTopics: list of topics to which client will immediately subscribe
	InitialTopics [][]byte
}

type Publishable struct {
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
		keepaliveTimer: time.NewTimer(cfg.KeepalivePeriod),
		out:            make(chan *Publishable, 0),
		in:             make(chan *Message, 0),
		connect:        make(chan interface{}, 1),
	}
	go client.process()

	return client
}

func (c *Client) Read() *Message {
	return <-c.in
}

func (c *Client) ReadTimeout(timeout time.Duration) (*Message, bool) {
	select {
	case msg := <-c.in:
		return msg, true
	case <-time.After(timeout):
		return nil, false
	}
}

func (c *Client) Subscribe(topic []byte) *Publishable {
	return &Publishable{
		c:   c,
		msg: &Message{Type: Subscribe, Topic: topic},
	}
}

func (c *Client) Unsubscribe(topic []byte) *Publishable {
	return &Publishable{
		c:   c,
		msg: &Message{Type: Unsubscribe, Topic: topic},
	}
}

func (c *Client) Publish(topic, body []byte) *Publishable {
	return &Publishable{
		c:   c,
		msg: &Message{Type: Publish, Topic: topic, Body: body},
	}
}

func (p *Publishable) Send() {
	p.c.out <- p
}

func (p *Publishable) sendImmediate() error {
	p.c.resetKeepaliveTimer()
	err := p.c.enc.Encode(p.msg)
	if err == nil {
		glog.Infof("Sent message %v", p.msg)
	}
	return err
}

func (c *Client) process() {
	c.forceConnect()
	for {
		select {
		case p := <-c.out:
			glog.Info("Sending message")
			c.doWithConnection(p.sendImmediate)
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
		if c.conn == nil {
			glog.Info("Dialing new conn")
			c.conn, err = c.cfg.Dial()
			if err != nil {
				glog.Warningf("Unable to dial broker: %v", err)
				c.close()
				continue
			}

			// Set up msgpack
			c.enc = msgpack.NewEncoder(c.conn)

			// Send initial messages
			err = c.initialMessages()
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

func (c *Client) initialMessages() error {
	glog.Info("Sending initial messages")
	if c.cfg.AuthenticationKey != "" {
		err := c.authenticate(c.cfg.AuthenticationKey).sendImmediate()
		if err != nil {
			return err
		}
	}

	for _, topic := range c.cfg.InitialTopics {
		err := c.Subscribe(topic).sendImmediate()
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
			c.close()
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

func (c *Client) authenticate(authenticationKey string) *Publishable {
	return &Publishable{
		c:   c,
		msg: &Message{Type: Authenticate, Body: []byte(authenticationKey)},
	}
}

func (c *Client) keepAlive() *Publishable {
	return &Publishable{
		c:   c,
		msg: &Message{Type: KeepAlive},
	}
}

func (c *Client) resetKeepaliveTimer() {
	c.keepaliveTimer.Reset(c.cfg.KeepalivePeriod)
}

func (c *Client) close() {
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}
}
