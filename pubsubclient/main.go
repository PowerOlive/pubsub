// testclient subscribes to one or more topics and prints out whatever it
// receives on those topics. Topics and message bodies are assumed to be UTF-8
// strings.
package main

import (
	"crypto/tls"
	"flag"
	"net"
	"os"
	"strings"

	"github.com/getlantern/golog"
	"github.com/getlantern/pubsub"
)

const (
	reportingInterval = 10000
)

var (
	log = golog.LoggerFor("testclient")

	addr   = flag.String("addr", "pubsub-test.lantern.io:14443", "The address to which to connect")
	topics = flag.String("topics", "", "Comma-separated list of topics")
)

func main() {
	flag.Parse()

	if *topics == "" {
		log.Error("Please specify one or more topics")
		flag.Usage()
		os.Exit(1)
	}

	client := pubsub.Connect(&pubsub.ClientConfig{
		Dial: func() (net.Conn, error) {
			return tls.Dial("tcp", *addr, nil)
		},
	})
	for _, topic := range strings.Split(*topics, ",") {
		log.Debugf("Subscribing to topic: %v", topic)
		client.Subscribe([]byte(topic))
	}

	log.Debug("Starting to read")

	for {
		msg := client.Read()
		log.Debugf("%v -> %v", string(msg.Topic), string(msg.Body))
	}
}
