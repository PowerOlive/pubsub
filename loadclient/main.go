// loadclient generates load by sending messages to a broker. It sends to
// random topics corresponding to the topics subscribed by perfclient
// (perfclient0, perfclient1, etc.).
package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"math"
	"net"
	"os"
	"time"

	"github.com/getlantern/pubsub"
)

const (
	reportingInterval = 10000
	checkInterval     = 1000
)

var (
	addr        = flag.String("addr", "pubsub.lantern.io:14443", "The address to which to connect")
	authkey     = flag.String("authkey", "", "The authentication key")
	numclients  = flag.Int("numclients", 15000, "The number of concurrent clients that are running")
	targettps   = flag.Int("targettps", 100000, "The target transactions per second")
	parallelism = flag.Int("parallel", 100, "The number of parallel clients to run")

	body = []byte("this is the message body")
)

func main() {
	flag.Parse()

	if *authkey == "" {
		fmt.Fprint(os.Stderr, "authkey is required")
		flag.Usage()
		os.Exit(1)
	}

	sent := make(chan int)
	targettpsPerClient := *targettps / *parallelism

	tlsConfig := &tls.Config{
		ClientSessionCache: tls.NewLRUClientSessionCache(10),
	}

	dial := func() (net.Conn, error) {
		return tls.Dial("tcp", *addr, tlsConfig)
	}

	for i := 0; i < *parallelism; i++ {
		go launchClient(targettpsPerClient, dial, sent)
	}

	fmt.Fprintf(os.Stderr, "Launched %d workers\n", *parallelism)
	trackTPS(sent)
}

func launchClient(targettps int, dial func() (net.Conn, error), sent chan int) {
	client := pubsub.Connect(&pubsub.ClientConfig{
		Dial:              dial,
		AuthenticationKey: *authkey,
	})

	targetDuration := time.Duration(checkInterval * time.Second / time.Duration(targettps))
	start := time.Now()
	for j := 0; j < math.MaxInt32; j++ {
		client.Publish([]byte(fmt.Sprintf("perfclient%d", j%*numclients)), body).Send()
		sent <- 1
		if j%checkInterval == 0 && j > 0 {
			delta := time.Now().Sub(start)
			delay := targetDuration - delta
			if delay > 0 {
				// Limit rate
				time.Sleep(delay)
			}
		}
	}
}

func trackTPS(sent chan int) {
	start := time.Now()
	total := int64(0)
	for i := range sent {
		total += int64(i)
		if total%reportingInterval == 0 {
			tps := float64(reportingInterval*time.Second) / float64(time.Now().Sub(start))
			fmt.Fprintf(os.Stderr, "Total: %7d,   TPS: %f\n", total, tps)
			start = time.Now()
		}
	}
}
