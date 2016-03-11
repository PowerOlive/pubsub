// perfclient launches a bunch of parallel clients that subscribe to unique a
// unique topic per client (perfclient0, perfclient1, etc.).
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
)

var (
	addr       = flag.String("addr", "pubsub.lantern.io:14443", "The address to which to connect")
	numclients = flag.Int("numclients", 15000, "The number of concurrent clients to run")
	rampup     = flag.Duration("rampup", 15*time.Second, "How long to take ramping up all clients")
)

func main() {
	flag.Parse()

	received := make(chan int)
	rampupDelay := time.Duration(int64(*rampup) / int64(*numclients))

	dial := func() (net.Conn, error) {
		return tls.Dial("tcp", *addr, nil)
	}

	for i := 0; i < *numclients; i++ {
		go launchClient(i, dial, received)
		time.Sleep(rampupDelay)
	}

	fmt.Fprintf(os.Stderr, "Launched %d clients\n", *numclients)
	trackTPS(received)
}

func launchClient(i int, dial func() (net.Conn, error), received chan int) {
	client := pubsub.Connect(&pubsub.ClientConfig{
		Dial:          dial,
		InitialTopics: [][]byte{[]byte(fmt.Sprintf("perfclient%d", i))},
	})
	for j := 0; j < math.MaxInt32; j++ {
		client.Read()
		received <- 1
	}
}

func trackTPS(received chan int) {
	start := time.Now()
	total := int64(0)
	for i := range received {
		total += int64(i)
		if total%reportingInterval == 0 {
			tps := float64(reportingInterval*time.Second) / float64(time.Now().Sub(start))
			fmt.Fprintf(os.Stderr, "Total: %7d,   TPS: %f\n", total, tps)
			start = time.Now()
		}
	}
}
