// loadclient generates load by sending messages to a broker. It sends to
// random topics corresponding to the topics subscribed by perfclient
// (perfclient0, perfclient1, etc.).
package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"net/http"
	"os"
	"time"

	"github.com/getlantern/pubsub"
	"github.com/golang/glog"
)

const (
	reportingInterval = 10000
	checkInterval     = 1000
)

var (
	addr        = flag.String("addr", "pubsub.lantern.io:443", "The address to which to connect")
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

	for i := 0; i < *parallelism; i++ {
		go launchClient(targettpsPerClient, sent)
	}

	fmt.Fprintf(os.Stderr, "Launched %d clients\n", *parallelism)
	trackTPS(sent)
}

func launchClient(targettps int, sent chan int) {
	client := &http.Client{}

	targetDuration := time.Duration(checkInterval * time.Second / time.Duration(targettps))
	start := time.Now()
	for j := 0; j < math.MaxInt32; j++ {
		b := new(bytes.Buffer)
		json.NewEncoder(b).Encode(&pubsub.JSONMessage{
			Topic: base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("perfclient%d", j%*numclients))),
			Body:  base64.StdEncoding.EncodeToString(body),
		})
		req, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("http://%v/messages", *addr), b)
		req.Header.Set(pubsub.ContentType, pubsub.ContentTypeJSON)
		req.Header.Set(pubsub.XAuthenticationKey, *authkey)
		resp, err := client.Do(req)
		if resp.Body != nil {
			io.Copy(ioutil.Discard, resp.Body)
			resp.Body.Close()
		}
		if err != nil {
			glog.Warningf("Error making HTTP request: %v", err)
		}
		if resp.StatusCode != http.StatusCreated {
			glog.Warningf("Unexpected response status: %d", resp.StatusCode)
		}
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
