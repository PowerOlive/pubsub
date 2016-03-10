package main

import (
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"

	"github.com/getlantern/pubsub"
	"github.com/golang/glog"
)

var (
	addr     = flag.String("addr", ":1443", "The address at which to listen for connections")
	httpAddr = flag.String("httpaddr", ":443", "The address at which to listen for HTTP connections")
	authkey  = flag.String("authkey", "", "The authentication key to use for authenticating publishers")
)

func main() {
	flag.Parse()

	if *authkey == "" {
		glog.Error("Please specify an authkey")
		flag.Usage()
		os.Exit(1)
	}

	l, err := net.Listen("tcp", *addr)
	if err != nil {
		glog.Fatalf("Unable to listen: %v", err)
	}

	hl, err := net.Listen("tcp", *httpAddr)
	if err != nil {
		glog.Fatalf("Unable to listen HTTP: %v", err)
	}

	broker := pubsub.NewBroker(&pubsub.BrokerConfig{
		AuthenticationKey: *authkey,
	})

	go func() {
		err := http.Serve(hl, broker)
		if err != nil {
			glog.Fatalf("Error serving HTTP: %v", err)
		}
	}()

	fmt.Fprintf(os.Stdout, "Listening for connections at %v\n", l.Addr())
	broker.Serve(l)
}
