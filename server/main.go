package main

import (
	"flag"
	"fmt"
	"net"
	"os"

	"github.com/getlantern/pubsub"
	"github.com/golang/glog"
)

var (
	addr    = flag.String("addr", ":14080", "The address at which to listen for connections")
	authkey = flag.String("authkey", "", "The authentication key to use for authenticating publishers")
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

	broker := pubsub.NewBroker(&pubsub.BrokerConfig{
		AuthenticationKey: *authkey,
	})

	fmt.Fprintf(os.Stdout, "Listening for connections at %v\n", l.Addr())
	broker.Serve(l)
}
