package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"

	"github.com/getlantern/pubsub"
	"github.com/getlantern/tlsdefaults"
	"github.com/golang/glog"
)

var (
	addr      = flag.String("addr", ":14443", "The address at which to listen for TLS connections")
	httpsaddr = flag.String("httpsaddr", ":62443", "The address at which to listen for HTTPS connections")
	pkfile    = flag.String("pkfile", "pk.pem", "Path to the private key PEM file")
	certfile  = flag.String("certfile", "cert.pem", "Path to the certificate PEM file")
	authkey   = flag.String("authkey", "", "The authentication key to use for authenticating publishers")
)

func main() {
	flag.Parse()

	if *authkey == "" {
		glog.Error("Please specify an authkey")
		flag.Usage()
		os.Exit(1)
	}

	l, err := tlsdefaults.Listen(*addr, *pkfile, *certfile)
	if err != nil {
		glog.Fatalf("Unable to listen: %v", err)
	}
	fmt.Fprintf(os.Stdout, "Listening for TLS connections at %v\n", l.Addr())

	hl, err := tlsdefaults.Listen(*httpsaddr, *pkfile, *certfile)
	if err != nil {
		glog.Fatalf("Unable to listen HTTPS: %v", err)
	}
	fmt.Fprintf(os.Stdout, "Listening for HTTPS connections at %v\n", hl.Addr())

	broker := pubsub.NewBroker(&pubsub.BrokerConfig{
		AuthenticationKey: *authkey,
	})

	go func() {
		err := http.Serve(hl, broker)
		if err != nil {
			glog.Fatalf("Error serving HTTPS: %v", err)
		}
	}()

	broker.Serve(l)
}
