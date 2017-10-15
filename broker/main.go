package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"net/http"
	"os"

	"github.com/getlantern/pubsub"
	"github.com/golang/glog"
	"golang.org/x/crypto/acme/autocert"
)

var (
	addr      = flag.String("addr", ":14443", "The address at which to listen for TLS connections")
	httpsaddr = flag.String("httpsaddr", ":62443", "The address at which to listen for HTTPS connections")
	authkey   = flag.String("authkey", "", "The authentication key to use for authenticating publishers")
)

func main() {
	flag.Parse()

	if *authkey == "" {
		glog.Error("Please specify an authkey")
		flag.Usage()
		os.Exit(1)
	}

	m := autocert.Manager{
		Prompt: autocert.AcceptTOS,
		Cache:  autocert.DirCache("certs"),
		Email:  "admin@getlantern.org",
	}
	tlsConfig := &tls.Config{
		GetCertificate:           m.GetCertificate,
		PreferServerCipherSuites: true,
	}

	l, err := tls.Listen("tcp", *addr, tlsConfig)
	if err != nil {
		glog.Fatalf("Unable to listen: %v", err)
	}
	fmt.Fprintf(os.Stdout, "Listening for TLS connections at %v\n", l.Addr())

	hl, err := tls.Listen("tcp", *httpsaddr, tlsConfig)
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
