package pubsub

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
)

const (
	// XAuthenticationKey is the custom header containing the authentication key
	XAuthenticationKey = "X-Authentication-Key"

	// ContentType is the key for the Content-Type header
	ContentType = "Content-Type"

	// ContentTypeJSON is the allowed content type
	ContentTypeJSON = "application/json"
)

// JSONMessage is the datatype representing the request body
type JSONMessage struct {
	Topic string `json:"topic"`
	Body  string `json:"body"`
}

// ServeHTTP implements the http.Handler interface and supports publishing messages via HTTP.
func (b *Broker) ServeHTTP(resp http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		resp.WriteHeader(http.StatusMethodNotAllowed)
		fmt.Fprintf(resp, "Method %v not allowed\n", req.Method)
		return
	}

	contentType := req.Header.Get(ContentType)
	if contentType != ContentTypeJSON {
		resp.WriteHeader(http.StatusUnsupportedMediaType)
		fmt.Fprintf(resp, "Media type %v unsupported\n", contentType)
		return
	}

	authKey := req.Header.Get(XAuthenticationKey)
	if authKey != b.cfg.AuthenticationKey {
		resp.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(resp, "Wrong auth token\n")
		return
	}

	dec := json.NewDecoder(req.Body)
	jmsg := &JSONMessage{}
	err := dec.Decode(jmsg)
	if err != nil {
		badRequest(resp, "Error decoding JSON: %v", err)
		return
	}

	if jmsg.Topic == "" {
		badRequest(resp, "Missing topic")
		return
	}

	if jmsg.Body == "" {
		badRequest(resp, "Missing body")
		return
	}

	topic, err := base64.StdEncoding.DecodeString(jmsg.Topic)
	if err != nil {
		badRequest(resp, "Error decoding topic: %v", err)
		return
	}

	body, err := base64.StdEncoding.DecodeString(jmsg.Body)
	if err != nil {
		badRequest(resp, "Error decoding body: %v", err)
		return
	}

	msg := &Message{
		Type:  Publish,
		Topic: topic,
		Body:  body,
	}

	b.out <- msg

	resp.WriteHeader(http.StatusCreated)
	fmt.Fprintln(resp, "Success!")
}

func badRequest(resp http.ResponseWriter, msg string, args ...interface{}) {
	resp.WriteHeader(http.StatusBadRequest)
	fmt.Fprintf(resp, msg+"\n", args...)
}
