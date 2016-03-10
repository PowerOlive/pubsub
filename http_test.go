package pubsub

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"net"
	"net/http"
	"testing"

	"github.com/getlantern/testify/assert"
	"github.com/golang/glog"
)

func TestHTTPRoundTrip(t *testing.T) {
	authenticationKey := "authentication key"
	topic := []byte("The Topic")
	body := []byte("The Body")

	l, err := net.Listen("tcp", "localhost:0")
	if !assert.NoError(t, err, "Unable to listen") {
		return
	}
	addr := l.Addr().String()

	hl, err := net.Listen("tcp", "localhost:0")
	if !assert.NoError(t, err, "Unable to listen HTTP") {
		return
	}
	httpAddr := hl.Addr().String()

	broker := NewBroker(&BrokerConfig{
		AuthenticationKey: authenticationKey,
	})
	go broker.Serve(l)
	go http.Serve(hl, broker)

	dial := func() (net.Conn, error) {
		return net.Dial("tcp", addr)
	}
	client := Connect(&ClientConfig{
		Dial:          dial,
		InitialTopics: [][]byte{topic},
	})
	glog.Info("Connected client")

	goodContentType := ContentTypeJSON
	badContentType := "somethingelse"
	goodAuthKey := authenticationKey
	badAuthKey := "bad authentication key"
	goodTopic := base64.StdEncoding.EncodeToString(topic)
	goodBody := base64.StdEncoding.EncodeToString(body)
	badTopic := string([]byte{0, 0, 0, 0})
	badBody := badTopic

	resp, _ := request(httpAddr, badContentType, goodAuthKey, goodTopic, goodBody)
	assert.Equal(t, http.StatusUnsupportedMediaType, resp.StatusCode)

	resp, _ = request(httpAddr, goodContentType, badAuthKey, goodTopic, goodBody)
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)

	resp, _ = request(httpAddr, goodContentType, goodAuthKey, "", "")
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

	resp, _ = request(httpAddr, goodContentType, goodAuthKey, "", goodBody)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

	resp, _ = request(httpAddr, goodContentType, goodAuthKey, badTopic, goodBody)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

	resp, _ = request(httpAddr, goodContentType, goodAuthKey, goodTopic, "")
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

	resp, _ = request(httpAddr, goodContentType, goodAuthKey, goodTopic, badBody)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

	resp, _ = request(httpAddr, goodContentType, goodAuthKey, goodTopic, goodBody)
	if !assert.Equal(t, http.StatusCreated, resp.StatusCode) {
		return
	}

	msg := client.Read()
	assert.Equal(t, topic, msg.Topic, "client received wrong topic")
	assert.Equal(t, body, msg.Body, "client received wrong body")
}

func request(addr string, contentType string, authKey string, topic string, body string) (*http.Response, error) {
	client := &http.Client{}
	b := new(bytes.Buffer)
	if topic == "" && body == "" {
		b.Write([]byte("Not valid JSON"))
	} else {
		err := json.NewEncoder(b).Encode(&JSONMessage{
			Topic: topic,
			Body:  body,
		})
		if err != nil {
			return nil, err
		}
	}
	req, err := http.NewRequest(http.MethodPost, "http://"+addr+"/messages", b)
	if err != nil {
		return nil, err
	}
	req.Header.Set(ContentType, contentType)
	req.Header.Set(XAuthenticationKey, authKey)
	return client.Do(req)
}
