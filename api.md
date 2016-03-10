FORMAT: 1A
HOST: https://io.lantern.io/

# pubsub

pubsub is an API for publishing messages to the pubsub message broker.

## Group Message

## Messages [/messages]

### Publish a message [POST]

This publishes a message to a topic. In pubsub, topics and bodies are just byte arrays, so to post these through JSON they have to be base64 encoded first.

+ topic (string) - The base64 encoded topic to which to publish
+ body (string) - The base64 encoded body of the message to publish

+ Request

  + Headers

            Content-Type: application/json
            X-Authentication-Key: <authentication key here>

  + Body

            {
                Topic: "TXkgVG9waWM=",
                Body: "SGVyZSBpcyBhIHNhbXBsZSBib2R5"
            }

+ Response 201 (text/plain)
+ Response 400 (text/plain)

            Message will indicate what specifically was wrong

+ Response 401 (text/plain)
+ Response 405 (text/plain)
+ Response 415 (text/plain)
