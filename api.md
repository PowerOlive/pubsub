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

+ Request (application/json)

        {
            Topic: "TXkgVG9waWM=",
            Body: "SGVyZSBpcyBhIHNhbXBsZSBib2R5"
        }

+ Response 201
