package pubsub

import (
	"context"
	"encoding/json"

	amqp "github.com/rabbitmq/amqp091-go"
)

// PublishJSON publishes a value as JSON to a specified exchange using a key.
func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	// marshall the val to JSON bytes
	jsonBytes, err := json.Marshal(val)
	if err != nil {
		return err
	}
	// Use the channel's .PublishWithContext method to publish the message to the exchange with the routing key. Some configurations:
	//
	ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp.Publishing{
		ContentType: "application/json",
		Body:        jsonBytes,
	})

	return nil
}

