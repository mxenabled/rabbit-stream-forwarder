package main

import (
	"errors"
	"fmt"

	"github.com/streadway/amqp"
)

func forwardDelivery(ch *amqp.Channel, msg amqp.Delivery, exchange string, debug bool) error {
	if debug {
		fmt.Println("Forwarding delivery...", exchange, msg.RoutingKey)
	}

	err := ch.Publish(
		exchange,       // exchange
		msg.RoutingKey, // routing key
		false,          // mandatory
		false,          // immediate
		convertDeliveryToPublishing(msg),
	)
	if err != nil {
		return err
	}
	return nil
}

func convertDeliveryToPublishing(msg amqp.Delivery) amqp.Publishing {
	return amqp.Publishing{
		Headers:     msg.Headers,
		ContentType: msg.ContentType,
		Body:        msg.Body,
	}
}

func printDelivery(msg amqp.Delivery) {
	fmt.Println(msg.Headers)
	fmt.Printf("Content-Type: %s\n", msg.ContentType)
	fmt.Println(string(msg.Body))
	fmt.Println(msg.RoutingKey)
}

func extractOffset(msg amqp.Delivery) (int64, error) {
	offset, ok := msg.Headers["x-stream-offset"]
	if !ok {
		return int64(0), errors.New("Stream offset not found on delivery!")
	}
	return offset.(int64), nil
}
