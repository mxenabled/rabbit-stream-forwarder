package main

import (
	"testing"
	"time"

	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
)

func testAmqpConnection() (*amqp.Connection, error) {
	return amqp.Dial("amqp://guest:guest@127.0.0.1:5672")
}

func TestForwarderBasicSetupAndTearDown(t *testing.T) {
	withTestStreamTransaction(t, func(forwarder *Forwarder) {
		go forwarder.Start(nil)
		forwarder.Stop()
	})
}

func TestForwarderPublishesMessagesIntoDestination(t *testing.T) {
	exchangeName := "test-output"
	queueName := "test-output"

	// Setup output exchange destination
	conn, err := testAmqpConnection()
	assert.Nil(t, err, "shall not error during amqp connection creation")
	defer conn.Close()

	ch, err := conn.Channel()
	assert.Nil(t, err, "shall not error during amqp channel creation")
	defer ch.Close()

	_, err = ch.QueueDeclare(
		queueName,
		true,
		false,
		false,
		false,
		amqp.Table{"x-queue-type": "classic"},
	)
	assert.Nil(t, err, "shall not error during queue declaration")

	err = ch.ExchangeDeclare(
		exchangeName,
		"topic",
		false,
		true,
		false,
		false,
		nil,
	)
	assert.Nil(t, err, "shall not error during exchange declaration")

	err = ch.QueueBind(
		queueName,
		"#",
		exchangeName,
		false,
		nil,
	)
	assert.Nil(t, err, "shall not error during queue binding")

	withTestStreamTransaction(t, func(forwarder *Forwarder) {
		go forwarder.Start(nil)

		for i := 0; i < 100; i++ {
			err := ch.Publish(
				exchangeName,
				"some.routing.key",
				false,
				false,
				amqp.Publishing{},
			)
			assert.Nil(t, err, "shall not error during stream publishing")

			// Even tests have to accomodate for slight
			// publishing latency.
			time.Sleep(time.Millisecond * 25)

			q, err := ch.QueueDeclarePassive(
				queueName,
				true,
				false,
				false,
				false,
				amqp.Table{"x-queue-type": "classic"},
			)
			assert.Nil(t, err, "shall not error while fetching message ready stats")

			assert.Equal(t, i+1, q.Messages, "There should be a message in the queue!")
		}

		forwarder.Stop()
	})

	_, err = ch.QueueDelete(
		queueName,
		false,
		false,
		false,
	)
	assert.Nil(t, err, "shall not error during queue deletion")

	err = ch.ExchangeDelete(
		exchangeName,
		false,
		false,
	)
	assert.Nil(t, err, "shall not error during exchange deletion")
}

func withTestStreamTransaction(t *testing.T, tx func(forwarder *Forwarder)) {
	offsetManager := NewDevNullOffsetManager()
	streamName := "rabbit-stream-forwarder-test"
	exchange := "rabbit-stream-forwarder-test"

	subConn, err := testAmqpConnection()
	assert.Nil(t, err, "shall not error during amqp connection creation")
	defer subConn.Close()

	pubConn, err := testAmqpConnection()
	assert.Nil(t, err, "shall not error during amqp connection creation")
	defer pubConn.Close()

	err = testCreateStream(pubConn, streamName, exchange)
	assert.Nil(t, err, "shall not error during test stream creation")

	forwarder, err := NewForwarder(
		subConn,
		pubConn,
		offsetManager,
		streamName,
		exchange,
		nil,
		false,
	)
	assert.Nil(t, err, "shall not error during creation with NewForwarder")

	// Execute transactional test.
	tx(forwarder)

	// Ensure stream is cleaned up.
	err = testDeleteStream(pubConn, streamName, exchange)
	assert.Nil(t, err, "Stream creation in test should not fail")
}

func testCreateStream(conn *amqp.Connection, streamName string, exchange string) error {
	ch, err := conn.Channel()
	if err != nil {
		return err
	}

	_, err = ch.QueueDeclare(
		streamName,
		true,
		false,
		false,
		false,
		amqp.Table{"x-queue-type": "stream"},
	)
	if err != nil {
		return err
	}

	err = ch.ExchangeDeclare(
		exchange,
		"topic",
		false,
		true,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	err = ch.QueueBind(
		streamName,
		"#",
		exchange,
		false,
		nil,
	)
	return err
}

func testDeleteStream(conn *amqp.Connection, streamName string, exchange string) error {
	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	_, err = ch.QueueDelete(
		streamName,
		false,
		false,
		false,
	)
	if err != nil {
		return err
	}

	err = ch.ExchangeDelete(
		exchange,
		false,
		false,
	)
	return err
}
