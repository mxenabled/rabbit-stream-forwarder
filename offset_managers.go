package main

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strconv"

	"github.com/streadway/amqp"
)

var ErrFirstRunUserMustSpecifyOffset = errors.New("Forwarder with name not found, stream created, but user must specify offset!")

// OffsetManager is interface for callers to use as there are different strategies in managing offset.
// Current implementations are RabbitOffsetManager, FileOffsetManager, and DevNullOffsetManager.
type OffsetManager interface {
	GetOffset() (int64, error)
	WriteOffset(offset int64) error
}

// RabbitOffsetManager utilizes an AMQP connection and creates a stream to manage offset for the forwarder
// to survive process restarts and resume from work had left off.
type RabbitOffsetManager struct {
	conn             *amqp.Connection
	publishChan      *amqp.Channel
	consumeChan      *amqp.Channel
	consumerName     string
	streamName       string
	offsetStreamName string
}

// NewRabbitOffsetManager opens a channel to be used for offset management through a stream.
func NewRabbitOffsetManager(connection *amqp.Connection, consumerName string, streamName string) (RabbitOffsetManager, error) {
	offsetStreamName := fmt.Sprintf("rabbit-stream-forwarder.offset.tracking.%s.%s", consumerName, streamName)
	publishChan, err := connection.Channel()
	if err != nil {
		return RabbitOffsetManager{}, err
	}
	err = publishChan.ExchangeDeclare(
		"rabbit-stream-forwarder",
		"topic",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return RabbitOffsetManager{}, err
	}

	q, err := publishChan.QueueDeclare(
		offsetStreamName,
		true,
		false,
		false,
		false,
		amqp.Table{"x-queue-type": "stream"},
	)
	if err != nil {
		return RabbitOffsetManager{}, err
	}

	err = publishChan.QueueBind(
		offsetStreamName,
		offsetStreamName,
		"rabbit-stream-forwarder",
		false,
		nil,
	)
	if err != nil {
		return RabbitOffsetManager{}, err
	}

	if q.Messages < 1 {
		fmt.Println("Stream for offset management has no messages in it, initializing to 0")
		err := publishChan.Publish(
			"rabbit-stream-forwarder", // exchange
			offsetStreamName,          // routing key
			false,                     // mandatory
			false,                     // immediate
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte("0"),
			},
		)
		if err != nil {
			return RabbitOffsetManager{}, err
		}
		fmt.Printf("Successfully published initialization message of offset 0 to stream %s\n", offsetStreamName)
	}
	if err != nil {
		return RabbitOffsetManager{}, err
	}

	consumeChan, err := connection.Channel()
	if err != nil {
		return RabbitOffsetManager{}, err
	}
	consumeChan.Qos(1, 0, false)

	return RabbitOffsetManager{
		conn:             connection,
		publishChan:      publishChan,
		consumeChan:      consumeChan,
		consumerName:     consumerName,
		streamName:       streamName,
		offsetStreamName: offsetStreamName,
	}, nil
}

// GetOffset declares a stream on the rabbit connection to a specific rabbit-stream-forwarder exchange
// if it did not previously exist (detected by the amount of messages on the stream) returns error and leaves
// it up to the caller to determine what to use for offset.  If it already exists and there are messages,
// will retrieve the last message and parses the bytes to an int64 value.
func (r RabbitOffsetManager) GetOffset() (int64, error) {
	var foundOffset int64

	msgs, err := r.consumeChan.Consume(
		r.offsetStreamName,
		"stream-forwarder",
		false,
		true,
		false,
		false,
		amqp.Table{"x-stream-offset": "last"},
	)
	if err != nil {
		log.Fatal(err)
	}

	msg := <-msgs
	foundOffset, err = strconv.ParseInt(string(msg.Body), 10, 64)
	if err != nil {
		return foundOffset, err
	}
	fmt.Printf("Found offset %s from offset tracking stream %s\n", string(msg.Body), r.offsetStreamName)

	return foundOffset, nil
}

// WriteOffset writes a message to the stream containing the value of the offset.  The
// message is then used to survive process restarts and resume where the last process left off.
func (r RabbitOffsetManager) WriteOffset(offset int64) error {
	fmt.Printf("Publishing offset %d to stream %s\n", offset, r.offsetStreamName)
	// Maybe need to publisher confirm this
	return r.publishChan.Publish(
		"rabbit-stream-forwarder", // exchange
		r.offsetStreamName,        // routing key
		false,                     // mandatory
		false,                     // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(strconv.FormatInt(offset, 10)),
		},
	)
}

// DeleteStream is used mainly for testing (or whatever scenario it may be useful) to
// remove the stream that stores offset.
func (r RabbitOffsetManager) DeleteStream() error {
	_, err := r.publishChan.QueueDelete(
		r.offsetStreamName,
		false,
		false,
		false,
	)
	if err != nil {
		return err
	}

	err = r.publishChan.ExchangeDelete("rabbit-stream-forwarder", false, false)
	if err != nil {
		return err
	}

	return nil
}

// FileOffsetManager manages offset through a file on disk.
type FileOffsetManager struct {
	filePath string
}

// NewFileOffsetManager returns a new FileOffsetManager
func NewFileOffsetManager(filePath string) (FileOffsetManager, error) {
	if filePath == "" {
		return FileOffsetManager{}, errors.New("filePath with FileOffsetManager can not be empty")
	}
	return FileOffsetManager{
		filePath: filePath,
	}, nil
}

// GetOffset retrieves offset from file path given.  If file does not exist
// returns 0 and error.
func (r FileOffsetManager) GetOffset() (int64, error) {
	var offset int64
	fmt.Println("Fetching offset file...")
	f, err := os.OpenFile(r.filePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return offset, err
	}
	f.Close()

	content, err := ioutil.ReadFile(r.filePath)
	if err != nil {
		return offset, err
	}

	stringContent := string(content)
	if stringContent == "" {
		return offset, nil
	} else {
		offset, err := strconv.ParseInt(stringContent, 10, 64)
		if err != nil {
			return offset, errors.New(fmt.Sprintf("Invalid value found in offset file: %s\n", stringContent))
		}
		return offset, nil
	}
}

// WriteOffset writes the given offset to filePath.
func (r FileOffsetManager) WriteOffset(offset int64) error {
	fmt.Printf("Committing offset %d to offset file...\n", offset)
	content := fmt.Sprintf("%d", offset)
	return ioutil.WriteFile(r.filePath, []byte(content), 0644)
}

// DevNullOffsetManager is an offset manager that throws offsets into the ether.
type DevNullOffsetManager struct{}

// NewDevNullOffsetManager
func NewDevNullOffsetManager() DevNullOffsetManager {
	return DevNullOffsetManager{}
}

// GetOffset for devnull manager just returns a 0.
func (r DevNullOffsetManager) GetOffset() (int64, error) {
	return int64(0), nil
}

// WriteOffset does nothing.
func (r DevNullOffsetManager) WriteOffset(offset int64) error {
	return nil
}
