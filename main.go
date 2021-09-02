package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strconv"

	"github.com/cactus/go-statsd-client/v5/statsd"
	"github.com/streadway/amqp"
)

var (
	amqpUri           = flag.String("amqp-uri", "amqp://guest:guest@localhost:5672", "uri to connect to")
	streamName        = flag.String("stream-name", "", "stream name to forward")
	consumerName      = flag.String("consumer-name", "", "name to use on consuming functions in rabbit")
	exchange          = flag.String("exchange", "events", "name of exchange to forward messages to")
	offset            = flag.String("offset", "last", "offset to start from, valid values are last, first, exact offset (digits)")
	manageOffset      = flag.Bool("manage-offset", false, "if true, manually tracks offset and stores in file")
	offsetManagerType = flag.String("offset-manager-type", "rabbit", "type of offset manager to use, valid values are file, rabbit, none")
	offsetFilePath    = flag.String("offset-file-path", "/tmp/stream-forwarder-offset", "file used to manage offset with the manage-offset flag")
	publishChannelQty = flag.Int("publish-channel-qty", 1, "amount of channels to open and utilize for publishing")
	debug             = flag.Bool("debug", false, "if set to true will enable more verbose logging")

	tlsCaFile         = flag.String("tls-client-ca-file", "", "client ca file path")
	tlsClientCertFile = flag.String("tls-client-cert-file", "", "client certificate file path")
	tlsClientKeyFile  = flag.String("tls-client-key-file", "", "client key file path")

	useStatsd = flag.Bool("use-statsd", false, "set to true to push offset status to statsd")
	statsdUri = flag.String("statsd-uri", "127.0.0.1:8125", "uri to use for statsd client")
)

func main() {
	flag.Parse()
	var trackedOffset int64
	var offsetManager OffsetManager

	if *streamName == "" {
		log.Fatal("Stream name must be specified! Shutting down..")
	}

	var st statsdTracker
	if *useStatsd {
		config := &statsd.ClientConfig{
			Address: *statsdUri,
			Prefix:  "stream-forwarder",
		}
		c, err := statsd.NewClientWithConfig(config)
		if err != nil {
			log.Fatal(err)
		}
		defer c.Close()
		st = NewStatsdTracker(c, int64(0))
	}
	fmt.Println("Debug mode:", *debug)

	subConn, err := createAmqpConn(*amqpUri, *tlsCaFile, *tlsClientCertFile, *tlsClientKeyFile)
	defer subConn.Close()

	pubConn, err := createAmqpConn(*amqpUri, *tlsCaFile, *tlsClientCertFile, *tlsClientKeyFile)
	defer subConn.Close()

	ch, err := subConn.Channel()
	if err != nil {
		log.Fatal(err)
	}
	defer ch.Close()

	if *manageOffset {
		switch *offsetManagerType {
		case "rabbit":
			rom, err := NewRabbitOffsetManager(
				pubConn,
				*consumerName,
				*streamName,
			)
			if err != nil {
				log.Fatal(err)
			}
			offsetManager = rom
		case "file":
			fom, err := NewFileOffsetManager(*offsetFilePath)
			if err != nil {
				log.Fatal(err)
			}
			offsetManager = fom
		case "none":
			offsetManager = NewDevNullOffsetManager()
		default:
			log.Fatal("Manage offset was specified without a valid offset-manager value")
		}
	}

	var evaluatedOffset interface{}
	if *manageOffset {
		evaluatedOffset, err = offsetManager.GetOffset()
		switch err {
		case ErrFirstRunUserMustSpecifyOffset:
			evaluatedOffset = "first"
		case nil:
		default:
			log.Fatal(err)
		}
	} else {
		evaluatedOffset = *offset
	}
	fmt.Println("Evaluated Offset")
	fmt.Println(evaluatedOffset)

	fmt.Printf("Consuming from stream %s and forwarding to exchange %s\n", *streamName, *exchange)
	err = ch.Qos(1, 0, false)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Starting consumption from offset %s\n", evaluatedOffset)
	randomInt := rand.Intn(99999)
	msgs, err := ch.Consume(
		*streamName,
		"stream-forwarder"+strconv.Itoa(randomInt),
		false,
		true,
		false,
		false,
		amqp.Table{"x-stream-offset": evaluatedOffset},
	)
	if err != nil {
		log.Fatal(err)
	}

	deliveryBuffer := make(chan amqp.Delivery, 100)
	publishChannels := []*amqp.Channel{}

	for i := 0; i < *publishChannelQty; i++ {
		publishCh, err := pubConn.Channel()
		if err != nil {
			log.Fatal(err)
		}
		defer publishCh.Close()
		publishChannels = append(publishChannels, publishCh)
	}

	// If we panic for any reason, we need to make sure we still commit offset
	defer func() {
		if r := recover(); r != nil {
			offsetManager.WriteOffset(trackedOffset)
			fmt.Println(err)
		}
	}()

	var msgsSinceCommit int
	go func() {
		for msg := range msgs {
			if *debug {
				printDelivery(msg)
			}
			deliveryBuffer <- msg
			trackedOffset, err = extractOffset(msg)
			if err != nil {
				fmt.Printf("Error extracting offset.. %s\n", err.Error())
			}
			msg.Ack(false)
		}
	}()

	for _, ch := range publishChannels {
		go func(ch *amqp.Channel) {
			fmt.Println("Starting publishing worker..")
			for {
				msg := <-deliveryBuffer
				err := forwardDelivery(ch, msg, *exchange, *debug)
				if err != nil {
					fmt.Printf("Error forwarding delivery: %s\n", err.Error())
				}
				msgsSinceCommit += 1
				if msgsSinceCommit >= 1000 {
					if *manageOffset {
						err := offsetManager.WriteOffset(trackedOffset)
						if err != nil {
							log.Fatal("Error writing offset: ", err)
						}
					}
					msgsSinceCommit = 0
					if *useStatsd {
						st.Inc(trackedOffset)
					}
				}
			}
		}(ch)
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
	if *manageOffset {
		fmt.Println("Writing offset to file...")
		err := offsetManager.WriteOffset(trackedOffset)
		if err != nil {
			log.Fatal(err)
		}
	}
	if *useStatsd {
		fmt.Println("Writing offset to statsd...")
		st.Inc(trackedOffset)
	}
}

func createAmqpConn(uri, caFile, clientCertFile, clientKeyFile string) (*amqp.Connection, error) {
	if caFile != "" {
		tlsConfig, err := generateRabbitTLSConfig(
			caFile,
			clientCertFile,
			clientKeyFile,
		)
		if err != nil {
			return nil, err
		}
		return amqp.DialTLS(uri, tlsConfig)
	} else {
		return amqp.Dial(uri)
	}
}

// TODO: Unit test for the translation of delivery to publishing
func forwardDelivery(ch *amqp.Channel, msg amqp.Delivery, exchange string, debug bool) error {
	if debug {
		fmt.Println("Forwarding delivery...", exchange, msg.RoutingKey)
	}

	err := ch.Publish(
		exchange,       // exchange
		msg.RoutingKey, // routing key
		false,          // mandatory
		false,          // immediate
		amqp.Publishing{
			Headers:     msg.Headers,
			ContentType: msg.ContentType,
			Body:        msg.Body,
		},
	)
	if err != nil {
		return err
	}
	return nil
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

func rabbitAmqpConnection(url string) (*amqp.Connection, error) {
	return amqp.Dial(url)
}
