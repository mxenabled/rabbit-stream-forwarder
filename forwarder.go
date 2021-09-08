package main

import (
	"fmt"
	"log"
	"math/rand"
	"strconv"

	"github.com/streadway/amqp"
)

// Forwarder takes in connections and information to start
// the forwarding process of messages from a stream to a given
// exchange.
type Forwarder struct {
	subConn       *amqp.Connection
	pubConn       *amqp.Connection
	offsetManager OffsetManager

	streamName string
	exchange   string

	statsdClient *statsdTracker
	debug        bool

	deliveryBuffer chan amqp.Delivery

	done chan bool
}

// NewForwarder returns a new Forwarder.
func NewForwarder(subConn *amqp.Connection, pubConn *amqp.Connection, offsetManager OffsetManager, streamName string, exchange string, statsdClient *statsdTracker, debug bool) (*Forwarder, error) {
	return &Forwarder{
		subConn:        subConn,
		pubConn:        pubConn,
		offsetManager:  offsetManager,
		streamName:     streamName,
		exchange:       exchange,
		statsdClient:   statsdClient,
		debug:          debug,
		deliveryBuffer: make(chan amqp.Delivery, 100),
		done:           make(chan bool),
	}, nil
}

// Start begins the forwarding process.
func (f *Forwarder) Start(manualOffset interface{}) error {
	var trackedOffset int64
	var evaluatedOffset interface{}

	ch, err := f.subConn.Channel()
	if err != nil {
		log.Fatal(err)
	}
	defer ch.Close()

	err = ch.Qos(1, 0, false)
	if err != nil {
		return err
	}

	if manualOffset != nil {
		evaluatedOffset = manualOffset
	} else {
		evaluatedOffset, err = f.offsetManager.GetOffset()
		switch err {
		case ErrFirstRunUserMustSpecifyOffset:
			evaluatedOffset = "first"
		case nil:
		default:
			log.Fatal(err)
		}
	}

	fmt.Printf("Consuming from stream %s and forwarding to exchange %s at offset %d\n", f.streamName, f.exchange, evaluatedOffset)

	randomInt := rand.Intn(99999)
	msgs, err := ch.Consume(
		f.streamName,
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

	publishCh, err := f.pubConn.Channel()
	if err != nil {
		log.Fatal(err)
	}
	defer publishCh.Close()

	// If we panic for any reason, we need to make sure we still commit offset
	defer func() {
		if r := recover(); r != nil {
			f.offsetManager.WriteOffset(trackedOffset)
			fmt.Println(err)
		}
	}()

	go func() {
		for msg := range msgs {
			if f.debug {
				printDelivery(msg)
			}
			f.deliveryBuffer <- msg
			trackedOffset, err = extractOffset(msg)
			if err != nil {
				fmt.Printf("Error extracting offset.. %s\n", err.Error())
			}
			msg.Ack(false)
		}
	}()

	var msgsSinceCommit int
	go func(ch *amqp.Channel) {
		fmt.Println("Starting publishing worker..")
		for {
			msg := <-f.deliveryBuffer
			err := forwardDelivery(publishCh, msg, f.exchange, f.debug)
			if err != nil {
				fmt.Printf("Error forwarding delivery: %s\n", err.Error())
			}
			msgsSinceCommit += 1
			if f.statsdClient != nil {
				f.statsdClient.Inc(1)
			}
			if msgsSinceCommit >= 1000 {
				err := f.offsetManager.WriteOffset(trackedOffset)
				if err != nil {
					log.Fatal("Error writing offset: ", err)
				}
				msgsSinceCommit = 0
			}
		}
	}(ch)

	<-f.done
	err = f.offsetManager.WriteOffset(trackedOffset)
	if err != nil {
		log.Fatal(err)
	}
	if f.statsdClient != nil {
		f.statsdClient.Inc(trackedOffset)
	}

	return nil
}

// Stop is provided to stop fowarder process if run through go routines.
func (f *Forwarder) Stop() {
	f.done <- true
}
