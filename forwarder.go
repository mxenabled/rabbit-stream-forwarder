package main

import (
	"context"
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
	ctx            context.Context
	cancelCtx      context.CancelFunc
	subConn        *amqp.Connection
	pubConn        *amqp.Connection
	offsetManager  OffsetManager
	streamName     string
	exchange       string
	statsdClient   *statsdTracker
	debug          bool
	trackedOffset  int64
	deliveryBuffer chan amqp.Delivery
	running        bool
}

// NewForwarder returns a new Forwarder.
func NewForwarder(ctx context.Context, subConn *amqp.Connection, pubConn *amqp.Connection, offsetManager OffsetManager, streamName string, exchange string, statsdClient *statsdTracker, debug bool) (*Forwarder, error) {
	ctx, cancelCtx := context.WithCancel(ctx)
	return &Forwarder{
		ctx:            ctx,
		cancelCtx:      cancelCtx,
		subConn:        subConn,
		pubConn:        pubConn,
		offsetManager:  offsetManager,
		streamName:     streamName,
		exchange:       exchange,
		statsdClient:   statsdClient,
		debug:          debug,
		trackedOffset:  int64(0),
		deliveryBuffer: make(chan amqp.Delivery, 100),
		running:        false,
	}, nil
}

// Start begins the forwarding process.
func (f *Forwarder) Start(manualOffset interface{}) error {
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

	log.Println(fmt.Sprintf("Consuming from stream %s and forwarding to exchange %s at offset %d\n", f.streamName, f.exchange, evaluatedOffset))

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
			f.offsetManager.WriteOffset(f.trackedOffset)
			log.Println(err)
		}
	}()

	go func() {
		f.receiveDeliveries(msgs)
	}()

	go func() {
		f.forwardDeliveries(publishCh)
	}()

	f.running = true
	<-f.ctx.Done()
	err = f.offsetManager.WriteOffset(f.trackedOffset)
	if err != nil {
		log.Fatal(err)
	}
	if f.statsdClient != nil {
		f.statsdClient.Inc(f.trackedOffset)
	}

	return nil
}

func (f *Forwarder) receiveDeliveries(msgs <-chan amqp.Delivery) {
	for {
		select {
		case <-f.ctx.Done():
			log.Println("Context cancel received, stopping receiveDeliveries worker.")
			return
		case msg := <-msgs:
			if f.debug {
				printDelivery(msg)
			}
			f.deliveryBuffer <- msg
			trackedOffset, err := extractOffset(msg) // Maybe this should happen in the publisher?
			f.trackedOffset = trackedOffset
			if err != nil {
				log.Println("Error extracting offset: ", err.Error())
			}
			msg.Ack(false)
		}
	}
}

func (f *Forwarder) forwardDeliveries(ch *amqp.Channel) {
	var msgsSinceCommit int
	log.Println("Starting publishing worker..")
	for {
		select {
		case <-f.ctx.Done():
			log.Println("Context cancel received, stopping forwardDeliveries worker.")
			return
		case msg := <-f.deliveryBuffer:
			err := forwardDelivery(ch, msg, f.exchange, f.debug)
			if err != nil {
				log.Println("Error forwradding delivery:", err.Error())
			}
			msgsSinceCommit += 1
			if f.statsdClient != nil {
				f.statsdClient.Inc(1)
			}
			if msgsSinceCommit >= 1000 {
				err := f.offsetManager.WriteOffset(f.trackedOffset)
				if err != nil {
					log.Fatal("Error writing offset: ", err)
				}
				msgsSinceCommit = 0
			}
		}
	}
}

// Stop sends cancellation in context to stop forwarder routines.
func (f *Forwarder) Stop() {
	f.cancelCtx()
}
