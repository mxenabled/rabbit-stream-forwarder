package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"sync"

	"github.com/streadway/amqp"
)

// Forwarder takes in connections and information to start
// the forwarding process of messages from a stream to a given
// exchange.
type Forwarder struct {
	ctx            context.Context
	cancelCtx      context.CancelFunc
	cfg            ForwarderConfig
	trackedOffset  int64
	deliveryBuffer chan amqp.Delivery
	running        bool
	wg             sync.WaitGroup
}

// ForwarderConfig provides a config type for the caller to utilize to assign
// values needed for the process.
type ForwarderConfig struct {
	Ctx           context.Context
	SubConn       *amqp.Connection
	PubConn       *amqp.Connection
	OffsetManager OffsetManager
	StreamName    string
	Exchange      string
	StatsdClient  *statsdTracker
	Debug         bool
}

// NewForwarder returns a new Forwarder.
func NewForwarder(cfg ForwarderConfig) *Forwarder {
	ctx, cancelCtx := context.WithCancel(cfg.Ctx)

	return &Forwarder{
		ctx:            ctx,
		cancelCtx:      cancelCtx,
		cfg:            cfg,
		trackedOffset:  int64(0),
		deliveryBuffer: make(chan amqp.Delivery, 100),
		running:        false,
	}
}

// Start begins the forwarding process.
func (f *Forwarder) Start(manualOffset interface{}) error {
	var evaluatedOffset interface{}

	ch, err := f.cfg.SubConn.Channel()
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
		evaluatedOffset, err = f.cfg.OffsetManager.GetOffset()
		switch err {
		case ErrFirstRunUserMustSpecifyOffset:
			evaluatedOffset = "first"
		case nil:
		default:
			log.Fatal(err)
		}
	}

	log.Println(fmt.Sprintf("Consuming from stream %s and forwarding to exchange %s at offset %d\n", f.cfg.StreamName, f.cfg.Exchange, evaluatedOffset))

	randomInt := rand.Intn(99999)
	msgs, err := ch.Consume(
		f.cfg.StreamName,
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

	publishCh, err := f.cfg.PubConn.Channel()
	if err != nil {
		log.Fatal(err)
	}
	defer publishCh.Close()

	// If we panic for any reason, we need to make sure we still commit offset
	defer func() {
		if r := recover(); r != nil {
			f.cfg.OffsetManager.WriteOffset(f.trackedOffset)
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
	f.wg.Add(1)
	err = f.cfg.OffsetManager.WriteOffset(f.trackedOffset)
	if err != nil {
		log.Fatal(err)
	}
	if f.cfg.StatsdClient != nil {
		f.cfg.StatsdClient.Gauge(f.trackedOffset)
	}
	f.wg.Done()

	return nil
}

func (f *Forwarder) receiveDeliveries(msgs <-chan amqp.Delivery) {
	f.wg.Add(1)
	defer f.wg.Done()
	for {
		select {
		case <-f.ctx.Done():
			log.Println("Context cancel received, stopping receiveDeliveries worker.")
			return
		case msg := <-msgs:
			if f.cfg.Debug {
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
	f.wg.Add(1)
	defer f.wg.Done()
	log.Println("Starting publishing worker..")
	for {
		select {
		case <-f.ctx.Done():
			log.Println("Context cancel received, stopping forwardDeliveries worker.")
			return
		case msg := <-f.deliveryBuffer:
			err := forwardDelivery(ch, msg, f.cfg.Exchange, f.cfg.Debug)
			if err != nil {
				log.Println("Error forwradding delivery:", err.Error())
			}
			msgsSinceCommit += 1
			if f.cfg.StatsdClient != nil {
				f.cfg.StatsdClient.Inc()
			}
			if msgsSinceCommit >= 1000 {
				err := f.cfg.OffsetManager.WriteOffset(f.trackedOffset)
				if err != nil {
					log.Fatal("Error writing offset: ", err)
				}
				if f.cfg.StatsdClient != nil {
					f.cfg.StatsdClient.Gauge(f.trackedOffset)
				}
				msgsSinceCommit = 0
			}
		}
	}
}

// Stop sends cancellation in context to stop forwarder routines.
func (f *Forwarder) Stop() {
	f.cancelCtx()
	f.wg.Wait()
}
