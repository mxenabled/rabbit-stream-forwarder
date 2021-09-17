package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"

	"github.com/cactus/go-statsd-client/v5/statsd"
	"github.com/streadway/amqp"
)

var (
	amqpUri      = flag.String("amqp-uri", "amqp://guest:guest@localhost:5672", "uri to connect to")
	streamName   = flag.String("stream-name", "", "stream name to forward")
	consumerName = flag.String("consumer-name", "", "name to use on consuming functions in rabbit")
	exchange     = flag.String("exchange", "events", "name of exchange to forward messages to")

	overrideOffset = flag.Bool("override-offset", false, "must be set to true in order to use the offset flag to manually specify offset to begin from")
	offset         = flag.String("offset", "last", "offset to start from, valid values are last, first, exact offset (digits)")

	offsetManagerType = flag.String("offset-manager-type", "rabbit", "type of offset manager to use, valid values are file, rabbit, none")
	offsetFilePath    = flag.String("offset-file-path", "/tmp/stream-forwarder-offset", "file used to manage offset with the manage-offset flag")
	debug             = flag.Bool("debug", false, "if set to true will enable more verbose logging")

	tlsCaFile         = flag.String("tls-client-ca-file", "", "client ca file path")
	tlsClientCertFile = flag.String("tls-client-cert-file", "", "client certificate file path")
	tlsClientKeyFile  = flag.String("tls-client-key-file", "", "client key file path")

	useStatsd = flag.Bool("use-statsd", false, "set to true to push offset status to statsd")
	statsdUri = flag.String("statsd-uri", "127.0.0.1:8125", "uri to use for statsd client")
)

func main() {
	flag.Parse()
	var offsetManager OffsetManager

	if *streamName == "" {
		log.Fatal("Stream name must be specified! Shutting down..")
	}

	log.Println("Debug mode:", *debug)

	subConn, err := createAmqpConn(*amqpUri, *tlsCaFile, *tlsClientCertFile, *tlsClientKeyFile)
	if err != nil {
		log.Fatal(err)
	}
	defer subConn.Close()

	pubConn, err := createAmqpConn(*amqpUri, *tlsCaFile, *tlsClientCertFile, *tlsClientKeyFile)
	if err != nil {
		log.Fatal(err)
	}
	defer pubConn.Close()

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

	var st *statsdTracker
	if *useStatsd {
		if *statsdUri == "" {
			log.Fatal("User specified to use statsd but received empty statd uri!")
		}
		config := &statsd.ClientConfig{
			Address: *statsdUri,
			Prefix:  "stream-forwarder",
		}
		s, err := statsd.NewClientWithConfig(config)
		if err != nil {
			log.Fatal(err)
		}
		st = NewStatsdTracker(s, *streamName)
	}

	ctx := context.Background()

	forwarder, err := NewForwarder(
		ctx,
		subConn,
		pubConn,
		offsetManager,
		*streamName,
		*exchange,
		st,
		*debug,
	)
	if err != nil {
		log.Fatal(err)
	}
	defer forwarder.Stop()

	if *overrideOffset {
		o, err := deriveOffsetFromString(*offset)
		if err != nil {
			log.Fatal(fmt.Sprintf("Got an invalid offset value for the override, please see help for valid values: %s", *offset))
		}
		go forwarder.Start(o)
	} else {
		go forwarder.Start(nil)
	}
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
	log.Println("Cancel received and ending forwarder process...")
}

func deriveOffsetFromString(s string) (interface{}, error) {
	validWordArgs := []string{"last", "next", "first"}

	for _, v := range validWordArgs {
		if strings.Contains(v, s) {
			return s, nil
		}
	}

	i, err := strconv.Atoi(s)
	if err != nil {
		return nil, err
	}
	return i, nil
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
