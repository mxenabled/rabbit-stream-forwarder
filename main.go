package main

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"strings"

	"github.com/cactus/go-statsd-client/v5/statsd"
	"github.com/streadway/amqp"
)

var (
	trackedOffset int64
)

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

func main() {
	var (
		amqpUri           = flag.String("amqp-uri", "amqp://guest:guest@localhost:5672", "uri to connect to")
		streamName        = flag.String("stream-name", "", "stream name to forward")
		exchange          = flag.String("exchange", "events", "name of exchange to forward messages to")
		offset            = flag.String("offset", "last", "offset to start from, valid values are last, first, exact offset (digits)")
		manageOffset      = flag.Bool("manage-offset", false, "if true, manually tracks offset and stores in file")
		offsetFilePath    = flag.String("offset-file-path", "/tmp/stream-forwarder-offset", "file used to manage offset with the manage-offset flag")
		publishChannelQty = flag.Int("publish-channel-qty", 1, "amount of channels to open and utilize for publishing") // maybe connection qty as well
		debug             = flag.Bool("debug", false, "if set to true will enable more verbose logging")

		tlsCaFile         = flag.String("tls-client-ca-file", "", "client ca file path")
		tlsClientCertFile = flag.String("tls-client-cert-file", "", "client certificate file path")
		tlsClientKeyFile  = flag.String("tls-client-key-file", "", "client key file path")

		useStatsd = flag.Bool("use-statsd", false, "set to true to push offset status to statsd")
		statsdUri = flag.String("statsd-uri", "127.0.0.1:8125", "uri to use for statsd client")
	)
	flag.Parse()
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

	var evaluatedOffset interface{}
	if *manageOffset {
		evaluatedOffset, err = getOffset(*offsetFilePath)
		if err != nil {
			log.Fatal(err)
		}
	} else {
		evaluatedOffset = *offset
	}

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
		map[string]interface{}{"x-stream-offset": evaluatedOffset},
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
			commitOffset(*offsetFilePath, trackedOffset)
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

			msgsSinceCommit += 1
			if msgsSinceCommit >= 1000 {
				if *manageOffset {
					commitOffset(*offsetFilePath, trackedOffset)
				}
				msgsSinceCommit = 0
				if *useStatsd {
					st.Inc(trackedOffset)
				}
			}
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
			}
		}(ch)
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
	if *manageOffset {
		fmt.Println("Writing offset to file...")
		commitOffset(*offsetFilePath, trackedOffset)
	}
	if *useStatsd {
		fmt.Println("Writing offset to statsd...")
		st.Inc(trackedOffset)
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

func getOffset(filePath string) (interface{}, error) {
	fmt.Println("Fetching offset file...")
	f, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return "", err
	}
	f.Close()

	content, err := ioutil.ReadFile(filePath)
	if err != nil {
		return "", err
	}

	stringContent := string(content)
	if stringContent == "" {
		return "last", nil
	} else {
		i, err := strconv.Atoi(stringContent)
		if err != nil {
			log.Fatalf("Invalid value found in offset file: %s\n")
		}
		return i, nil
	}

	return strings.TrimSuffix(string(content), "\n"), nil
}

func commitOffset(filePath string, offset int64) {
	fmt.Printf("Committing offset %d to offset file...\n", offset)
	f, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		log.Fatal(err)
	}
	_, err = fmt.Fprintf(f, "%d", offset)
	if err != nil {
		log.Fatal("Error writing offset:", err)
	}
	if err := f.Close(); err != nil {
		log.Fatal(err)
	}
	f.Close()
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

func rabbitAmqpConnectionTLS(uri, caCertFile, clientCertFile, clientKeyFile string) (*amqp.Connection, error) {
	tlsConfig, err := generateRabbitTLSConfig(caCertFile, clientCertFile, clientKeyFile)
	if err != nil {
		return nil, err
	}

	return amqp.DialTLS(uri, tlsConfig)
}

func generateRabbitTLSConfig(caCertFile, clientCertFile, clientKeyFile string) (*tls.Config, error) {
	caCertFileBytes, err := ioutil.ReadFile(caCertFile)
	if err != nil {
		return nil, err
	}
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(caCertFileBytes)

	customVerify := func(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {
		roots := x509.NewCertPool()
		for _, rawCert := range rawCerts {
			cert, _ := x509.ParseCertificate(rawCert)
			roots.AddCert(cert)
			opts := x509.VerifyOptions{
				Roots: roots,
			}
			_, err := cert.Verify(opts)
			if err != nil {
				return err
			}
		}
		return nil
	}

	return generateTLSConfig(caCertFile, clientCertFile, clientKeyFile, customVerify)
}

type customVerifyFunc func(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error

// GenerateTLSConfig is used for tls dialing in go applications that are version >=1.15
func generateTLSConfig(caCertFile string, clientCertFile string, clientKeyFile string, customVerifyFunc customVerifyFunc) (*tls.Config, error) {
	clientKeyPair, err := tls.LoadX509KeyPair(clientCertFile, clientKeyFile)
	if err != nil {
		return nil, err
	}

	caCertFileBytes, err := ioutil.ReadFile(caCertFile)
	if err != nil {
		return nil, err
	}
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(caCertFileBytes)

	tlsConfig := &tls.Config{
		Certificates:       []tls.Certificate{clientKeyPair},
		RootCAs:            roots,
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: true,
		// Legacy TLS Verification using the new VerifyConnection callback
		// important for go version 1.15+ as some certificates in environments
		// that cause the new standard lib verification to fail.
		// This isn't really needed if your SSL certificates don't have the Common Name issue.
		// For more information: https://github.com/golang/go/issues/39568
		VerifyConnection: func(cs tls.ConnectionState) error {
			commonName := cs.PeerCertificates[0].Subject.CommonName
			if commonName != cs.ServerName {
				return fmt.Errorf("invalid certificate name %q, expected %q", commonName, cs.ServerName)
			}
			opts := x509.VerifyOptions{
				Roots:         roots,
				Intermediates: x509.NewCertPool(),
			}
			for _, cert := range cs.PeerCertificates[1:] {
				opts.Intermediates.AddCert(cert)
			}
			_, err := cs.PeerCertificates[0].Verify(opts)
			return err
		},
	}
	if customVerifyFunc != nil {
		tlsConfig.VerifyPeerCertificate = customVerifyFunc
		tlsConfig.VerifyConnection = nil
	}
	tlsConfig.BuildNameToCertificate()

	return tlsConfig, nil
}
