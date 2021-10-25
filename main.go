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
	"github.com/spf13/viper"
	"github.com/streadway/amqp"
)

const version string = "0.0.1"

var displayVersion bool

var (
	AmqpUri           = "amqp_uri"
	StreamName        = "stream_name"
	ConsumerName      = "consumer_name"
	Exchange          = "exchange"
	OverrideOffset    = "override_offset"
	Offset            = "offset"
	OffsetManagerType = "offset_manager_type"
	OffsetFilePath    = "offset_file_path"
	Debug             = "debug"
	TlsCaFile         = "tls_ca_file"
	TlsClientCertFile = "tls_client_cert_file"
	TlsClientKeyFile  = "tls_client_key_file"
	UseStatsd         = "use_statsd"
	StatsdUri         = "statsd_uri"
)

func bindConfig() {
	viper.BindEnv(AmqpUri)
	viper.BindEnv(StreamName)
	viper.BindEnv(ConsumerName)
	viper.BindEnv(Exchange)
	viper.BindEnv(OverrideOffset)
	viper.BindEnv(Offset)
	viper.BindEnv(OffsetManagerType)
	viper.BindEnv(OffsetFilePath)
	viper.BindEnv(Debug)
	viper.BindEnv(TlsCaFile)
	viper.BindEnv(TlsClientCertFile)
	viper.BindEnv(TlsClientKeyFile)
	viper.BindEnv(UseStatsd)
	viper.BindEnv(StatsdUri)

	configDefaults()
}

func configDefaults() {
	viper.SetDefault(AmqpUri, "amqp://guest:guest@localhost:5672")
	viper.SetDefault(StreamName, "")
	viper.SetDefault(ConsumerName, "rabbit-stream-forwarder")
	viper.SetDefault(Exchange, "events")
	viper.SetDefault(OverrideOffset, false)
	viper.SetDefault(Offset, "last")
	viper.SetDefault(OffsetManagerType, "rabbit")
	viper.SetDefault(OffsetFilePath, "/tmp/rabbit-stream-forwarder-offset")
	viper.SetDefault(Debug, false)
	viper.SetDefault(TlsCaFile, "")
	viper.SetDefault(TlsClientCertFile, "")
	viper.SetDefault(TlsClientKeyFile, "")
	viper.SetDefault(UseStatsd, true)
	viper.SetDefault(StatsdUri, "127.0.0.1:8125")
}

func main() {
	flag.BoolVar(&displayVersion, "version", false, "show the version and exit")
	flag.Parse()

	if displayVersion {
		fmt.Println(version)
		return
	}

	bindConfig()
	var offsetManager OffsetManager

	if viper.GetString(StreamName) == "" {
		log.Fatal("Stream name must be specified! Shutting down..")
	}

	log.Println("Debug mode:", viper.GetBool(Debug))

	subConn, err := createAmqpConn(
		viper.GetString(AmqpUri),
		viper.GetString(TlsCaFile),
		viper.GetString(TlsClientCertFile),
		viper.GetString(TlsClientKeyFile),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer subConn.Close()

	pubConn, err := createAmqpConn(
		viper.GetString(AmqpUri),
		viper.GetString(TlsCaFile),
		viper.GetString(TlsClientCertFile),
		viper.GetString(TlsClientKeyFile),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer pubConn.Close()

	switch viper.GetString(OffsetManagerType) {
	case "rabbit":
		rom, err := NewRabbitOffsetManager(
			pubConn,
			viper.GetString(ConsumerName),
			viper.GetString(StreamName),
		)
		if err != nil {
			log.Fatal(err)
		}
		offsetManager = rom
	case "file":
		fom, err := NewFileOffsetManager(viper.GetString(OffsetFilePath))
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
	if viper.GetBool(UseStatsd) {
		if viper.GetString(StatsdUri) == "" {
			log.Fatal("User specified to use statsd but received empty statd uri!")
		}
		config := &statsd.ClientConfig{
			Address: viper.GetString(StatsdUri),
			Prefix:  "stream-forwarder",
		}
		s, err := statsd.NewClientWithConfig(config)
		if err != nil {
			log.Fatal(err)
		}
		defer s.Close()
		st = NewStatsdTracker(s, viper.GetString(StreamName))
	}

	ctx := context.Background()

	forwarder := NewForwarder(
		ForwarderConfig{
			Ctx:           ctx,
			SubConn:       subConn,
			PubConn:       pubConn,
			OffsetManager: offsetManager,
			StreamName:    viper.GetString(StreamName),
			Exchange:      viper.GetString(Exchange),
			StatsdClient:  st,
			Debug:         viper.GetBool(Debug),
		},
	)
	defer forwarder.Stop()

	if viper.GetBool(OverrideOffset) {
		o, err := deriveOffsetFromString(viper.GetString(Offset))
		if err != nil {
			log.Fatal(fmt.Sprintf("Got an invalid offset value for the override, please see help for valid values: %s", viper.GetString(Offset)))
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
