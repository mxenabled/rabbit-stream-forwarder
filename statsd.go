package main

import (
	"fmt"
	"log"
	"strings"

	"github.com/cactus/go-statsd-client/v5/statsd"
)

// statdTracker is a construct to push offset tracking information of the rabbit-stream-forwarder
// so users can keep track of lag/offset information.
type statsdTracker struct {
	client     statsd.Statter
	streamName string
	incKey     string
	gaugeKey   string
}

// NewStatsdTracker returns a tracker to push offset data to statsd.
func NewStatsdTracker(client statsd.Statter, streamName string) *statsdTracker {
	// It's common to use dot notation for queue/stream names in RabbitMQ.
	// This makes it a bit odd with statsd keys, so instead swap the periods for hyphens.
	stream := strings.Replace(streamName, ".", "-", -1)
	return &statsdTracker{
		client:     client,
		streamName: streamName,
		incKey:     fmt.Sprintf("counters.%s.rate", stream),
		gaugeKey:   fmt.Sprintf("gauges.%s.offset", stream),
	}
}

// Close the statter connection.
func (s *statsdTracker) Close() {
	s.client.Close()
}

// Inc will increment to the incKey on the Statter
func (s *statsdTracker) Inc() error {
	err := s.client.Inc(
		s.incKey,
		1,
		1.0,
	)
	if err != nil {
		log.Println(err)
		return err
	}

	return nil
}

// Gauge will report the given offset to the gaugeKey on the Statter
func (s *statsdTracker) Gauge(offset int64) error {
	err := s.client.Gauge(
		s.gaugeKey,
		offset,
		1.0,
	)
	if err != nil {
		log.Println(err)
		return err
	}

	return nil
}
