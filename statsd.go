package main

import (
	"fmt"

	"github.com/cactus/go-statsd-client/v5/statsd"
)

// TODO: RabbitMQ stream internals operate pretty interestingly and 1 msg != +1 offset to said stream.
// This makes the original idea of this statsd implementation more complicated, as messages forwarded will not equal
// messages ready, or total offset in the stream.  This needs to be understood more to understand how we can
// accomplish the original intent of this, which was to measure offset lag.

// statdTracker is a construct to push offset tracking information of the rabbit-stream-forwarder
// so users can keep track of lag/offset information.
type statsdTracker struct {
	client statsd.Statter

	streamName string
	offset     int64
}

// NewStatsdTracker returns a tracker to push offset data to statsd.
func NewStatsdTracker(client statsd.Statter, offset int64) statsdTracker {
	return statsdTracker{
		client: client,
		offset: offset,
	}
}

// Inc will increment the statsd value by the difference between the previous call and the new offset.
func (s *statsdTracker) Inc(newOffset int64) error {
	fmt.Println("Committing offset to statsd", newOffset-s.offset)
	err := s.client.Inc(
		"apps.rabbit-stream-forwarder.counters.offset"+s.streamName,
		newOffset-s.offset,
		1.0,
	)
	if err != nil {
		fmt.Println(err)
		return err
	}

	s.offset = newOffset
	return nil
}
