package main

import (
	"flag"
	"io/ioutil"
	"os"
	"testing"

	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
)

func Test_FileOffsetManager_GetOffset(t *testing.T) {
	flag.Parse()
	test_cases := []struct {
		description  string
		fileExists   bool
		fileValue    []byte
		invalidValue bool
		expectation  int64
	}{
		{
			description:  "File exists and has valid value",
			fileExists:   true,
			fileValue:    []byte("100"),
			invalidValue: false,
			expectation:  int64(100),
		},
		{
			description:  "File exists and has invalid value",
			fileExists:   true,
			fileValue:    []byte("invalid-value"),
			invalidValue: true,
			expectation:  int64(0),
		},
		{
			description:  "File does not exist",
			fileExists:   false,
			fileValue:    []byte(""),
			invalidValue: false,
			expectation:  int64(0),
		},
	}

	fileName := "/tmp/rabbit-stream-forwarder-test"
	for _, tc := range test_cases {
		if tc.fileExists {
			err := ioutil.WriteFile(fileName, tc.fileValue, 0644)
			assert.Nil(t, err)
		}

		fom, err := NewFileOffsetManager(fileName)
		assert.Nil(t, err)
		retrievedOffset, err := fom.GetOffset()
		if tc.invalidValue {
			assert.NotNil(t, err)
		} else {
			assert.Nil(t, err)
		}

		assert.Equal(t, retrievedOffset, tc.expectation, "offsets not equal")
		err = os.Remove(fileName)
		assert.Nil(t, err)
	}
}

func Test_RabbitOffsetManager_NewRabbitOffsetManager(t *testing.T) {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672")
	if err != nil {
		t.Error(err)
	}
	_, err = NewRabbitOffsetManager(
		conn,
		"test-consumer",
		"test-forwarder-stream",
	)
	if err != nil {
		t.Error(err)
	}
}

func Test_RabbitOffsetManager_GetOffset(t *testing.T) {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672")
	if err != nil {
		t.Error(err)
	}
	rom, err := NewRabbitOffsetManager(
		conn,
		"test-consumer",
		"test-forwarder-stream",
	)
	if err != nil {
		t.Error(err)
	}

	offset, err := rom.GetOffset()
	if err != nil {
		t.Error(err)
	}

	assert.Equal(t, int64(0), offset, "when offset stream does not previously exists it initializes a 0 offset message")
}

func Test_RabbitOffsetManager_WriteOffset(t *testing.T) {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672")
	if err != nil {
		t.Error(err)
	}
	rom, err := NewRabbitOffsetManager(
		conn,
		"test-consumer",
		"test-forwarder-stream",
	)
	if err != nil {
		t.Error(err)
	}

	expectedOffset := int64(100)

	err = rom.WriteOffset(expectedOffset)
	if err != nil {
		t.Error(err)
	}

	offset, err := rom.GetOffset()
	if err != nil {
		t.Error(err)
	}

	assert.Equal(t, int64(100), offset, "when offset stream does not previously exists it initializes a 0 offset message")
}
