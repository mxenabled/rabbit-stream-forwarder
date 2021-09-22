package main

import (
	"flag"
	"io/ioutil"
	"os"
	"testing"

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
	conn, err := testAmqpConnection()
	assert.Nil(t, err, "shall not error during amqp connection creation")

	rom, err := NewRabbitOffsetManager(
		conn,
		"test-consumer",
		"test-forwarder-stream-new-offset-manager-test",
	)
	assert.Nil(t, err, "shall not error during NewRabbitOffsetManager")

	err = rom.DeleteStream()
	assert.Nil(t, err, "shall not error during stream deletion")
}

func Test_RabbitOffsetManager_GetOffset(t *testing.T) {
	conn, err := testAmqpConnection()
	assert.Nil(t, err, "shall not error during amqp connection creation")

	rom, err := NewRabbitOffsetManager(
		conn,
		"test-consumer",
		"test-forwarder-stream-get-offset-test",
	)
	assert.Nil(t, err, "shall not error during NewRabbitOffsetManager")

	offset, err := rom.GetOffset()
	assert.Nil(t, err, "shall not error during RabbitOffsetManager.GetOffset()")

	assert.Equal(t, int64(0), offset, "when offset stream does not previously exists it initializes a 0 offset message")
	err = rom.DeleteStream()
	assert.Nil(t, err, "shall not error during stream deletion")
}

func Test_RabbitOffsetManager_WriteOffset(t *testing.T) {
	conn, err := testAmqpConnection()
	assert.Nil(t, err, "shall not error during amqp connection creation")

	rom, err := NewRabbitOffsetManager(
		conn,
		"test-consumer",
		"test-forwarder-stream-write-offset-test",
	)
	assert.Nil(t, err, "shall not error during NewRabbitOffsetManager")

	expectedOffset := int64(100)

	err = rom.WriteOffset(expectedOffset)
	assert.Nil(t, err, "shall not error during RabbitOffsetManager.WriteOffset()")

	offset, err := rom.GetOffset()
	assert.Nil(t, err, "shall not error during RabbitOffsetManager.GetOffset()")

	assert.Equal(t, expectedOffset, offset, "when offset manager has written an offset, get offset returns the correct offset")

	err = rom.DeleteStream()
	assert.Nil(t, err, "shall not error during stream deletion")
}
