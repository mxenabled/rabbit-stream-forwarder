package main

import (
	"testing"

	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
)

func Test_extractOffset(t *testing.T) {
	test_cases := []struct {
		description string
		input       amqp.Delivery
		expectation int64
		shouldError bool
	}{
		{
			description: "Valid delivery extracts offset to int64",
			input: amqp.Delivery{
				Headers: amqp.Table{
					"x-stream-offset": int64(5),
				},
			},
			expectation: int64(5),
			shouldError: false,
		},
		{
			description: "Delivery does not contain stream offset returns error",
			input: amqp.Delivery{
				Headers: amqp.Table{},
			},
			expectation: int64(0),
			shouldError: true,
		},
	}

	for _, tc := range test_cases {
		result, err := extractOffset(tc.input)
		if tc.shouldError {
			assert.NotNil(t, err, tc.description)
		}
		assert.Equal(t, tc.expectation, result, tc.description)
	}
}

func Test_convertDeliveryToPublishing(t *testing.T) {
	test_cases := []struct {
		description string
		msg         amqp.Delivery
		expected    amqp.Publishing
	}{
		{
			description: "Information gets translated properly",
			msg: amqp.Delivery{
				Headers: amqp.Table{
					"x-stream-offset": int64(1),
				},
				ContentType: "text/plain",
				Body:        []byte("somebody"),
			},
			expected: amqp.Publishing{
				Headers: amqp.Table{
					"x-stream-offset": int64(1),
				},
				ContentType: "text/plain",
				Body:        []byte("somebody"),
			},
		},
	}

	for _, tc := range test_cases {
		result := convertDeliveryToPublishing(tc.msg)
		assert.Equal(t, tc.expected, result, tc.description)
	}
}
