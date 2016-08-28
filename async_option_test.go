package bqstreamer

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestAsyncWorkerGroupOptions creates a new Streamer and tests all options are working.
func TestAsyncWorkerGroupOptions(t *testing.T) {
	t.Parallel()

	assert := assert.New(t)

	// Test sending bad arguments.
	m := AsyncWorkerGroup{}
	assert.EqualError(SetAsyncNumWorkers(0)(&m), "number of workers must be a positive int")
	assert.EqualError(SetAsyncNumWorkers(-1)(&m), "number of workers must be a positive int")
	assert.EqualError(SetAsyncMaxRows(0)(&m), "max rows must be non-negative int")
	assert.EqualError(SetAsyncMaxRows(-1)(&m), "max rows must be non-negative int")
	assert.EqualError(SetAsyncMaxDelay(0)(&m), "max delay must be a positive time.Duration")
	assert.EqualError(SetAsyncMaxDelay(-1)(&m), "max delay must be a positive time.Duration")
	assert.EqualError(SetAsyncRetryInterval(0)(&m), "sleep before retry must be a positive time.Duration")
	assert.EqualError(SetAsyncRetryInterval(-1)(&m), "sleep before retry must be a positive time.Duration")
	assert.EqualError(SetAsyncMaxRetries(-1)(&m), "max retry insert must be a non-negative int")
	assert.EqualError(SetAsyncErrorChannel(nil)(&m), "error channel is nil")

	// Test valid arguments
	m = AsyncWorkerGroup{}
	assert.NoError(SetAsyncNumWorkers(5)(&m))
	assert.NoError(SetAsyncMaxRows(1)(&m))
	assert.NoError(SetAsyncMaxDelay(1 * time.Second)(&m))
	assert.NoError(SetAsyncRetryInterval(2 * time.Second)(&m))
	assert.NoError(SetAsyncMaxRetries(2)(&m))
	c := make(chan *InsertErrors)
	assert.NoError(SetAsyncErrorChannel(c)(&m))
	assert.NoError(SetAsyncIgnoreUnknownValues(true)(&m))
	assert.NoError(SetAsyncSkipInvalidRows(true)(&m))

	assert.Equal(5, m.numWorkers)
	assert.Equal(1, m.maxRows)
	assert.Empty(m.rowChan)
	assert.Equal(1*time.Second, m.maxDelay)
	assert.Equal(2*time.Second, m.retryInterval)
	assert.Equal(2, m.maxRetries)
	assert.Equal(c, m.errorChan)
	assert.True(m.ignoreUnknownValues)
	assert.True(m.skipInvalidRows)
}
