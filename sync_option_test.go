package bqstreamer

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestSyncWorkerOptions creates a new SyncWorker and tests all options are working.
func TestSyncWorkerOptions(t *testing.T) {
	t.Parallel()

	assert := assert.New(t)

	// Test sending bad arguments.
	w := SyncWorker{}
	assert.EqualError(SetSyncMaxRetries(-1)(&w), "max retries value must be a non-negative int")
	assert.EqualError(SetSyncRetryInterval(0)(&w), "retry interval value must be a positive time.Duration")
	assert.EqualError(SetSyncRetryInterval(-1)(&w), "retry interval value must be a positive time.Duration")

	// Test valid arguments
	w = SyncWorker{}
	assert.NoError(SetSyncRetryInterval(2 * time.Second)(&w))
	assert.NoError(SetSyncMaxRetries(2)(&w))
	assert.NoError(SetSyncIgnoreUnknownValues(true)(&w))
	assert.NoError(SetSyncSkipInvalidRows(true)(&w))

	assert.Equal(2*time.Second, w.retryInterval)
	assert.Equal(2, w.maxRetries)
	assert.True(w.ignoreUnknownValues)
	assert.True(w.skipInvalidRows)
}
