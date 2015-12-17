package bqstreamer

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2/jwt"
	bigquery "google.golang.org/api/bigquery/v2"
)

// returnNil is a function meant to override NewBigQueryService(),
// so it will return a nil BigQuery service.
//
// All multi-streamers used in tests use this function along with a nil
// *jwt.Config, which is also not needed.
//
// We need this because we don't want to contact BigQuery service and Google OAuth2 in unit tests,
// so multi-streamer passes a nil BigQuery service to each sub-streamer it creates.
var returnNil = func(t *jwt.Config) (*bigquery.Service, error) { return nil, nil }

// TestNewStreamer tests creating a new MultiStreamer.
func TestNewMultiStreamer(t *testing.T) {
	t.Parallel()

	assert := assert.New(t)
	require := require.New(t)

	// Test giving bad arguments.
	s, err := newMultiStreamer(returnNil, nil, 0, 10, 1*time.Second, 1*time.Second, 10)
	assert.Error(err)
	s, err = newMultiStreamer(returnNil, nil, -1, 10, 1*time.Second, 1*time.Second, 10)
	assert.Error(err)
	s, err = newMultiStreamer(returnNil, nil, 2, 0, 1*time.Second, 1*time.Second, 10)
	assert.Error(err)
	s, err = newMultiStreamer(returnNil, nil, 2, -1, 1*time.Second, 1*time.Second, 10)
	assert.Error(err)
	s, err = newMultiStreamer(returnNil, nil, 2, 10, 0, 1*time.Second, 10)
	assert.Error(err)
	s, err = newMultiStreamer(returnNil, nil, 2, 10, -1*time.Nanosecond, 1*time.Second, 10)
	assert.Error(err)
	s, err = newMultiStreamer(returnNil, nil, 2, 10, 1*time.Nanosecond, -1*time.Second, 10)
	assert.Error(err)

	// Test valid arguments.
	s, err = newMultiStreamer(returnNil, nil, 50, 10, 1*time.Second, 1*time.Second, 10)
	assert.NoError(err)
	assert.Empty(s.rowChannel)
	assert.Equal(50*10, cap(s.rowChannel))
	assert.NotNil(s.Errors)
	assert.Empty(s.Errors)

	for _, st := range s.streamers {
		require.NotNil(st)
		require.Equal(st.Errors, s.Errors)
		require.Equal(st.rowChannel, s.rowChannel)
		require.Len(st.rows, 10)
		require.Equal(1*time.Second, st.MaxDelay)
	}
}

// TestStartStreamer tests calling Start() starts all sub-streamers.
func TestStartMultiStreamer(t *testing.T) {
	t.Parallel()

	require := require.New(t)

	s, err := newMultiStreamer(returnNil, nil, 50, 10, 1*time.Second, 1*time.Second, 10)
	require.NoError(err)

	// Override sub-streamers.Start() function to just notify they had started.
	started := make(chan struct{}, 51)
	start := func() { started <- struct{}{} }

	for _, st := range s.streamers {
		st.Start = start
	}

	// Start multi-streamers and check all sub-streamers have been started.
	s.Start()
	for i := 0; i < 50; i++ {
		select {
		case <-started:
		case <-time.After(1 * time.Millisecond):
			require.Fail("Streamer %d wasn't started fast enough", i)
		}
	}

	select {
	default:
	case <-started:
		require.Fail("More sub-streamers have been started than necessary")
	}
}

// TestStopMultiStreamer tests calling Stop() stops all sub-streamers.
func TestStopMultiStreamer(t *testing.T) {
	t.Parallel()

	require := require.New(t)

	s, err := newMultiStreamer(returnNil, nil, 50, 10, 1*time.Second, 1*time.Second, 10)
	require.NoError(err)

	// Override sub-streamers.Stop() function to notify they had stopped.
	stopped := make(chan struct{}, 51)
	stop := func() { stopped <- struct{}{} }

	for _, st := range s.streamers {
		st.Stop = stop
	}

	// Start then stop sub-streamers, and test if they were mock "stopped".
	s.Start()
	time.Sleep(1 * time.Millisecond)
	s.Stop()
	time.Sleep(1 * time.Millisecond)

	for i := 0; i < 50; i++ {
		select {
		case <-stopped:
		case <-time.After(1 * time.Millisecond):
			require.Fail("Streamer %d wasn't stopped fast enough", i)
		}
	}

	select {
	default:
	case <-stopped:
		require.Fail("More sub-streamers have been stopped than necessary")
	}
}

// TestQueueRowMultiStreamer queues 4 rows to 2 tables, 2 datasets,
// and 2 projects (total 4 rows) and tests if they were queued by the sub-streamers.
func TestQueueRowMultiStreamer(t *testing.T) {
	t.Parallel()

	assert := assert.New(t)
	require := require.New(t)

	s, err := newMultiStreamer(returnNil, nil, 5, 10, 1*time.Second, 1*time.Second, 10)
	require.NoError(err)

	// Override sub-streamers.flush() function to not flush, since we're
	// only testing queueing rows in this test.
	flush := func() {}

	for _, st := range s.streamers {
		st.flush = flush
	}

	// Queue rows to 2 tables, 2 datasets, and 2 projects (total 4 rows).
	data := map[string]bigquery.JsonValue{"test_key": "test_value"}
	s.QueueRow("p1", "d1", "t1", data)
	s.QueueRow("p1", "d1", "t2", data)
	s.QueueRow("p1", "d2", "t1", data)
	s.QueueRow("p1", "d1", "t1", data)

	// Start multi-streamer and count queued rows.
	s.Start()
	s.Stop()

	c := 0
	for _, st := range s.streamers {
		if st.rowIndex > 0 {
			for j := 0; j < st.rowIndex; j++ {
				c++
			}
		}
	}

	assert.Equal(4, c, fmt.Sprintf("Queued rows not as expected - should've been 4 queued rows (was %d)", c))
}
