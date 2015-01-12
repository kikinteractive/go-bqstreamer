package main

import (
	"testing"
	"time"

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

// TestNewBigQueryStreamer tests creating a new BigQueryMultiStreamer.
func TestNewBigQueryMultiStreamer(t *testing.T) {
	// Test giving bad arguments.
	s, err := newBigQueryMultiStreamer(returnNil, nil, 0, 10, 1*time.Second)
	if err == nil {
		t.Error("New multi-streamer with 0 sub-streamers should've failed")
	}

	s, err = newBigQueryMultiStreamer(returnNil, nil, -1, 10, 1*time.Second)
	if err == nil {
		t.Error("New multi-streamer with -1 sub-streamers should've failed")
	}

	s, err = newBigQueryMultiStreamer(returnNil, nil, 2, 0, 1*time.Second)
	if err == nil {
		t.Error("New multi-streamer with 0 max rows should've failed")
	}

	s, err = newBigQueryMultiStreamer(returnNil, nil, 2, -1, 1*time.Second)
	if err == nil {
		t.Error("New multi-streamer with -1 max rows should've failed")
	}

	s, err = newBigQueryMultiStreamer(returnNil, nil, 2, 10, 0)
	if err == nil {
		t.Error("New multi-streamer with 0 max delay should've failed")
	}

	s, err = newBigQueryMultiStreamer(returnNil, nil, 2, 10, -1*time.Nanosecond)
	if err == nil {
		t.Error("New multi-streamer with -1 nanosecond max delay should've failed")
	}

	// Test giving good arguments.
	s, err = newBigQueryMultiStreamer(returnNil, nil, 50, 10, 1*time.Second)
	if err != nil {
		t.Error(err)
	}

	if s.rowChannel == nil {
		t.Error("Row channel is nil")
	}
	if len(s.rowChannel) != 0 {
		t.Error("Row channel isn't empty")
	}
	if cap(s.rowChannel) != 10 {
		t.Errorf("cap(rowChannel) != 10 (is %d)", len(s.rowChannel))
	}

	if s.Errors == nil {
		t.Error("Error channel is nil")
	}
	if len(s.Errors) != 0 {
		t.Error("Error channel isn't empty")
	}

	for i, st := range s.streamers {
		if st == nil {
			t.Fatalf("Streamer %d is nil", i+1)
		}

		if st.Errors != s.Errors {
			t.Errorf("Streamer %d error channel wasn't set to multi-streamer's error channel", i)
		}

		if st.rowChannel != s.rowChannel {
			t.Errorf("Streamer %d row channel wasn't set to multi-streamer's row channel", i)
		}

		if len(st.rows) != 10 {
			t.Errorf("Streamer %d rows not set to value sent to multi-streamer", i)
		}

		if st.maxDelay != 1*time.Second {
			t.Errorf("Streamer %d max delay not set to value sent to multi-streamer", i)
		}
	}
}

// TestStartStreamer tests calling Start() starts all sub-streamers.
func TestStartMultiStreamer(t *testing.T) {
	s, err := newBigQueryMultiStreamer(returnNil, nil, 50, 10, 1*time.Second)
	if err != nil {
		t.Error(err)
	}

	// Override sub-streamers.Start() function to just notify they had started.
	started := make(chan bool, 51)
	start := func() {
		started <- true
	}

	for _, st := range s.streamers {
		st.Start = start
	}

	// Start multi-streamers and check all sub-streamers have been started.
	s.Start()
	timer := time.NewTimer(1 * time.Millisecond)
	for i := 0; i < 50; i++ {
		select {
		case <-started:
		case <-timer.C:
			t.Fatalf("Streamer %d wasn't started fast enough", i)
		}
	}

	select {
	default:
	case <-started:
		t.Error("More sub-streamers have been started than necessary")
	}
}

// TestStopMultiStreamer tests calling Stop() stops all sub-streamers.
func TestStopMultiStreamer(t *testing.T) {
	s, err := newBigQueryMultiStreamer(returnNil, nil, 50, 10, 1*time.Second)
	if err != nil {
		t.Error(err)
	}

	// Override sub-streamers.Stop() function to notify they had stopped.
	stopped := make(chan bool, 51)
	stop := func() {
		stopped <- true
	}

	for _, st := range s.streamers {
		st.Stop = stop
	}

	// Start then stop sub-streamers, and test if they were mock "stopped".
	s.Start()
	time.Sleep(1 * time.Millisecond)
	s.Stop()
	time.Sleep(1 * time.Millisecond)

	timer := time.NewTimer(1 * time.Millisecond)
	for i := 0; i < 50; i++ {
		select {
		case <-stopped:
		case <-timer.C:
			t.Fatalf("Streamer %d wasn't stopped fast enough", i)
		}
	}

	select {
	default:
	case <-stopped:
		t.Error("More sub-streamers have been stopped than necessary")
	}
}

// TestQueueRowMultiStreamer queues 4 rows to 2 tables, 2 datasets,
// and 2 projects (total 4 rows) and tests if they were queued by the sub-streamers.
func TestQueueRowMultiStreamer(t *testing.T) {
	s, err := newBigQueryMultiStreamer(returnNil, nil, 5, 10, 1*time.Second)
	if err != nil {
		t.Error(err)
	}

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

	if c != 4 {
		t.Errorf("Queued rows not as expected - should've been 4 queued rows (was %d)", c)
	}
}
