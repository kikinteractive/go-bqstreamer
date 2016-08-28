package bqstreamer

import (
	"log"
	"time"

	bigquery "google.golang.org/api/bigquery/v2"
)

// This example initializes an AsyncWorkerGroup,
// sets up an error handling goroutine,
// and enqueues a single row.
//
// An insert operation to BigQuery will be executed once either
// a maximum delay time has passed, maximum rows have been enqueued,
// or the AsyncWorkerGroup has been closed.
func ExampleAsyncWorkerGroup() {
	// Init a new AsyncWorkerGroup:

	// Initialize an error channel,
	// into which all AsyncWorkers will report their errors.
	//
	// NOTE this channel must be read from, otherwise the workers will block and hang.
	errChan := make(chan *InsertErrors)

	// Define a function for processing insert results.
	// This function only logs insert errors.
	done := make(chan struct{})
	defer close(done)

	// Error handling goroutine,
	// which just fetches errors and throws them away.
	go func() {
		for range errChan {
			select {
			case <-done:
				// Read all remaining errors (if any are left)
				// and return.
				for range errChan {
				}
				return
			case <-errChan:
			}
		}
	}()

	jwtConfig, err := NewJWTConfig("path_to_key.json")
	if err != nil {
		log.Fatalln(err)
	}

	// Initialize a worker group.
	g, err := NewAsyncWorkerGroup(
		jwtConfig,
		SetAsyncNumWorkers(10),               // Number of background workers in the group.
		SetAsyncMaxRows(500),                 // Amount of rows that must be enqueued before executing an insert operation to BigQuery.
		SetAsyncMaxDelay(1*time.Second),      // Time to wait between inserts.
		SetAsyncRetryInterval(1*time.Second), // Time to wait between failed insert retries.
		SetAsyncMaxRetries(10),               // Maximum amount of retries a failed insert is allowed to be retried.
		SetAsyncIgnoreUnknownValues(true),    // Ignore unknown fields when inserting rows.
		SetAsyncSkipInvalidRows(true),        // Skip bad rows when inserting.
		SetAsyncErrorChannel(errChan),        // Set unified error channel.
	)

	if err != nil {
		log.Fatalln(err)
	}

	// Start AsyncWorkerGroup.
	// Start() starts the background workers and returns immediately.
	g.Start()

	// Close() blocks until all workers have inserted any remaining rows to
	// BigQuery and closed.
	defer g.Close()

	// Enqueue a single row.
	//
	// An insert operation will be executed once the time delay defined by
	// SetAsyncMaxDelay is reached,
	// or enough rows have been queued (not shown in this example).
	g.Enqueue(
		NewRow(
			"my-project",
			"my-dataset",
			"my-table",
			map[string]bigquery.JsonValue{"key": "value"},
		))
}
