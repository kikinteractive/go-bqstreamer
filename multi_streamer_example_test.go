package bqstreamer

import (
	"google.golang.org/api/bigquery/v2"
	"log"
	"time"
)

// This example uses a MultiStreamer.
// A single row is queued, and will be flushed once a time threshold has passed,
// or if the MultiStreamer is explicitly closed.
//
// Starting a MultiStreamer is a non-blocking operation (unlike Streamer),
// so there's no need to run it in its own goroutine.
//
// You should probably use it instead of Streamer, as it provides better concurrency and speed.
func ExampleMultiStreamer() {
	// Init OAuth2/JWT. This is required for authenticating with BigQuery.
	// https://cloud.google.com/bigquery/authorization
	// https://developers.google.com/console/help/new/#generatingoauth2
	jwtConfig, err := NewJWTConfig("path_to_key.json")
	if err != nil {
		log.Fatalln(err)
	}

	// Set MultiStreamer configuration.
	numStreamers := 10                  // Number of concurrent sub-streamers (workers) to use.
	maxRows := 500                      // Amount of rows queued before forcing insert to BigQuery.
	maxDelay := 1 * time.Second         // Time to pass between forcing insert to BigQuery.
	sleepBeforeRetry := 1 * time.Second // Time to wait between failed insert retries.
	maxRetryInsert := 10                // Maximum amount of failed insert retries before discarding rows and moving on.

	// Init a new multi-streamer.
	ms, err := NewMultiStreamer(
		jwtConfig, numStreamers, maxRows, maxDelay, sleepBeforeRetry, maxRetryInsert)

	// Start multi-streamer and workers.
	ms.Start()
	defer ms.Stop()

	// Worker errors are reported to MultiStreamer.Errors channel.
	// This inits a goroutine the reads from this channel and logs errors.
	//
	// It can be closed by sending "true" to the shutdownErrorChan channel.
	shutdownErrorChan := make(chan bool)
	go func() {
		var err error

		readErrors := true
		for readErrors {
			select {
			case <-shutdownErrorChan:
				readErrors = false
			case err = <-ms.Errors:
				log.Println(err)
			}
		}
	}()
	defer func() { shutdownErrorChan <- true }()

	// Queue a single row.
	// Insert will happen once maxDelay time has passed,
	// or maxRows rows have been queued.
	ms.QueueRow(
		"project-id", "dataset-id", "table-id",
		map[string]bigquery.JsonValue{"key": "value"},
	)
}
