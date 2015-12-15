package bqstreamer

import (
	"google.golang.org/api/bigquery/v2"
	"log"
	"time"
)

// This example uses a single Streamer.
// A single row is queued, and will be flushed once a time threshold has passed,
// or if the streamer is explicitly closed.
//
// Note starting a Streamer is a blocking operation, so it needs to run in its own goroutine.
//
// You should probably use MultiStreamer, as it provides better concurrency and speed,
// but Streamer is there if you need to.
func ExampleStreamer() {
	// Init OAuth2/JWT. This is required for authenticating with BigQuery.
	// See the following URLs for more info:
	// https://cloud.google.com/bigquery/authorization
	// https://developers.google.com/console/help/new/#generatingoauth2
	jwtConfig, err := NewJWTConfig("path_to_key.json")
	if err != nil {
		log.Fatalln(err)
	}

	// Init a BigQuery client.
	service, err := NewBigQueryService(jwtConfig)
	if err != nil {
		log.Fatalln(err)
	}

	// Set streamer configuration.
	maxRows := 500                      // Amount of rows queued before forcing insert to BigQuery.
	maxDelay := 1 * time.Second         // Time to pass between forcing insert to BigQuery.
	sleepBeforeRetry := 1 * time.Second // Time to wait between failed insert retries.
	maxRetryInsert := 10                // Maximum amount of failed insert retries before discarding rows and moving on.

	// Init a new streamer.
	s, err := NewStreamer(service, maxRows, maxDelay, sleepBeforeRetry, maxRetryInsert)
	if err != nil {
		log.Fatalln(err)
	}

	// Start multi-streamer and workers.
	// A Streamer (NOT a MultiStreamer) is blocking,
	// so it needs to be start in its own goroutine.
	go s.Start()
	defer s.Stop()

	// Queue a single row.
	// Insert will happen once maxDelay time has passed,
	// or maxRows rows have been queued.
	s.QueueRow(
		"project-id", "dataset-id", "table-id",
		map[string]bigquery.JsonValue{"key": "value"},
	)
}
