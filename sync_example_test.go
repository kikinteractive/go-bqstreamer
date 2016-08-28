package bqstreamer

import (
	"log"
	"time"

	"golang.org/x/oauth2"
	"google.golang.org/api/bigquery/v2"
)

// This example initializes a single SyncWorker,
// enqueues a single row, and execute an insert operation which inserts this
// row into its associated table in BigQuery.
func ExampleSyncWorker() {
	// Init OAuth2/JWT. This is required for authenticating with BigQuery.
	// See the following URLs for more info:
	// https://cloud.google.com/bigquery/authorization
	// https://developers.google.com/console/help/new/#generatingoauth2
	jwtConfig, err := NewJWTConfig("path_to_key.json")
	if err != nil {
		log.Fatalln(err)
	}

	// Init a new SyncWorker.
	w, err := NewSyncWorker(
		jwtConfig.Client(oauth2.NoContext),  // http.Client authenticated via OAuth2.
		SetSyncRetryInterval(1*time.Second), // Time to wait between failed insert retries.
		SetSyncMaxRetries(10),               // Maximum amount of retries a failed insert is allowed to be retried.
		SetSyncIgnoreUnknownValues(true),    // Ignore unknown fields when inserting rows.
		SetSyncSkipInvalidRows(true),        // Skip bad rows when inserting.
	)

	if err != nil {
		log.Fatalln(err)
	}

	// Enqueue a single row.
	w.Enqueue(
		NewRow(
			"my-project",
			"my-dataset",
			"my-table",
			map[string]bigquery.JsonValue{"key": "value"},
		))

	// Alternatively, you can supply your own unique identifier to the row,
	// for de-duplication purposes.
	//
	// See the following article for more info:
	// https://cloud.google.com/bigquery/streaming-data-into-bigquery#dataconsistency
	//
	// w.Enqueue(
	// 	NewRowWithID(
	// 		"my-project",
	// 		"my-dataset",
	// 		"my-table",
	// 		"my-unique-row-id",
	// 		map[string]bigquery.JsonValue{"key": "value"},
	// 	))

	// Execute an insert operation.
	//
	// NOTE this function returns insert errors,
	// demonstrated in another example.
	w.Insert()

	// Alternatively, you can use InsertWithRetry,
	// which will retry failed inserts in case of BigQuery server errors.
	//
	// insertErrs := w.InsertWithRetry()
}
