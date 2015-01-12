// +build integration

package main

import (
	"encoding/json"
	"flag"
	"os"
	"testing"
	"time"

	bigquery "google.golang.org/api/bigquery/v2"
)

var (
	keyPath   = flag.String("key", "", "oauth2 json key path, acquired via https://console.developers.google.com")
	projectID = flag.String("project", "", "bigquery project id")
	datasetID = flag.String("dataset", "", "bigquery dataset id")
	tableID   = flag.String("table", "", "bigquery table id")
	jsonPath  = flag.String("json", "bigquery_streamer_integration.json", "json file path to be inserted into bigquery")
)

// TestInsertTableToBigQuery test stream inserting a row (given as argument)
// 5 times to BigQuery, and logs the response.
// NOTE this test doesn't check if the inserted rows were inserted correctly,
// it just inserts them.
//
// Usage: 'go test -v -tags=integration -key /path/to/key.json -project projectID -dataset datasetID -table tableID'
func TestInsertTableToBigQuery(t *testing.T) {
	flag.Parse()

	// Validate custom parameters.
	if *keyPath == "" {
		t.Fatal("missing key parameter")
	}
	if *projectID == "" {
		t.Fatal("missing project parameter")
	}
	if *datasetID == "" {
		t.Fatal("missing dataset parameter")
	}
	if *tableID == "" {
		t.Fatal("missing table parameter")
	}
	if *jsonPath == "" {
		t.Fatal("missing json parameter")
	}

	// Read JSON data.
	f, err := os.Open(*jsonPath)
	if err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(f)
	jsonData := map[string]interface{}{}
	err = decoder.Decode(&jsonData)
	if err != nil {
		t.Fatal("json decoding error:", err)
	}

	// Convert JSON data read from file to BigQuery JSON type.
	jsonPayload := map[string]bigquery.JsonValue{}
	for k, v := range jsonData {
		jsonPayload[k] = v
	}

	jwtConfig, err := NewJWTConfig(*keyPath)
	if err != nil {
		t.Fatal(err)
	}

	service, err := NewBigQueryService(jwtConfig)
	if err != nil {
		t.Fatal(err)
	}

	// Set flush threshold to 5 so flush will happen immediately.
	s, err := NewBigQueryStreamer(service, 5, 1*time.Minute)
	if err != nil {
		t.Fatal(err)
	}

	// Insert the same row several times.
	for i := 0; i < 5; i++ {
		s.QueueRow(*projectID, *datasetID, *tableID, jsonPayload)
	}

	// Override insertAll() function to call the original one and notify "flushed" via channel.
	flushed := make(chan bool)
	s.insertAll = func() {
		s.insertAllToBigQuery()
		flushed <- true
	}

	// Start the server and wait enough time for the server to flush.
	timer := time.NewTimer(5 * time.Second)
	go s.Start()
	select {
	case <-flushed:
	case <-timer.C:
		t.Error("insertAll() didn't happen fast enough")
	}
	s.Stop()

	// Log BigQuery errors.
	// NOTE these are not test failures, just responses.
	readErrors := true
	for readErrors {
		select {
		case err, ok := <-s.Errors:
			if !ok {
				t.Fatal("Error channel is closed")
			}

			t.Log("BigQuery Error:", err)
		default:
			t.Log("No errors")
			readErrors = false
		}
	}
}
