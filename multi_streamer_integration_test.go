// +build integrationmulti

package bqstreamer

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

// TestMultiStreamerInsertTableToBigQuery test stream inserting a row (given as argument)
// 5 times to BigQuery using a multi-streamer, and logs the response.
// NOTE this test doesn't check if the inserted rows were inserted correctly,
// it just inserts them.
//
// Usage: 'go test -v -tags=integration-multi-streamer -key /path/to/key.json -project projectID -dataset datasetID -table tableID'
func TestMultiStreamerInsertTableToBigQuery(t *testing.T) {
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
	jsonData := []map[string]interface{}{}
	err = decoder.Decode(&jsonData)
	if err != nil {
		t.Fatal("json decoding error:", err)
	}

	jwtConfig, err := NewJWTConfig(*keyPath)
	if err != nil {
		t.Fatal(err)
	}

	// Set flush max delay threshold to 1 second so sub-streamers will flush
	// almost immediately.
	s, err := NewMultiStreamer(jwtConfig, 3, 5, 1*time.Second, 1*time.Second, 3)
	if err != nil {
		t.Fatal(err)
	}

	// Convert file JSON data to BigQuery JSON type, and queue them in streamer.
	for _, row := range jsonData {
		jsonPayload := map[string]bigquery.JsonValue{}
		for k, v := range row {
			jsonPayload[k] = v
		}
		s.QueueRow(*projectID, *datasetID, *tableID, jsonPayload)
	}

	// Start and wait a bit for sub-streamers to read inserted rows.
	// Then, stop the multi-streamer, forcing all sub-streamers to flush.
	go s.Start()
	time.Sleep(5 * time.Second)
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
