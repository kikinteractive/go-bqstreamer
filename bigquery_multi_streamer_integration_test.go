// +build integrationmulti

package main

import (
	"encoding/json"
	"flag"
	"os"
	"testing"
	"time"

	valid "github.com/asaskevich/govalidator"
	bigquery "google.golang.org/api/bigquery/v2"
)

var (
	email     = flag.String("email", "", "oauth2 email")
	pemPath   = flag.String("pem", "", "oauth2 pem key path")
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
// Usage: 'go test -v -tags=integration-multi-streamer -email example@developer.gserviceaccount.com -pem key.pem -project projectID -dataset datasetID -table tableID'
func TestMultiStreamerInsertTableToBigQuery(t *testing.T) {
	flag.Parse()

	// Validate custom parameters.
	if *email == "" {
		t.Fatal("missing email parameter")
	}
	if !valid.IsEmail(*email) {
		t.Fatal("email parameter must be a valid email string")
	}
	if *pemPath == "" {
		t.Fatal("missing pem parameter")
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

	token, err := NewJWTToken(*email, *pemPath)
	if err != nil {
		t.Fatal(err)
	}

	// Set flush max delay threshold to 1 second so sub-streamers will flush
	// almost immediately.
	s, err := NewBigQueryMultiStreamer(token, 3, 5, 1*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	// Insert the same row several times.
	for i := 0; i < 5; i++ {
		s.QueueRow(*projectID, *datasetID, *tableID, jsonPayload)
	}

	// Start and wait a bit for sub-streamers to read inserted rows.
	// Then, stop the multi-streamer, forcing all sub-streamers to flush.
	go s.Start()
	time.Sleep(1 * time.Second)
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
