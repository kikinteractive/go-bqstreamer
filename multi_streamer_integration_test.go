// +build integrationmulti

package bqstreamer

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2/jwt"
	bigquery "google.golang.org/api/bigquery/v2"
)

var (
	keyPath   = flag.String("key", "", "oauth2 json key path, acquired via https://console.developers.google.com")
	projectID = flag.String("project", "", "bigquery project id")
	datasetID = flag.String("dataset", "", "bigquery dataset id")
	tableID   = flag.String("table", "", "bigquery table id")
	jsonPath  = flag.String("json", "bigquery_streamer_integration.json", "json file path to be inserted into bigquery")

	jwtConfig *jwt.Config
)

func init() {
	flag.Parse()

	// Validate custom parameters.
	crash := true
SANITY:
	switch {
	case *keyPath == "":
		fmt.Println("missing key parameter")
	case *projectID == "":
		fmt.Println("missing project parameter")
	case *datasetID == "":
		fmt.Println("missing dataset parameter")
	case *tableID == "":
		fmt.Println("missing table parameter")
	case *jsonPath == "":
		fmt.Println("missing json parameter")
	default:
		var err error
		if jwtConfig, err = NewJWTConfig(*keyPath); err != nil {
			fmt.Println(err)
			break SANITY
		}
		return
	}

	if crash {
		flag.Usage()
		os.Exit(2)
	}
}

// TestMultiStreamerInsertTableToBigQuery test stream inserting a row (given as argument)
// 5 times to BigQuery using a multi-streamer, and logs the response.
// NOTE this test doesn't check if the inserted rows were inserted correctly,
// it just inserts them.
//
// Usage: 'go test -v -tags=integration-multi-streamer -key /path/to/key.json -project projectID -dataset datasetID -table tableID'
func TestMultiStreamerInsertTableToBigQuery(t *testing.T) {
	t.Parallel()

	assert := assert.New(t)
	require := require.New(t)

	// Read JSON data.
	f, err := os.Open(*jsonPath)
	require.NoError(err)
	defer func() { assert.NoError(f.Close()) }()
	decoder := json.NewDecoder(f)
	jsonData := []map[string]interface{}{}
	require.NoError(decoder.Decode(&jsonData))

	// Set flush max delay threshold to 1 second so sub-streamers will flush
	// almost immediately.
	s, err := NewMultiStreamer(jwtConfig, 3, 5, 1*time.Second, 1*time.Second, 3)
	require.NoError(err)

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
			assert.True(ok, "Error channel is closed")
			t.Log("BigQuery Error:", err)
		default:
			t.Log("No errors")
			readErrors = false
		}
	}
}
