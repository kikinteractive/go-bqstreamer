// +build integration

package bqstreamer

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/jwt"
	bigquery "google.golang.org/api/bigquery/v2"
)

var (
	keyPath   = flag.String("key", "", "oauth2 json key path, acquired via https://console.developers.google.com")
	projectID = flag.String("project", "", "bigquery project id")
	datasetID = flag.String("dataset", "", "bigquery dataset id")
	tableID   = flag.String("table", "", "bigquery table id")

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
	default:
		var err error
		if jwtConfig, err = NewJWTConfig(*keyPath); err != nil {
			fmt.Println(err)
			break SANITY
		}
		return
	}

	if crash {
		os.Exit(2)
	}
}

// TestBadRowsToBigQuery inserting the a table with malformed rows
// to BigQuery, and tests the response.
func TestInsertBadRowsToBigQuery(t *testing.T) {
	t.Parallel()

	assert := assert.New(t)
	require := require.New(t)

	inserted := make(chan struct{}, 1)
	client := http.Client{
		Transport: NewNotifyTransport(
			jwtConfig.Client(oauth2.NoContext).Transport,
			// Notify "flushed" via channel on calling InsertAll().
			func(transport http.RoundTripper, req *http.Request) (*http.Response, error) {
				res, err := transport.RoundTrip(req)
				// NOTE notifying "inserted" after the request has been made.
				inserted <- struct{}{}
				return res, err
			})}

	// Set max rows threshold to five so flush will happen immediately.
	w, err := NewSyncWorker(&client, SetSyncMaxRetries(3), SetSyncRetryInterval(1*time.Second))
	require.NoError(err)

	// Queue two good rows and two invalid rows.
	// Invalid rows should be rejected and trigger a retry insert.
	w.Enqueue(NewRow(*projectID, *datasetID, *tableID, map[string]bigquery.JsonValue{"a": 1, "b": "1", "c": "2006-01-02T15:01:01.000000Z"}))
	// Invalid row:
	w.Enqueue(NewRow(*projectID, *datasetID, *tableID, map[string]bigquery.JsonValue{"a": "wrong value type", "b": "2", "c": "2006-01-02T15:02:02.000000Z"}))
	// Invalid row:
	w.Enqueue(NewRow(*projectID, *datasetID, *tableID, map[string]bigquery.JsonValue{"a": 3, "b": "3", "c": "2006-01-02T15:03:03.000000Z", "d": "non-existent field name"}))
	w.Enqueue(NewRow(*projectID, *datasetID, *tableID, map[string]bigquery.JsonValue{"a": 4, "b": "4", "c": "2006-01-02T15:04:04.000000Z"}))

	// Execute an insert operation for enqueued rows:

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		select {
		case <-inserted:
		case <-time.After(3 * time.Second):
			assert.Fail("insert wasn't called fast enough")
		}
		select {
		case <-inserted:
			require.Fail("insert was called a second time")
		case <-time.After(3 * time.Second):
		}
	}()

	insertErrs := w.InsertWithRetry()

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		require.Fail("insert didn't occur as expected")
	}

	// Test insert response:

	tableErrs := insertErrs.All()
	require.Len(tableErrs, 1)
	for _, table := range tableErrs {
		attempts := table.Attempts()
		require.Len(attempts, 1)

		// Test exepcted rejections:

		assert.NoError(attempts[0].Error())
		assert.Len(attempts[0].rows, 4)

		expectedErrs := []bigquery.ErrorProto{
			bigquery.ErrorProto{
				Reason: "stopped",
			},
			bigquery.ErrorProto{
				DebugInfo: `generic::invalid_argument: Cannot convert value to integer (bad value).`,
				Location:  "a",
				Reason:    "invalid",
				Message:   "Cannot convert value to integer (bad value).",
			},
			bigquery.ErrorProto{
				DebugInfo: `generic::not_found: no such field.`,
				Location:  "d",
				Reason:    "invalid",
				Message:   "no such field.",
			},
			bigquery.ErrorProto{
				Reason: "stopped",
			},
		}

		for _, row := range attempts[0].rows {
			assert.Len(row.Errors, 1, fmt.Sprintf("%d", row.Index))
			assert.Equal(expectedErrs[row.Index], *row.Errors[0], fmt.Sprintf("%d", row.Index))
		}
	}
}

// TestGoodRowsToBigQuery inserting the a table with malformed rows
// to BigQuery, and tests the response.
func TestInsertGoodRowsToBigQuery(t *testing.T) {
	t.Parallel()

	assert := assert.New(t)
	require := require.New(t)

	inserted := make(chan struct{}, 1)
	client := http.Client{
		Transport: NewNotifyTransport(
			jwtConfig.Client(oauth2.NoContext).Transport,
			// Notify "flushed" via channel on calling InsertAll().
			func(transport http.RoundTripper, req *http.Request) (*http.Response, error) {
				res, err := transport.RoundTrip(req)
				// NOTE notifying "inserted" after the request has been made.
				inserted <- struct{}{}
				return res, err
			})}

	// Set max rows threshold to five so flush will happen immediately.
	w, err := NewSyncWorker(&client, SetSyncMaxRetries(3), SetSyncRetryInterval(1*time.Second))
	require.NoError(err)

	// Queue two good rows and two invalid rows.
	// Invalid rows should be rejected and trigger a retry insert.
	w.Enqueue(NewRow(*projectID, *datasetID, *tableID, map[string]bigquery.JsonValue{"a": 1, "b": "1", "c": "2006-01-02T15:01:01.000000Z"}))
	w.Enqueue(NewRow(*projectID, *datasetID, *tableID, map[string]bigquery.JsonValue{"a": 2, "b": "2", "c": "2006-01-02T15:02:02.000000Z"}))

	// Execute an insert operation for enqueued rows:

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		select {
		case <-inserted:
		case <-time.After(3 * time.Second):
			assert.Fail("insert wasn't called fast enough")
		}
		select {
		case <-inserted:
			require.Fail("insert was called a second time")
		case <-time.After(3 * time.Second):
		}
	}()

	insertErrs := w.InsertWithRetry()

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		require.Fail("insert didn't occur as expected")
	}

	// Test insert response:

	tableErrs := insertErrs.All()
	require.Len(tableErrs, 1)
	for _, table := range tableErrs {
		// Tets No expected errors:
		attempts := table.Attempts()
		require.Len(attempts, 1)
		assert.NoError(attempts[0].Error())
		assert.Len(attempts[0].rows, 0)
	}
}

// NotifyTransport is a mock http.Transport, and implements http.RoundTripper
// interface.
//
// It notifies via channel that the RoundTripper() function was called,
// then calls and returns the embedded Transport.RoundTripper().
type NotifyTransport struct {
	transport http.RoundTripper
	roundTrip func(http.RoundTripper, *http.Request) (*http.Response, error)
}

func NewNotifyTransport(
	transport http.RoundTripper,
	roundTrip func(http.RoundTripper, *http.Request) (*http.Response, error)) *NotifyTransport {

	return &NotifyTransport{
		transport: transport,
		roundTrip: roundTrip,
	}
}

func (t *NotifyTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return t.roundTrip(t.transport, req)
}
