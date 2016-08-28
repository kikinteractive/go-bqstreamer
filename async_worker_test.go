package bqstreamer

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"strconv"
	"sync"
	"testing"
	"time"

	bigquery "google.golang.org/api/bigquery/v2"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newAsyncWorker is a helper function for unit tests,
// that initializes a new Worker using given configuration.
func newAsyncWorker(sw *SyncWorker, rows int, delay time.Duration) *asyncWorker {
	return &asyncWorker{
		worker: sw,

		// Make channel buffers big enough so they won't block tests unexpectedly.
		rowChan:   make(chan Row, 100),
		errorChan: make(chan *InsertErrors, 20),

		maxRows:  rows,
		maxDelay: delay,

		done:       make(chan struct{}),
		closedChan: make(chan struct{}),
	}
}

// TestAsyncWorkerClose calls Stop() before starting a worker,
// then starts a Worker and checks if it immediately stopped.
func TestAsyncWorkerClose(t *testing.T) {
	t.Parallel()

	assert := assert.New(t)
	require := require.New(t)

	// Mock an http.Client that always return an "OK" empty response
	// with no
	c := http.Client{
		Transport: newTransport(func(req *http.Request) (*http.Response, error) {
			res := http.Response{
				Header:     make(http.Header),
				Request:    req,
				StatusCode: 200,
				// Empty JSON body, meaning "no errors".
				Body: ioutil.NopCloser(bytes.NewBufferString(`{}`))}

			return &res, nil
		})}

	// Set insert delay to be large enough
	// so we could test if calling Stop() was the the trigger for insert.
	sw, err := NewSyncWorker(&c, SetSyncMaxRetries(10), SetSyncRetryInterval(1*time.Second))
	require.NoError(err)
	w := newAsyncWorker(sw, 100, 1*time.Minute)

	done := w.Close()
	select {
	case <-w.done:
	case <-time.After(1 * time.Second):
		assert.Fail("worker didn't stop fast enough")
	}
	select {
	case <-done:
		assert.Fail("Start() closed ClosedChan when Start() wasn't supposed to be running at all")
	case <-time.After(1 * time.Second):
	}
}

// TestAsyncWorkerEnqueue tests enqueueing a row.
func TestAsyncWorkerEnqueue(t *testing.T) {
	t.Parallel()

	require := require.New(t)

	// Mock an http.Client which tests that only a single insert request was
	// executed, and with the expected rows.
	inserted := make(chan struct{})
	calledNum := 0
	c := http.Client{
		Transport: newTransport(func(req *http.Request) (*http.Response, error) {
			var tableReq bigquery.TableDataInsertAllRequest
			b, _ := ioutil.ReadAll(req.Body)
			require.NoError(json.Unmarshal(b, &tableReq))

			var res http.Response
			switch calledNum {
			case 0:
				res = http.Response{
					Header:     make(http.Header),
					Request:    req,
					StatusCode: 200,
					// Empty JSON body, meaning "no errors".
					Body: ioutil.NopCloser(bytes.NewBufferString(`{}`))}
			default:
				require.Fail("Insert was called more than once")
			}

			// Notify that this table was mock "inserted".
			inserted <- struct{}{}

			calledNum++

			return &res, nil
		})}

	// Set max rows and delay to be big enough
	// so they will not interfere with this test.
	sw, err := NewSyncWorker(&c, SetSyncMaxRetries(5), SetSyncRetryInterval(1*time.Second))
	require.NoError(err)
	w := newAsyncWorker(sw, 100, 1*time.Minute)
	w.rowChan <- NewRowWithID("p", "d", "t", "id0", map[string]bigquery.JsonValue{"k0": "v0"})

	// Start worker and test the row was fetched from the row channel and
	// enqueued in the internal Worker row slice.
	w.Start()
	time.Sleep(100 * time.Millisecond)
	require.Len(w.worker.row, 1)
	require.Equal(NewRowWithID("p", "d", "t", "id0", map[string]bigquery.JsonValue{"k0": "v0"}), w.worker.row[0])

	// Enqueue four additional rows, and test they were "mock" inserted.
	w.rowChan <- NewRowWithID("p", "d", "t", "id1", map[string]bigquery.JsonValue{"k1": "v1"})
	w.rowChan <- NewRowWithID("p", "d", "t", "id2", map[string]bigquery.JsonValue{"k2": "v2"})
	w.rowChan <- NewRowWithID("p", "d", "t", "id3", map[string]bigquery.JsonValue{"k3": "v3"})
	w.rowChan <- NewRowWithID("p", "d", "t", "id4", map[string]bigquery.JsonValue{"k4": "v4"})
	time.Sleep(100 * time.Millisecond)
	require.Len(w.worker.row, 5)
}

// TestAsyncWorkerMaxDelay tests Worker executes an insert to BigQuery
// when max delay timer expires.
func TestAsyncWorkerMaxDelay(t *testing.T) {
	t.Parallel()

	assert := assert.New(t)
	require := require.New(t)

	// Mock an http.Client which tests that only a single insert request was
	// executed, and with the expected rows.
	inserted := make(chan struct{})
	calledNum := 0
	ps := projects{}
	c := http.Client{
		Transport: newTransport(func(req *http.Request) (*http.Response, error) {
			pID, dID, tID := getInsertMetadata(req.URL.Path)
			var tableReq bigquery.TableDataInsertAllRequest
			b, _ := ioutil.ReadAll(req.Body)
			require.NoError(json.Unmarshal(b, &tableReq))

			var res http.Response
			switch calledNum {
			case 0:
				for _, tr := range tableReq.Rows {
					assert.NotNil(tr)

					// Mock "insert row" to table: Create project, dataset and table
					// if uninitalized.
					initTableIfNotExists(ps, pID, dID, tID)
					ps[pID][dID][tID] = append(ps[pID][dID][tID], &bigquery.TableDataInsertAllRequestRows{tr.InsertId, tr.Json, []string{}})
				}

				res = http.Response{
					Header:     make(http.Header),
					Request:    req,
					StatusCode: 200,
					// Empty JSON body, meaning "no errors".
					Body: ioutil.NopCloser(bytes.NewBufferString(`{}`))}
			default:
				require.Fail("Insert was called more than once")
			}

			// Notify that this table was mock "inserted".
			inserted <- struct{}{}

			calledNum++

			return &res, nil
		})}

	sw, err := NewSyncWorker(&c, SetSyncMaxRetries(10), SetSyncRetryInterval(1*time.Second))
	require.NoError(err)
	w := newAsyncWorker(sw, 10, 1*time.Second)

	// Enqueue lines with delay = 1s and max rows = 10
	// so insert should be triggered by delay timer expiration.
	w.rowChan <- NewRowWithID("p", "d", "t", "id0", map[string]bigquery.JsonValue{"k0": "v0"})
	w.rowChan <- NewRowWithID("p", "d", "t", "id1", map[string]bigquery.JsonValue{"k1": "v1"})
	w.rowChan <- NewRowWithID("p", "d", "t", "id2", map[string]bigquery.JsonValue{"k2": "v2"})
	w.rowChan <- NewRowWithID("p", "d", "t", "id3", map[string]bigquery.JsonValue{"k3": "v3"})
	w.rowChan <- NewRowWithID("p", "d", "t", "id4", map[string]bigquery.JsonValue{"k4": "v4"})

	// Start Worker and measure time it should take to insert by max delay.
	// Add a small interval to timer to avoid failing when our timer expired
	// just a moment before Worker's timer.
	w.Start()

	// Fail if no insert occured in "max delay" time.
	select {
	case <-inserted:
	case <-time.After(w.maxDelay + 1*time.Second):
		require.Fail("insert didn't occur as expected")
	}

	select {
	case <-w.Close():
	case <-time.After(1 * time.Second):
		assert.Fail("Close() didn't work as expected")
	}

	// Test inserted rows.
	assert.Equal(
		projects{
			"p": project{
				"d": dataset{
					"t": table{
						&bigquery.TableDataInsertAllRequestRows{"id0", map[string]bigquery.JsonValue{"k0": "v0"}, []string{}},
						&bigquery.TableDataInsertAllRequestRows{"id1", map[string]bigquery.JsonValue{"k1": "v1"}, []string{}},
						&bigquery.TableDataInsertAllRequestRows{"id2", map[string]bigquery.JsonValue{"k2": "v2"}, []string{}},
						&bigquery.TableDataInsertAllRequestRows{"id3", map[string]bigquery.JsonValue{"k3": "v3"}, []string{}},
						&bigquery.TableDataInsertAllRequestRows{"id4", map[string]bigquery.JsonValue{"k4": "v4"}, []string{}},
					}}}},
		ps)
}

// TestAsyncWorkerMaxRows tests the Worker executes an insert to BigQuery
// whem max rows are enqueued for a specific table.
func TestAsyncWorkerMaxRows(t *testing.T) {
	t.Parallel()

	assert := assert.New(t)
	require := require.New(t)

	// Mock an http.Client which tests that only a single insert request was
	// executed, and with the expected rows.
	inserted := make(chan struct{})
	calledNum := 0
	ps := projects{}
	c := http.Client{
		Transport: newTransport(func(req *http.Request) (*http.Response, error) {
			pID, dID, tID := getInsertMetadata(req.URL.Path)
			var tableReq bigquery.TableDataInsertAllRequest
			b, _ := ioutil.ReadAll(req.Body)
			require.NoError(json.Unmarshal(b, &tableReq))

			var res http.Response
			switch calledNum {
			case 0:
				for _, tr := range tableReq.Rows {
					assert.NotNil(tr)

					// Mock "insert row" to table: Create project, dataset and table
					// if uninitalized.
					initTableIfNotExists(ps, pID, dID, tID)
					ps[pID][dID][tID] = append(ps[pID][dID][tID], &bigquery.TableDataInsertAllRequestRows{tr.InsertId, tr.Json, []string{}})
				}

				res = http.Response{
					Header:     make(http.Header),
					Request:    req,
					StatusCode: 200,
					// Empty JSON body, meaning "no errors".
					Body: ioutil.NopCloser(bytes.NewBufferString(`{}`))}
			default:
				require.Fail("Insert was called more than once")
			}

			// Notify that this table was mock "inserted".
			inserted <- struct{}{}

			calledNum++

			return &res, nil
		})}

	sw, err := NewSyncWorker(&c, SetSyncMaxRetries(10), SetSyncRetryInterval(1*time.Second))
	require.NoError(err)
	w := newAsyncWorker(sw, 5, 1*time.Minute)
	require.NoError(err)

	// Enqueue lines with delay = 1m and max rows = 5
	// so insert should be triggered by max rows being enqueued.
	w.rowChan <- NewRowWithID("p", "d", "t", "id0", map[string]bigquery.JsonValue{"k0": "v0"})
	w.rowChan <- NewRowWithID("p", "d", "t", "id1", map[string]bigquery.JsonValue{"k1": "v1"})
	w.rowChan <- NewRowWithID("p", "d", "t", "id2", map[string]bigquery.JsonValue{"k2": "v2"})
	w.rowChan <- NewRowWithID("p", "d", "t", "id3", map[string]bigquery.JsonValue{"k3": "v3"})
	w.rowChan <- NewRowWithID("p", "d", "t", "id4", map[string]bigquery.JsonValue{"k4": "v4"})

	// Start Worker and measure time it should take to insert by max rows.
	w.Start()

	// Test if insert happened almost immediately, forced by rows queue being filled.
	select {
	case <-inserted:
	case <-time.After(1 * time.Second):
		assert.Fail("insert didn't occur as expected")
	}

	select {
	case <-w.Close():
	case <-time.After(1 * time.Second):
		assert.Fail("Close() didn't work as expected")
	}

	// Test inserted rows.
	assert.Equal(
		projects{
			"p": project{
				"d": dataset{
					"t": table{
						&bigquery.TableDataInsertAllRequestRows{"id0", map[string]bigquery.JsonValue{"k0": "v0"}, []string{}},
						&bigquery.TableDataInsertAllRequestRows{"id1", map[string]bigquery.JsonValue{"k1": "v1"}, []string{}},
						&bigquery.TableDataInsertAllRequestRows{"id2", map[string]bigquery.JsonValue{"k2": "v2"}, []string{}},
						&bigquery.TableDataInsertAllRequestRows{"id3", map[string]bigquery.JsonValue{"k3": "v3"}, []string{}},
						&bigquery.TableDataInsertAllRequestRows{"id4", map[string]bigquery.JsonValue{"k4": "v4"}, []string{}},
					}}}},
		ps)
}

// TestAsyncWorkerInsertErrors inserts 5 rows and mocks 3 response errors for 3 of these rows,
// then tests whether these errors were forwarded to error channel.
func TestAsyncWorkerInsertErrors(t *testing.T) {
	t.Parallel()

	assert := assert.New(t)
	require := require.New(t)

	// Mock response to return mocked errors, and notify table was mock "flushed".
	// Will be used for testing errors were sent to error channel.
	inserted := make(chan struct{})
	calledNum := 0
	client := http.Client{
		Transport: newTransport(func(req *http.Request) (*http.Response, error) {
			var tableReq bigquery.TableDataInsertAllRequest
			b, _ := ioutil.ReadAll(req.Body)
			require.NoError(json.Unmarshal(b, &tableReq))

			// Default response.
			res := http.Response{
				Header:     make(http.Header),
				Request:    req,
				StatusCode: 200,
				// Empty JSON body, meaning "no errors".
				Body: ioutil.NopCloser(bytes.NewBufferString(`{}`))}

			var r bigquery.TableDataInsertAllResponse
			switch calledNum {
			case 0:
				// Return response errors for line 0,2,4.
				r = bigquery.TableDataInsertAllResponse{
					Kind: "bigquery",
					InsertErrors: []*bigquery.TableDataInsertAllResponseInsertErrors{
						&bigquery.TableDataInsertAllResponseInsertErrors{
							Index: 0,
							Errors: []*bigquery.ErrorProto{
								&bigquery.ErrorProto{Location: "l01", Message: "m00", Reason: "r01"},
								&bigquery.ErrorProto{Location: "l02", Message: "m01", Reason: "r02"},
							}},
						&bigquery.TableDataInsertAllResponseInsertErrors{
							Index:  2,
							Errors: []*bigquery.ErrorProto{&bigquery.ErrorProto{Location: "l20", Message: "m20", Reason: "r20"}}},
						&bigquery.TableDataInsertAllResponseInsertErrors{
							Index: 4,
							Errors: []*bigquery.ErrorProto{
								&bigquery.ErrorProto{Location: "l50", Message: "m50", Reason: "r50"},
							}}}}
				if b, err := json.Marshal(&r); assert.NoError(err) {
					res.Body = ioutil.NopCloser(bytes.NewBuffer(b))
				}
			default:
				// r = bigquery.TableDataInsertAllResponse{}
				// if b, err := json.Marshal(&r); assert.NoError(err) {
				// 	res.Body = ioutil.NopCloser(bytes.NewBuffer(b))
				// }
			}

			calledNum++

			// Notify that this table was mock "flushed".
			inserted <- struct{}{}

			return &res, nil
		})}

	// Set a row threshold to 5 so it will flush immediately on calling Start().
	sw, err := NewSyncWorker(&client, SetSyncRetryInterval(1*time.Second), SetSyncMaxRetries(10))
	require.NoError(err)
	w := newAsyncWorker(sw, 5, 1*time.Minute)

	// Queue 5 rows to the same table.
	w.rowChan <- NewRowWithID("p", "d", "t", "id0", map[string]bigquery.JsonValue{"k0": "v0"})
	w.rowChan <- NewRowWithID("p", "d", "t", "id1", map[string]bigquery.JsonValue{"k1": "v1"})
	w.rowChan <- NewRowWithID("p", "d", "t", "id2", map[string]bigquery.JsonValue{"k2": "v2"})
	w.rowChan <- NewRowWithID("p", "d", "t", "id3", map[string]bigquery.JsonValue{"k3": "v3"})
	w.rowChan <- NewRowWithID("p", "d", "t", "id4", map[string]bigquery.JsonValue{"k4": "v4"})

	// Start Worker and wait for insert to happen.
	w.Start()

	select {
	case <-inserted:
	case <-time.After(1 * time.Second):
		assert.Fail("insert didn't occur as expected")
	}

	// First loop is for initial insert (with error response).
	// Second loop is for no errors (successul), so no retry insert would happen.
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			select {
			case <-inserted:
			case <-time.After(3 * time.Second):
				require.Fail("insert wasn't called fast enough", strconv.Itoa(i))
			}
		}(i)
	}

	select {
	case <-w.Close():
	case <-time.After(1 * time.Second):
		assert.Fail("Close() didn't work as expected")
	}

	// Test an insert error was returned.
	require.Len(w.errorChan, 1)
	insertErrs := <-w.errorChan

	// Fetch table and table insert attempts.
	tables := insertErrs.All()
	assert.Len(tables, 1)
	attempts := tables[0].Attempts()
	require.Len(attempts, 1)

	// Fetch rows. No need to deep test them deeply
	// since this is test in sync package.
	assert.Len(attempts[0].All(), 3)

	// Test no more inserts happened.
	assert.Len(inserted, 0)

	// ErrorChan should not receive any more errors in the meanwhile
	assert.Empty(w.errorChan)
}
