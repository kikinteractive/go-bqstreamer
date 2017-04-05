package bqstreamer

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"strconv"
	"testing"
	"time"

	bigquery "google.golang.org/api/bigquery/v2"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAsyncWorkerGroupNew tests creating a new AsyncWorkerGroup.
func TestAsyncWorkerGroupNew(t *testing.T) {
	t.Parallel()

	assert := assert.New(t)
	require := require.New(t)

	// TestAsyncWorkerGroup giving bad arguments.
	var err error
	_, err = NewAsyncWorkerGroup(nil)
	assert.EqualError(err, "jwt.Config is nil")
	errChan := make(chan *InsertErrors)
	_, err = newAsyncWorkerGroup(
		nil,
		SetAsyncNumWorkers(0),
		SetAsyncMaxRows(10),
		SetAsyncMaxDelay(1),
		SetAsyncRetryInterval(1),
		SetAsyncMaxRetries(10),
		SetAsyncErrorChannel(errChan),
		SetAsyncIgnoreUnknownValues(true),
		SetAsyncSkipInvalidRows(true),
	)
	assert.EqualError(err, "number of workers must be a positive int")

	// TestAsyncWorkerGroup valid arguments.
	var m *AsyncWorkerGroup
	errChan = make(chan *InsertErrors)
	m, err = newAsyncWorkerGroup(
		func() *http.Client { return &http.Client{} },
		SetAsyncNumWorkers(50),
		SetAsyncMaxRows(10),
		SetAsyncMaxDelay(1*time.Second),
		SetAsyncRetryInterval(1*time.Second),
		SetAsyncMaxRetries(10),
		SetAsyncErrorChannel(errChan),
		SetAsyncIgnoreUnknownValues(true),
		SetAsyncSkipInvalidRows(true),
	)

	assert.NoError(err)
	assert.Empty(m.rowChan)
	assert.Equal(50*10, cap(m.rowChan))
	assert.Equal(errChan, m.errorChan)

	for _, w := range m.workers {
		require.NotNil(w)
		require.Equal(m.errorChan, w.errorChan)
		require.Equal(m.rowChan, w.rowChan)
		require.Equal(10, w.maxRows)
		require.Equal(1*time.Second, w.maxDelay)
		require.Equal(1*time.Second, w.worker.retryInterval)
		require.Equal(10, w.worker.maxRetries)
		require.True(w.worker.ignoreUnknownValues)
		require.True(w.worker.skipInvalidRows)
	}
}

// TestAsyncWorkerGroupClose tests calling Close() closes all workers.
func TestAsyncWorkerGroupClose(t *testing.T) {
	t.Parallel()

	require := require.New(t)

	// Using nopClient like in TestAsyncWorkerGroupNewSteramer().
	errChan := make(chan *InsertErrors)
	m, err := newAsyncWorkerGroup(func() *http.Client { return &http.Client{} }, SetAsyncNumWorkers(50), SetAsyncMaxRows(10), SetAsyncMaxDelay(1*time.Second), SetAsyncRetryInterval(1*time.Second), SetAsyncMaxRetries(10), SetAsyncErrorChannel(errChan))
	require.NoError(err)

	// Start AsyncWorkerGroup and then close it, and test if all workers were stopped.
	m.Start()
	m.Close()

	// TestAsyncWorkerGroup if close notification was sent to every worker.
	for i, w := range m.workers {
		select {
		case _, ok := <-w.done:
			// A closed stop channel indicates the stop notification was sent.
			require.False(ok, "worker done channel is open", strconv.Itoa(i))
		case <-time.After(1 * time.Millisecond):
			require.Fail("worker wasn't closed fast enough", strconv.Itoa(i))
		}
	}

	// TestAsyncWorkerGroup if all workers have closed.
	for i, w := range m.workers {
		select {
		case _, ok := <-w.closedChan:
			// A closed stopped channel indicates the worker has stopped.
			require.False(ok, "worker ClosedChan is open", strconv.Itoa(i))
		case <-time.After(1 * time.Millisecond):
			require.Fail("worker didn't close fast enough", strconv.Itoa(i))
		}
	}

	// Error channel shouldn't contain any additional messages
	// after AsyncWorkerGroup has been closed.
	require.Equal(0, len(m.errorChan))
}

// TestAsyncWorkerGroupQueueRowStreamer queues 4 rows to 2 tables, 2 datasets, and 2 projects
// (total 4 rows) and tests if they were queued any of the workers.
func TestAsyncWorkerGroupQueueRowStreamer(t *testing.T) {
	t.Parallel()

	assert := assert.New(t)
	require := require.New(t)

	// Track inserted rows.
	ps := projects{}

	// Create mock http.Clients to be used by workers.
	client := http.Client{
		Transport: newTransport(func(req *http.Request) (*http.Response, error) {
			pID, dID, tID := getInsertMetadata(req.URL.Path)
			var tableReq bigquery.TableDataInsertAllRequest
			b, _ := ioutil.ReadAll(req.Body)
			require.NoError(json.Unmarshal(b, &tableReq))

			// Add all table rows to a local projects-datasets-table map.
			// This mocks rows that were inserted to BigQuery,
			// which we will test against.
			for _, tr := range tableReq.Rows {
				assert.NotNil(tr)

				// Mock "insert row" to table: Create project, dataset and table
				// if uninitalized.
				initTableIfNotExists(ps, pID, dID, tID)
				ps[pID][dID][tID] = append(ps[pID][dID][tID], &bigquery.TableDataInsertAllRequestRows{tr.InsertId, tr.Json, []string{}, []string{}})
			}

			res := http.Response{
				Header:     make(http.Header),
				Request:    req,
				StatusCode: 200,
				// Empty JSON body, meaning "no errors".
				Body: ioutil.NopCloser(bytes.NewBufferString(`{}`))}

			return &res, nil
		})}

	m, err := newAsyncWorkerGroup(func() *http.Client { return &client }, SetAsyncNumWorkers(5), SetAsyncMaxRows(10), SetAsyncMaxDelay(1*time.Second), SetAsyncRetryInterval(1*time.Second), SetAsyncMaxRetries(10))
	require.NoError(err)

	// Queue rows to 2 tables, 2 datasets, and 2 projects (total 4 rows).
	m.Enqueue(NewRowWithID("p1", "d1", "t1", "id111", map[string]bigquery.JsonValue{"k111": "v111"}))
	m.Enqueue(NewRowWithID("p1", "d1", "t2", "id112", map[string]bigquery.JsonValue{"k112": "v112"}))
	m.Enqueue(NewRowWithID("p1", "d2", "t1", "id121", map[string]bigquery.JsonValue{"k121": "v121"}))
	m.Enqueue(NewRowWithID("p2", "d1", "t1", "id211", map[string]bigquery.JsonValue{"k211": "v211"}))

	// Start and close AsyncWorkerGroup in order to force insert.
	m.Start()
	time.Sleep(100 * time.Millisecond)
	m.Close()

	// TestAsyncWorkerGroup for inserted rows by comparing tracked Projects.
	assert.Equal(
		projects{
			"p1": project{
				"d1": dataset{
					"t1": table{
						&bigquery.TableDataInsertAllRequestRows{"id111", map[string]bigquery.JsonValue{"k111": "v111"}, []string{}, []string{}},
					},
					"t2": table{
						&bigquery.TableDataInsertAllRequestRows{"id112", map[string]bigquery.JsonValue{"k112": "v112"}, []string{}, []string{}},
					},
				},
				"d2": dataset{
					"t1": table{
						&bigquery.TableDataInsertAllRequestRows{"id121", map[string]bigquery.JsonValue{"k121": "v121"}, []string{}, []string{}},
					},
				},
			},
			"p2": project{
				"d1": dataset{
					"t1": table{
						&bigquery.TableDataInsertAllRequestRows{"id211", map[string]bigquery.JsonValue{"k211": "v211"}, []string{}, []string{}},
					},
				},
			},
		},
		ps)
}
