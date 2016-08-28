package bqstreamer

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"reflect"
	"regexp"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	bigquery "google.golang.org/api/bigquery/v2"
	"google.golang.org/api/googleapi"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSyncWorkerNew tests creating a new Worker.
func TestSyncWorkerNewSyncWorker(t *testing.T) {
	t.Parallel()

	assert := assert.New(t)
	require := require.New(t)
	var err error

	// Test bad arguments.
	//
	// NOTE we're testing a bad argument for the second option, to make sure
	// the options are evaluated one after the other, and not just the first
	// one or anything of the sort.
	_, err = NewSyncWorker(&http.Client{}, SetSyncMaxRetries(3), SetSyncRetryInterval(0))
	assert.EqualError(err, "retry interval value must be a positive time.Duration")

	// Test valid arguments.
	w, err := NewSyncWorker(&http.Client{}, SetSyncMaxRetries(10), SetSyncRetryInterval(1*time.Second))
	require.NoError(err)
	assert.NotNil(w.row)
	assert.Empty(w.row, 0)
	assert.Equal(rowSize, cap(w.row))
	assert.Equal(10, w.maxRetries)
	assert.Equal(1*time.Second, w.retryInterval)
}

// TestSyncWorkerEnqueue tests queueing a row.
func TestSyncWorkerEnqueue(t *testing.T) {
	t.Parallel()

	assert := assert.New(t)
	require := require.New(t)

	w, err := NewSyncWorker(&http.Client{})
	require.NoError(err)

	w.Enqueue(NewRowWithID("p", "d", "t", "r1", map[string]bigquery.JsonValue{"k1": "v1"}))
	w.Enqueue(NewRowWithID("p", "d", "t", "r2", map[string]bigquery.JsonValue{"k2": "v2"}))
	w.Enqueue(NewRowWithID("p", "d", "t", "r3", map[string]bigquery.JsonValue{"k3": "v3"}))

	require.Len(w.row, 3)
	assert.Equal(
		w.row,
		[]Row{
			NewRowWithID("p", "d", "t", "r1", map[string]bigquery.JsonValue{"k1": "v1"}),
			NewRowWithID("p", "d", "t", "r2", map[string]bigquery.JsonValue{"k2": "v2"}),
			NewRowWithID("p", "d", "t", "r3", map[string]bigquery.JsonValue{"k3": "v3"}),
		})
}

// TestSyncWorkerInsertAll queues 20 rows to 4 tables, 5 to each row,
// and tests if rows were inserted to a mock project-dataset-table-rows type.
func TestSyncWorkerInsertAll(t *testing.T) {
	t.Parallel()

	assert := assert.New(t)
	require := require.New(t)

	// Cache all rows from all tables to a local variable.
	// Will be used for testing which rows were actually "inserted".
	inserted := make(chan struct{})
	var ps projects
	client := http.Client{
		Transport: newTransport(func(req *http.Request) (*http.Response, error) {
			// Get project, dataset, table IDs of current request.
			pID, dID, tID := getInsertMetadata(req.URL.Path)
			var tableReq bigquery.TableDataInsertAllRequest
			b, _ := ioutil.ReadAll(req.Body)
			require.NoError(json.Unmarshal(b, &tableReq))

			// Add all table rows to a local projects-datasets-tables map.
			// This mocks the rows that were inserted to BigQuery, and we will test against them.
			for _, tr := range tableReq.Rows {
				assert.NotNil(tr)

				// Mock "insert row" to table: Create project, dataset and table
				// if uninitalized.
				initTableIfNotExists(ps, pID, dID, tID)
				ps[pID][dID][tID] = append(ps[pID][dID][tID], &bigquery.TableDataInsertAllRequestRows{tr.InsertId, tr.Json, []string{}})
			}

			// Notify that this table was mock "inserted".
			inserted <- struct{}{}

			res := http.Response{
				Header:     make(http.Header),
				Request:    req,
				StatusCode: 200,
				// Empty JSON body, meaning "no errors".
				Body: ioutil.NopCloser(bytes.NewBufferString(`{}`))}

			return &res, nil
		})}

	// We intend to insert 20 rows in this test, so set maxRows = 20 and delay
	// to be long enough so flush will occur due to rows queue filling up.
	//
	// Test both "simple" insertTable, and insert with retry functions.
	w, err := NewSyncWorker(&client)
	require.NoError(err)
	for _, insertFunc := range []func() *InsertErrors{
		w.Insert,
		w.InsertWithRetry,
	} {
		ps = make(projects)
		funcName := runtime.FuncForPC(reflect.ValueOf(insertFunc).Pointer()).Name()

		// Distribute 5 rows to 4 tables in 2 datasets in 2 projects (total 20 rows).
		for i := 0; i < 5; i++ {
			k := fmt.Sprintf("k%d", i)
			v := fmt.Sprintf("v%d", i)
			w.Enqueue(NewRowWithID("p1", "d1", "t1", "p1d1t1"+k, map[string]bigquery.JsonValue{k: v}))
			w.Enqueue(NewRowWithID("p1", "d1", "t2", "p1d1t2"+k, map[string]bigquery.JsonValue{k: v}))
			w.Enqueue(NewRowWithID("p1", "d2", "t1", "p1d2t1"+k, map[string]bigquery.JsonValue{k: v}))
			w.Enqueue(NewRowWithID("p2", "d1", "t1", "p2d1t1"+k, map[string]bigquery.JsonValue{k: v}))
		}

		// Start Worker and wait for 4 flushes to happen, one for each table.
		// Fail if flushes take too long.

		var wg sync.WaitGroup
		for i := 0; i < 4; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()

				select {
				case <-inserted:
				case <-time.After(1 * time.Second):
					require.Fail("insert wasn't called fast enough", strconv.Itoa(i), funcName)
				}
			}(i)
		}

		insertErrs := insertFunc()

		done := make(chan struct{})
		go func() { wg.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(1 * time.Second):
			require.Fail("insert didn't occur as expected", funcName)
		}

		assert.Len(inserted, 0, funcName) // Test no more inserts happened.
		assert.Len(w.row, 0, funcName)    // Test rows were reset after insert

		// Test rows slice wasn't re-allocated with a larger size.
		// This make sure we're keeping the same row slice size throught the
		// Worker's life.
		assert.Equal(rowSize, cap(w.row), funcName)

		// Test no insert
		tables := insertErrs.All()
		require.Len(tables, 4, funcName)
		for _, table := range tables {
			for _, row := range table.Attempts() {
				assert.NoError(row.Error(), funcName)
				assert.Empty(row.All(), funcName)
			}
		}

		assert.Equal(
			projects{
				"p1": project{
					"d1": dataset{
						"t1": table{
							&bigquery.TableDataInsertAllRequestRows{"p1d1t1k0", map[string]bigquery.JsonValue{"k0": "v0"}, []string{}},
							&bigquery.TableDataInsertAllRequestRows{"p1d1t1k1", map[string]bigquery.JsonValue{"k1": "v1"}, []string{}},
							&bigquery.TableDataInsertAllRequestRows{"p1d1t1k2", map[string]bigquery.JsonValue{"k2": "v2"}, []string{}},
							&bigquery.TableDataInsertAllRequestRows{"p1d1t1k3", map[string]bigquery.JsonValue{"k3": "v3"}, []string{}},
							&bigquery.TableDataInsertAllRequestRows{"p1d1t1k4", map[string]bigquery.JsonValue{"k4": "v4"}, []string{}},
						},
						"t2": table{
							&bigquery.TableDataInsertAllRequestRows{"p1d1t2k0", map[string]bigquery.JsonValue{"k0": "v0"}, []string{}},
							&bigquery.TableDataInsertAllRequestRows{"p1d1t2k1", map[string]bigquery.JsonValue{"k1": "v1"}, []string{}},
							&bigquery.TableDataInsertAllRequestRows{"p1d1t2k2", map[string]bigquery.JsonValue{"k2": "v2"}, []string{}},
							&bigquery.TableDataInsertAllRequestRows{"p1d1t2k3", map[string]bigquery.JsonValue{"k3": "v3"}, []string{}},
							&bigquery.TableDataInsertAllRequestRows{"p1d1t2k4", map[string]bigquery.JsonValue{"k4": "v4"}, []string{}},
						}},
					"d2": dataset{
						"t1": table{
							&bigquery.TableDataInsertAllRequestRows{"p1d2t1k0", map[string]bigquery.JsonValue{"k0": "v0"}, []string{}},
							&bigquery.TableDataInsertAllRequestRows{"p1d2t1k1", map[string]bigquery.JsonValue{"k1": "v1"}, []string{}},
							&bigquery.TableDataInsertAllRequestRows{"p1d2t1k2", map[string]bigquery.JsonValue{"k2": "v2"}, []string{}},
							&bigquery.TableDataInsertAllRequestRows{"p1d2t1k3", map[string]bigquery.JsonValue{"k3": "v3"}, []string{}},
							&bigquery.TableDataInsertAllRequestRows{"p1d2t1k4", map[string]bigquery.JsonValue{"k4": "v4"}, []string{}},
						}}},
				"p2": project{
					"d1": dataset{
						"t1": table{
							&bigquery.TableDataInsertAllRequestRows{"p2d1t1k0", map[string]bigquery.JsonValue{"k0": "v0"}, []string{}},
							&bigquery.TableDataInsertAllRequestRows{"p2d1t1k1", map[string]bigquery.JsonValue{"k1": "v1"}, []string{}},
							&bigquery.TableDataInsertAllRequestRows{"p2d1t1k2", map[string]bigquery.JsonValue{"k2": "v2"}, []string{}},
							&bigquery.TableDataInsertAllRequestRows{"p2d1t1k3", map[string]bigquery.JsonValue{"k3": "v3"}, []string{}},
							&bigquery.TableDataInsertAllRequestRows{"p2d1t1k4", map[string]bigquery.JsonValue{"k4": "v4"}, []string{}},
						}}}},
			ps, funcName)
	}
}

// TestSyncSkipInvalidRows tests wether enabling SkipInvalidRows option in the
// worker causes insert requests to include the SkipInvalidRows flag = true.
func TestSyncSkipInvalidRows(t *testing.T) {
	t.Parallel()

	assert := assert.New(t)
	require := require.New(t)

	// Mock response to return mocked errors, and notify table was mock "inserted".
	inserted := make(chan struct{})
	ps := make(projects)
	client := http.Client{
		Transport: newTransport(func(req *http.Request) (*http.Response, error) {
			pID, dID, tID := getInsertMetadata(req.URL.Path)
			var tableReq bigquery.TableDataInsertAllRequest
			b, _ := ioutil.ReadAll(req.Body)
			require.NoError(json.Unmarshal(b, &tableReq))

			// Test the SkipInvalidRows flag is enabled.
			assert.True(tableReq.SkipInvalidRows)

			// Add all table rows to a local projects-datasets-tables map.
			// This mocks the rows that were inserted to BigQuery, and we will test against them.
			for _, tr := range tableReq.Rows {
				assert.NotNil(tr)

				// Mock "insert row" to table: Create project, dataset and table
				// if uninitalized.
				initTableIfNotExists(ps, pID, dID, tID)
				ps[pID][dID][tID] = append(ps[pID][dID][tID], &bigquery.TableDataInsertAllRequestRows{tr.InsertId, tr.Json, []string{}})
			}

			res := http.Response{
				Header:     make(http.Header),
				Request:    req,
				StatusCode: 200,
				// Empty JSON body, meaning "no errors".
				Body: ioutil.NopCloser(bytes.NewBufferString(`{}`))}

			// Notify that this table was mock "inserted".
			inserted <- struct{}{}

			return &res, nil
		})}

	// Make retry sleep delay as small as possible and != 0.
	w, err := NewSyncWorker(&client, SetSyncSkipInvalidRows(true))
	require.NoError(err)

	// Queue 5 rows to the same table.
	w.Enqueue(NewRowWithID("p", "d", "t", "id0", map[string]bigquery.JsonValue{"k0": "v0"}))
	w.Enqueue(NewRowWithID("p", "d", "t", "id1", map[string]bigquery.JsonValue{"k1": "v1"}))
	w.Enqueue(NewRowWithID("p", "d", "t", "id2", map[string]bigquery.JsonValue{"k2": "v2"}))
	w.Enqueue(NewRowWithID("p", "d", "t", "id3", map[string]bigquery.JsonValue{"k3": "v3"}))
	w.Enqueue(NewRowWithID("p", "d", "t", "id4", map[string]bigquery.JsonValue{"k4": "v4"}))

	// Test HTTP 503 server error response triggered a retry.
	//
	// First loop is for testing initial insert,
	// second is for testing retry insert happened.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		select {
		case <-inserted:
		case <-time.After(1 * time.Second):
			assert.Fail("insert didn't happen fast enough")
		}
	}()

	insertErrs := w.InsertWithRetry()

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(1 * time.Second):
		require.Fail("insert didn't occur as expected")
	}

	assert.Len(inserted, 0) // Test no more inserts happened.
	assert.Len(w.row, 0)    // Test rows were reset after insert.

	// Test rows slice wasn't re-allocated with a larger size.
	// This make sure we're keeping the same row slice size throught the
	// Worker's life.
	assert.Equal(rowSize, cap(w.row))

	// Test insert
	tables := insertErrs.All()
	require.Len(tables, 1)
	require.Len(tables[0].Attempts(), 1)
	assert.NoError(tables[0].Attempts()[0].Error())
	assert.Empty(tables[0].Attempts()[0].All())

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

// TestSyncIgnoreUnknownValues tests wether enabling IgnoreUnknownValues option in the
// worker causes insert requests to include the IgnoreUnknownValues flag = true.
func TestSyncIgnoreUnknownValues(t *testing.T) {
	t.Parallel()

	assert := assert.New(t)
	require := require.New(t)

	// Mock response to return mocked errors, and notify table was mock "inserted".
	inserted := make(chan struct{})
	ps := make(projects)
	client := http.Client{
		Transport: newTransport(func(req *http.Request) (*http.Response, error) {
			pID, dID, tID := getInsertMetadata(req.URL.Path)
			var tableReq bigquery.TableDataInsertAllRequest
			b, _ := ioutil.ReadAll(req.Body)
			require.NoError(json.Unmarshal(b, &tableReq))

			// Test the SkipInvalidRows flag is enabled.
			assert.True(tableReq.IgnoreUnknownValues)

			// Add all table rows to a local projects-datasets-tables map.
			// This mocks the rows that were inserted to BigQuery, and we will test against them.
			for _, tr := range tableReq.Rows {
				assert.NotNil(tr)

				// Mock "insert row" to table: Create project, dataset and table
				// if uninitalized.
				initTableIfNotExists(ps, pID, dID, tID)
				ps[pID][dID][tID] = append(ps[pID][dID][tID], &bigquery.TableDataInsertAllRequestRows{tr.InsertId, tr.Json, []string{}})
			}

			res := http.Response{
				Header:     make(http.Header),
				Request:    req,
				StatusCode: 200,
				// Empty JSON body, meaning "no errors".
				Body: ioutil.NopCloser(bytes.NewBufferString(`{}`))}

			// Notify that this table was mock "inserted".
			inserted <- struct{}{}

			return &res, nil
		})}

	// Make retry sleep delay as small as possible and != 0.
	w, err := NewSyncWorker(&client, SetSyncIgnoreUnknownValues(true))
	require.NoError(err)

	// Queue 5 rows to the same table.
	w.Enqueue(NewRowWithID("p", "d", "t", "id0", map[string]bigquery.JsonValue{"k0": "v0"}))
	w.Enqueue(NewRowWithID("p", "d", "t", "id1", map[string]bigquery.JsonValue{"k1": "v1"}))
	w.Enqueue(NewRowWithID("p", "d", "t", "id2", map[string]bigquery.JsonValue{"k2": "v2"}))
	w.Enqueue(NewRowWithID("p", "d", "t", "id3", map[string]bigquery.JsonValue{"k3": "v3"}))
	w.Enqueue(NewRowWithID("p", "d", "t", "id4", map[string]bigquery.JsonValue{"k4": "v4"}))

	// Test HTTP 503 server error response triggered a retry.
	//
	// First loop is for testing initial insert,
	// second is for testing retry insert happened.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		select {
		case <-inserted:
		case <-time.After(1 * time.Second):
			assert.Fail("insert didn't happen fast enough")
		}
	}()

	insertErrs := w.InsertWithRetry()

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(1 * time.Second):
		require.Fail("insert didn't occur as expected")
	}

	assert.Len(inserted, 0) // Test no more inserts happened.
	assert.Len(w.row, 0)    // Test rows were reset after insert.

	// Test rows slice wasn't re-allocated with a larger size.
	// This make sure we're keeping the same row slice size throught the
	// Worker's life.
	assert.Equal(rowSize, cap(w.row))

	// Test insert
	tables := insertErrs.All()
	require.Len(tables, 1)
	require.Len(tables[0].Attempts(), 1)
	assert.NoError(tables[0].Attempts()[0].Error())
	assert.Empty(tables[0].Attempts()[0].All())

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

// TestSyncWorkerShouldRetryInsert tests if the shouldRetryInsert function correctly
// returns true for Google API 500, 503
func TestSyncWorkerShouldRetryInsert(t *testing.T) {
	t.Parallel()

	assert := assert.New(t)
	require := require.New(t)

	w, err := NewSyncWorker(&http.Client{}, SetSyncMaxRetries(10), SetSyncRetryInterval(100*time.Millisecond))
	require.NoError(err)

	// Test whether a GoogleAPI HTTP 501, 503 errors returns true.
	// i.e. the function decides the request related to this error should be retried.
	for _, tt := range []struct {
		code  int
		retry bool
	}{
		{500, true},
		{503, true},
		{501, false},
		{400, false},
		{404, false},
	} {
		start := time.Now()
		retry := w.shouldRetryInsert(
			&googleapi.Error{
				Code:    tt.code,
				Message: "m",
				Body:    "b",
				Errors: []googleapi.ErrorItem{
					googleapi.ErrorItem{Reason: "r1", Message: "m1"},
					googleapi.ErrorItem{Reason: "r2", Message: "m2"},
				}})
		assert.Equal(tt.retry, retry, tt.code)
		// Test backoff time was according to SleepBeforeRetry setting + 10ms extra.
		if tt.retry {
			assert.WithinDuration(time.Now(), start, 110*time.Millisecond)
		}
	}

	// Test a non-GoogleAPI error, but a generic error instead.
	assert.False(w.shouldRetryInsert(errors.New("Non-GoogleAPI error")))
}

// TestSyncWorkerInsertAllWithServerErrorResponse tests if an insert failed with a server
// error (500, 503) triggers a retry insert.
func TestSyncWorkerInsertAllWithServerErrorResponse(t *testing.T) {
	t.Parallel()

	assert := assert.New(t)
	require := require.New(t)

	// Mock response to return mocked errors, and notify table was mock "inserted".
	// Also count the times the insert function was called to make sure it's retried exactly once.
	inserted := make(chan struct{})
	calledNum := 0
	ps := make(projects)
	client := http.Client{
		Transport: newTransport(func(req *http.Request) (*http.Response, error) {
			pID, dID, tID := getInsertMetadata(req.URL.Path)
			var tableReq bigquery.TableDataInsertAllRequest
			b, _ := ioutil.ReadAll(req.Body)
			require.NoError(json.Unmarshal(b, &tableReq))

			// Return 503 server error on first call, and test if it triggered
			// a retry insert with exactly same data on the second call.
			var err error
			// Default response.
			var res http.Response
			switch calledNum {
			case 0:
				err = &googleapi.Error{
					Code:    503,
					Message: "m", Body: "b",
					Errors: []googleapi.ErrorItem{
						googleapi.ErrorItem{Reason: "r1", Message: "m1"},
						googleapi.ErrorItem{Reason: "r2", Message: "m2"},
					}}

				// Response should be HTTP 503.
				if b, err := json.Marshal(err); assert.NoError(err) {
					res = http.Response{
						Header:     make(http.Header),
						Request:    req,
						StatusCode: 503,
						Body:       ioutil.NopCloser(bytes.NewBuffer(b))}
				}
			case 1:
				// This insert should keep the table as it is,
				// and trigger retry after server error.

				// Add all table rows to a local projects-datasets-tables map.
				// This mocks the rows that were inserted to BigQuery, and we will test against them.
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
				require.Fail("Insert was called more than 2 times")
			}

			// Notify that this table was mock "inserted".
			inserted <- struct{}{}

			calledNum++

			return &res, nil
		})}

	// Make retry sleep delay as small as possible and != 0.
	w, err := NewSyncWorker(&client, SetSyncMaxRetries(10), SetSyncRetryInterval(1*time.Nanosecond))
	require.NoError(err)

	// Queue 5 rows to the same table.
	w.Enqueue(NewRowWithID("p", "d", "t", "id0", map[string]bigquery.JsonValue{"k0": "v0"}))
	w.Enqueue(NewRowWithID("p", "d", "t", "id1", map[string]bigquery.JsonValue{"k1": "v1"}))
	w.Enqueue(NewRowWithID("p", "d", "t", "id2", map[string]bigquery.JsonValue{"k2": "v2"}))
	w.Enqueue(NewRowWithID("p", "d", "t", "id3", map[string]bigquery.JsonValue{"k3": "v3"}))
	w.Enqueue(NewRowWithID("p", "d", "t", "id4", map[string]bigquery.JsonValue{"k4": "v4"}))

	// Test HTTP 503 server error response triggered a retry.
	//
	// First loop is for testing initial insert,
	// second is for testing retry insert happened.
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			select {
			case <-inserted:
			case <-time.After(1 * time.Second):
				assert.Fail("insert didn't happen fast enough", strconv.Itoa(i))
			}
		}(i)
	}

	insertErrs := w.InsertWithRetry()

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(1 * time.Second):
		require.Fail("insert didn't occur as expected")
	}

	assert.Len(inserted, 0) // Test no more inserts happened.
	assert.Len(w.row, 0)    // Test rows were reset after insert.

	// Test rows slice wasn't re-allocated with a larger size.
	// This make sure we're keeping the same row slice size throught the
	// Worker's life.
	assert.Equal(rowSize, cap(w.row))

	// Test insert
	tables := insertErrs.All()
	require.Len(tables, 1)
	require.Len(tables[0].Attempts(), 2)
	assert.EqualError(
		tables[0].Attempts()[0].Error(),
		`googleapi: got HTTP response code 503 with body: {"code":503,"message":"m","Body":"b","Header":null,"Errors":[{"reason":"r1","message":"m1"},{"reason":"r2","message":"m2"}]}`)
	assert.NoError(tables[0].Attempts()[1].Error())
	assert.Empty(tables[0].Attempts()[0].All())
	assert.Empty(tables[0].Attempts()[1].All())

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

// TestSyncWorkerInsertAllWithNonServerErrorResponse tests if an insert failed with an error
// which is NOT a server error (503) does NOT trigger a retry insert.
func TestSyncWorkerInsertAllWithNonServerErrorResponse(t *testing.T) {
	t.Parallel()

	assert := assert.New(t)
	require := require.New(t)

	// Mock response to return mocked errors, and notify table was mock "inserted".
	// Also count the times the insert function was called to make sure it's retried exactly once.
	inserted := make(chan struct{})
	ps := make(projects)
	calledNum := 0
	client := http.Client{
		Transport: newTransport(func(req *http.Request) (*http.Response, error) {
			pID, dID, tID := getInsertMetadata(req.URL.Path)
			var tableReq bigquery.TableDataInsertAllRequest
			b, _ := ioutil.ReadAll(req.Body)
			require.NoError(json.Unmarshal(b, &tableReq))

			var err error

			// Default response.
			res := http.Response{
				Header:     make(http.Header),
				Request:    req,
				StatusCode: 200,
				// Empty JSON body, meaning "no errors".
				Body: ioutil.NopCloser(bytes.NewBufferString(`{}`))}

			// Return a 501 (not 500, 503) server error on first call,
			// and test if it did NOT trigger a retry insert.
			switch calledNum {
			case 0:
				err = &googleapi.Error{
					Code:    501,
					Message: "m", Body: "b",
					Errors: []googleapi.ErrorItem{
						googleapi.ErrorItem{Reason: "r1", Message: "m1"},
						googleapi.ErrorItem{Reason: "r2", Message: "m2"},
					}}

				if b, err := json.Marshal(err); assert.NoError(err) {
					// Response should be HTTP 503.

					res = http.Response{
						Header:     make(http.Header),
						Request:    req,
						StatusCode: 501,
						Body:       ioutil.NopCloser(bytes.NewBuffer(b))}
				}

				for _, tr := range tableReq.Rows {
					assert.NotNil(tr)

					// Mock "insert row" to table: Create project, dataset and table
					// if uninitalized.
					initTableIfNotExists(ps, pID, dID, tID)
					ps[pID][dID][tID] = append(ps[pID][dID][tID], &bigquery.TableDataInsertAllRequestRows{tr.InsertId, tr.Json, []string{}})
				}
			default:
				require.Fail("Insert was called more than once")
			}

			// Notify that this table was mock "flushed".
			inserted <- struct{}{}

			calledNum++

			return &res, nil
		})}

	// Make retry sleep delay as small as possible and != 0.
	w, err := NewSyncWorker(&client, SetSyncMaxRetries(10), SetSyncRetryInterval(1*time.Nanosecond))
	require.NoError(err)

	// Queue 5 rows to the same table.
	w.Enqueue(NewRowWithID("p", "d", "t", "id0", map[string]bigquery.JsonValue{"k0": "v0"}))
	w.Enqueue(NewRowWithID("p", "d", "t", "id1", map[string]bigquery.JsonValue{"k1": "v1"}))
	w.Enqueue(NewRowWithID("p", "d", "t", "id2", map[string]bigquery.JsonValue{"k2": "v2"}))
	w.Enqueue(NewRowWithID("p", "d", "t", "id3", map[string]bigquery.JsonValue{"k3": "v3"}))
	w.Enqueue(NewRowWithID("p", "d", "t", "id4", map[string]bigquery.JsonValue{"k4": "v4"}))

	// Test only a single insert occured.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		select {
		case <-inserted:
		case <-time.After(1 * time.Second):
			assert.Fail("initial insert didn't occur as expected")
		}
	}()

	insertErrs := w.InsertWithRetry()

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(1 * time.Second):
		require.Fail("insert didn't occur as expected")
	}

	assert.Len(inserted, 0) // Test no more inserts happened.
	assert.Len(w.row, 0)    // Test rows were reset after insert.

	// Test rows slice wasn't re-allocated with a larger size.
	// This make sure we're keeping the same row slice size throught the
	// Worker's life.
	assert.Equal(rowSize, cap(w.row))

	// A 501 error should cause an insertError with a 501 error
	// to be returned, along with the result (which is empty for this test).
	tables := insertErrs.All()
	require.Len(tables, 1)
	require.Len(tables[0].Attempts(), 1)
	assert.EqualError(
		tables[0].Attempts()[0].Error(),
		`googleapi: got HTTP response code 501 with body: {"code":501,"message":"m","Body":"b","Header":null,"Errors":[{"reason":"r1","message":"m1"},{"reason":"r2","message":"m2"}]}`)
	assert.Empty(tables[0].Attempts()[0].All())

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

// TestSyncWorkerMaxRetryInsert tests if a repeatedly failing insert attempt
// (failing with non-rejected rows errors) is eventually dropped and ignored,
// and Worker is moving on to the next table insert.
func TestSyncWorkerMaxRetryInsert(t *testing.T) {
	t.Parallel()

	assert := assert.New(t)
	require := require.New(t)

	// Mock response to return mocked errors, and notify table was mock "flushed".
	// Also count the times the insert function was called to make sure it's retried exactly 3 times.
	inserted := make(chan struct{})
	calledNum := 0
	client := http.Client{
		Transport: newTransport(func(req *http.Request) (*http.Response, error) {
			pID, dID, tID := getInsertMetadata(req.URL.Path)
			var tableReq bigquery.TableDataInsertAllRequest
			b, _ := ioutil.ReadAll(req.Body)
			require.NoError(json.Unmarshal(b, &tableReq))

			var err error

			// Default response.
			res := http.Response{
				Header:     make(http.Header),
				Request:    req,
				StatusCode: 200,
				// Empty JSON body, meaning "no errors".
				Body: ioutil.NopCloser(bytes.NewBufferString(`{}`))}

			// Return Google API HTTP 503 error on every call, which should be retried and logged.
			// Thus, we should expect 3 logged
			if calledNum < 3 {
				err = &googleapi.Error{
					Code:    503,
					Message: "m",
					Body:    "b",
					Errors: []googleapi.ErrorItem{
						googleapi.ErrorItem{Reason: "r1", Message: "m1"},
						googleapi.ErrorItem{Reason: "r2", Message: "m2"},
					}}

				if b, err := json.Marshal(err); assert.NoError(err) {
					// Response should be HTTP 503.
					res = http.Response{
						Header:     make(http.Header),
						Request:    req,
						StatusCode: 503,
						Body:       ioutil.NopCloser(bytes.NewBuffer(b))}
				}

				// Test request payload.
				ps := projects{}
				for _, tr := range tableReq.Rows {
					assert.NotNil(tr)

					// Mock "insert row" to table: Create project, dataset and table
					// if uninitalized.
					initTableIfNotExists(ps, pID, dID, tID)
					ps[pID][dID][tID] = append(ps[pID][dID][tID], &bigquery.TableDataInsertAllRequestRows{tr.InsertId, tr.Json, []string{}})
				}
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
			} else {
				assert.Fail("Insert was called more than 3 times")
			}

			// Notify that this table was mock "inserted".
			inserted <- struct{}{}

			calledNum++

			return &res, nil
		})}

	// Set a row threshold to 5 so it will flush immediately on calling Start().
	// Also make retry sleep delay as small as possible and != 0.
	w, err := NewSyncWorker(&client, SetSyncMaxRetries(3), SetSyncRetryInterval(1*time.Nanosecond))
	require.NoError(err)

	// Queue 5 rows to the same table.
	w.Enqueue(NewRowWithID("p", "d", "t", "id0", map[string]bigquery.JsonValue{"k0": "v0"}))
	w.Enqueue(NewRowWithID("p", "d", "t", "id1", map[string]bigquery.JsonValue{"k1": "v1"}))
	w.Enqueue(NewRowWithID("p", "d", "t", "id2", map[string]bigquery.JsonValue{"k2": "v2"}))
	w.Enqueue(NewRowWithID("p", "d", "t", "id3", map[string]bigquery.JsonValue{"k3": "v3"}))
	w.Enqueue(NewRowWithID("p", "d", "t", "id4", map[string]bigquery.JsonValue{"k4": "v4"}))

	// Test only a single insert occured.
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			select {
			case <-inserted:
			case <-time.After(1 * time.Second):
				assert.Fail("insert didn't occur as expected", strconv.Itoa(i))
			}
		}(i)
	}

	insertErrs := w.InsertWithRetry()

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(1 * time.Second):
		require.Fail("insert didn't occur as expected")
	}

	assert.Len(inserted, 0) // Test no more inserts happened.
	assert.Len(w.row, 0)    // Test rows were reset after insert.

	tables := insertErrs.All()
	require.Len(tables, 1)
	// 3 attempts and the 4th is the one that aborts the insert operation.
	require.Len(tables[0].Attempts(), 4)
	assert.EqualError(tables[0].Attempts()[0].Error(), `googleapi: got HTTP response code 503 with body: {"code":503,"message":"m","Body":"b","Header":null,"Errors":[{"reason":"r1","message":"m1"},{"reason":"r2","message":"m2"}]}`)
	assert.EqualError(tables[0].Attempts()[1].Error(), `googleapi: got HTTP response code 503 with body: {"code":503,"message":"m","Body":"b","Header":null,"Errors":[{"reason":"r1","message":"m1"},{"reason":"r2","message":"m2"}]}`)
	assert.EqualError(tables[0].Attempts()[2].Error(), `googleapi: got HTTP response code 503 with body: {"code":503,"message":"m","Body":"b","Header":null,"Errors":[{"reason":"r1","message":"m1"},{"reason":"r2","message":"m2"}]}`)
	assert.EqualError(tables[0].Attempts()[3].Error(), "Insert table p.d.t retried 4 times, dropping insert and moving on")
}

// getInsertMetadata is a helper function that fetches the project, dataset,
// and table IDs from a url string.
func getInsertMetadata(url string) (projectID, datasetID, tableID string) {
	re := regexp.MustCompile(`bigquery/v2/projects/(?P<projectID>.+?)/datasets/(?P<datasetID>.+?)/tables/(?P<tableId>.+?)/insertAll`)
	res := re.FindAllStringSubmatch(url, -1)
	p := res[0][1]
	d := res[0][2]
	t := res[0][3]
	return p, d, t
}

// transport is a mock http.Transport, and implements http.RoundTripper
// interface.
//
// It is used for mocking BigQuery responses via bigquery.Service.
type transport struct {
	roundTrip func(*http.Request) (*http.Response, error)
}

func newTransport(roundTrip func(*http.Request) (*http.Response, error)) *transport {
	return &transport{roundTrip}
}
func (t *transport) RoundTrip(req *http.Request) (*http.Response, error) { return t.roundTrip(req) }
