package bqstreamer

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bigquery "google.golang.org/api/bigquery/v2"
	"google.golang.org/api/googleapi"
)

// TestNewStreamer tests creating a new Streamer.
func TestNewStreamer(t *testing.T) {
	t.Parallel()

	assert := assert.New(t)
	var err error

	// Test sending bad arguments.
	_, err = NewStreamer(nil, 0, 1*time.Second, 1*time.Second, 10)
	assert.Error(err)
	_, err = NewStreamer(nil, -1, 1*time.Second, 1*time.Second, 10)
	assert.Error(err)
	_, err = NewStreamer(nil, 10, 0, 1*time.Second, 10)
	assert.Error(err)
	_, err = NewStreamer(nil, 10, -1*time.Nanosecond, 1*time.Second, 10)
	assert.Error(err)
	_, err = NewStreamer(nil, 10, 1*time.Nanosecond, -1*time.Second, 10)
	assert.Error(err)
	_, err = NewStreamer(nil, 10, 1*time.Nanosecond, -1*time.Second, -1)
	assert.Error(err)

	// Test valid arguments.
	s, err := NewStreamer(nil, 10, 1*time.Second, 1*time.Second, 10)
	assert.NoError(err)
	assert.NotNil(s.rowChannel)
	assert.Empty(s.rowChannel)
	assert.Equal(10, cap(s.rowChannel))
	assert.NotNil(s.rows)
	assert.Equal(0, s.rowIndex)
	assert.Equal(1*time.Second, s.MaxDelay)
	assert.Equal(1*time.Second, s.SleepBeforeRetry)
	assert.Equal(10, s.MaxRetryInsert)
	assert.NotNil(s.stopChannel)
	assert.Empty(s.stopChannel)
	assert.NotNil(s.Errors)
	assert.Empty(s.Errors)
	assert.NotNil(s.Start)
	assert.NotNil(s.Stop)
	assert.NotNil(s.flush)
	assert.NotNil(s.insertAll)
	assert.NotNil(s.insertTable)
}

// TestStop calls Stop(), starts a streamer, and checks if it immediately stopped.
func TestStop(t *testing.T) {
	t.Parallel()

	assert := assert.New(t)
	require := require.New(t)

	// Set delay threshold to be large enough so we could test if stop message
	// caused streamer to stop and flush.
	s, err := NewStreamer(nil, 100, 1*time.Minute, 1*time.Second, 10)
	require.NoError(err)

	// Override flush function to just send a "flushed" notification,
	// instead of actually sending to BigQuery.
	flushed := make(chan struct{})
	s.flush = func() { flushed <- struct{}{} }

	go s.Stop()
	select {
	case <-s.stopChannel:
	case <-time.After(1 * time.Second):
		assert.Fail("a stop message wasn't sent on s.stopChannel fast enough")
	}

	// Test if streamer flushes quickly after a stop signal is sent.
	go s.Stop()
	go s.Start()
	select {
	case <-flushed:
	case <-time.After(1 * time.Second):
		assert.Fail("flush() wasn't called fast enough")
	}
}

// TestQueueRow tests queueing a row.
func TestQueueRow(t *testing.T) {
	t.Parallel()

	assert := assert.New(t)
	require := require.New(t)

	s, err := NewStreamer(nil, 10, 1*time.Second, 1*time.Second, 10)
	require.NoError(err)

	data := map[string]bigquery.JsonValue{"test_key": "test_value"}
	s.QueueRow("p", "d", "t", data)

	var q *row
	select {
	case q = <-s.rowChannel:
	default:
		require.Fail("no row on s.rowChannel")
	}

	assert.Equal("p", q.projectID)
	assert.Equal("d", q.datasetID)
	assert.Equal("t", q.tableID)

	if v, ok := q.data["test_key"]; assert.True(ok) {
		assert.Equal(v, data["test_key"])
	}
}

// TestMaxDelayFlushCall tests streamer is calling flush() to BigQuery when maxDelay
// timer expires.
func TestMaxDelayFlushCall(t *testing.T) {
	t.Parallel()

	assert := assert.New(t)
	require := require.New(t)

	// Set maxRows = 10 and insert a single line, so we can be sure flushing
	// occured by delay timer expiring.
	s, err := NewStreamer(nil, 10, 1*time.Second, 1*time.Second, 10)
	require.NoError(err)

	// Override flush function to just send a "flushed" notification,
	// instead of actually sending to BigQuery.
	flushed := make(chan struct{})
	s.flush = func() { flushed <- struct{}{} }

	data := map[string]bigquery.JsonValue{"test_key": "test_value"}
	r := row{"p", "d", "t", data}

	s.QueueRow(r.projectID, r.datasetID, r.tableID, r.data)

	// Start streamer and measure time it should take to flush by maxDelay.
	// Add a small interval to timer to avoid failing when our timer expired
	// just a moment before streamer's timer.
	go s.Start()
	defer s.Stop()

	// Fail if no flush happened in maxDelay time.
	select {
	case <-flushed:
	case <-time.After(s.MaxDelay + 1*time.Millisecond):
		assert.Fail("flush() wasn't called in maxDelay time (%.2f seconds)", s.MaxDelay.Seconds())
	}

	// Flushing doesn't actually flush in this test, so s.rows still holds what
	// we inserted.
	assert.Equal(r.projectID, s.rows[0].projectID)
}

// TestMaxRowsFlushCal tests streamer is calling flush() to insert BigQuery when maxRows have
// been queued.
func TestMaxRowsFlushCall(t *testing.T) {
	t.Parallel()

	assert := assert.New(t)
	require := require.New(t)

	// Set a long delay before flushing, so we can be sure flushing occured due
	// to rows filling up.
	s, err := NewStreamer(nil, 10, 1*time.Minute, 1*time.Second, 10)
	require.NoError(err)

	// Override flush function to just send a "flushed" notification,
	// instead of actually sending to BigQuery.
	flushed := make(chan struct{})
	s.flush = func() { flushed <- struct{}{} }

	data := map[string]bigquery.JsonValue{"test_key": "test_value"}
	r := row{"p", "d", "t", data}

	// Start streamer and measure time it should take to flush by maxRows.
	go s.Start()

	// Insert 10 rows to force flushing by maxRows.
	for i := 0; i <= 9; i++ {
		s.QueueRow(r.projectID, r.datasetID, r.tableID, r.data)
	}

	// Test if flushing happened almost immediately, forced by rows queue being filled.
	select {
	case <-flushed:
	case <-time.After(1 * time.Second):
		assert.Fail("flush() wasn't called by rows queue getting filled")
	}

	s.Stop()

	// Flushing doesn't actually flush in this test, so s.rows still holds what
	// we inserted.
	for i := 0; i <= 9; i++ {
		require.Equal(r.projectID, s.rows[i].projectID)
	}
}

// TestInsertAll queues 20 rows to 4 tables, 5 to each row,
// and tests if rows were inserted to a mock project-dataset-table-rows type.
func TestInsertAll(t *testing.T) {
	t.Parallel()

	assert := assert.New(t)
	require := require.New(t)

	// We intend to insert 20 rows in this test, so set maxRows = 20 and delay
	// to be long enough so flush will occur due to rows queue filling up.
	s, err := NewStreamer(nil, 20, 1*time.Minute, 1*time.Second, 10)
	require.NoError(err)

	// Mock insertTable function to cache all rows from all tables to a local variable.
	// Will be used for testing which rows were actually "inserted".
	ps := map[string]project{}
	flushed := make(chan struct{})

	s.insertTable = func(pId, dId, tId string, tt table) (r *bigquery.TableDataInsertAllResponse, err error) {
		// Add all table rows to a local projects-datasets-table map,
		// mocking rows that were inserted to BigQuery, which we will test against.
		for i, tr := range tt {
			assert.NotNil(tr, fmt.Sprintf("%s.%s.%s.row[%d]", pId, dId, tId, i))

			// Mock "insert row" to table: Create project, dataset and table
			// if uninitalized.
			createTableIfNotExists(ps, pId, dId, tId)
			ps[pId][dId][tId] = append(ps[pId][dId][tId], tr)
		}

		// Return 0 insert errors.
		r = &bigquery.TableDataInsertAllResponse{}

		// Notify that this table was mock "flushed".
		flushed <- struct{}{}

		return
	}

	// Distribute 5 rows to 4 tables in 2 datasets in 2 projects (total 20 rows).
	for i := 0; i < 5; i++ {
		k := fmt.Sprintf("k%d", i)
		v := fmt.Sprintf("v%d", i)
		s.QueueRow("p1", "d1", "t1", map[string]bigquery.JsonValue{k: v})
		s.QueueRow("p1", "d1", "t2", map[string]bigquery.JsonValue{k: v})
		s.QueueRow("p1", "d2", "t1", map[string]bigquery.JsonValue{k: v})
		s.QueueRow("p2", "d1", "t1", map[string]bigquery.JsonValue{k: v})
	}

	// Start streamer and wait for 4 flushes to happen, one for each table.
	// Fail if flushes take too long.
	go s.Start()
	for i := 0; i < 4; i++ {
		select {
		case <-flushed:
		case <-time.After(1 * time.Millisecond):
			assert.Fail(fmt.Sprintf("flush() %d wasn't called fast enough", i))
		}
	}
	s.Stop()

	assert.Equal(0, s.rowIndex, "rowIndex not reset to 0 after all flushes were done")

	// Check all queued projects, datasets, and tables were actually mock "inserted".
	// We can't use assert.Equals() here since the table rows have a generated
	// row ID.
	_, ok := ps["p1"]
	require.True(ok)
	_, ok = ps["p2"]
	require.True(ok)
	_, ok = ps["p1"]["d1"]
	require.True(ok)
	_, ok = ps["p1"]["d2"]
	require.True(ok)
	_, ok = ps["p1"]["d1"]["t1"]
	require.True(ok)
	_, ok = ps["p1"]["d1"]["t2"]
	require.True(ok)

	// Check no more than 2 projects, 2 datasets, and 2 tables were mock "inserted".
	require.Len(ps, 2)
	require.Len(ps["p1"], 2)
	require.Len(ps["p1"]["d1"], 2)

	// Check all rows from all tables were mock "inserted".
	checkRows := func(ps map[string]project, p, d, tt string, num int) {
		for i := 0; i < num; i++ {
			r := ps[p][d][tt][i]
			require.NotNil(r, fmt.Sprintf("Row %d missing from table %s.%s.%s", i, p, d, tt))

			v, ok := r.jsonValue[fmt.Sprintf("k%d", i)]
			require.True(ok, fmt.Sprintf("Key k%d missing from %s.%s.%s.row[%d] (row is %s)", i, p, d, tt, i, r))
			require.Equal(v, fmt.Sprintf("v%d", i), fmt.Sprintf("%s.%s.%s.row[%d] key's value is not v%d (is %s)", p, d, tt, i, i, v))
		}

		if len(ps[p][d][tt]) > num {
			require.Fail("There are more rows in %s.%s.%s than expected (there are %d)", p, d, tt, len(ps[p][d][tt]))
		}
	}
	checkRows(ps, "p1", "d1", "t1", 5)
	checkRows(ps, "p1", "d1", "t2", 5)
	checkRows(ps, "p1", "d2", "t1", 5)
	checkRows(ps, "p1", "d1", "t1", 5)
}

// TestShouldRetryInsertAfterError tests if the ShouldRetry function correctly
// returns true for specific errors.
// Currently only for googleapi.Errorgoogleapi.Error.Code = 503 or 500
func TestShouldRetryInsertAfterError(t *testing.T) {
	t.Parallel()

	assert := assert.New(t)
	require := require.New(t)

	s, err := NewStreamer(nil, 5, 1*time.Minute, 1*time.Second, 10)
	require.NoError(err)

	// Test whether a GoogleAPI HTTP 503 error returns true.
	// i.e. the function decides the request related to this error should be retried.
	assert.True(s.shouldRetryInsertAfterError(
		&googleapi.Error{Code: 503, Message: "m", Body: "b", Errors: []googleapi.ErrorItem{
			googleapi.ErrorItem{Reason: "r1", Message: "m1"},
			googleapi.ErrorItem{Reason: "r2", Message: "m2"}}}),
		"GoogleAPI HTTP 503 insert error wasn't marked as \"to be retried\"")

	// Test whether errors above were logged.
	select {
	case err = <-s.Errors:
		if assert.IsType(&googleapi.Error{}, err) {
			assert.Equal(
				&googleapi.Error{Code: 503, Message: "m", Body: "b", Errors: []googleapi.ErrorItem{
					googleapi.ErrorItem{Reason: "r1", Message: "m1"},
					googleapi.ErrorItem{Reason: "r2", Message: "m2"}}},
				err.(*googleapi.Error))
		}
	default:
		assert.Fail("Logged error after GoogleAPI 503 insert error is missing")
	}

	// Test another GoogleAPI response with a different status code,
	// and check the function returns false.
	assert.False(s.shouldRetryInsertAfterError(
		&googleapi.Error{Code: 501, Message: "m", Body: "b", Errors: []googleapi.ErrorItem{
			googleapi.ErrorItem{Reason: "r1", Message: "m1"},
			googleapi.ErrorItem{Reason: "r2", Message: "m2"}}}),
		"GoogleAPI HTTP 500 insert error was marked as \"to be retried\"")

	// Test whether errors above were logged.
	select {
	case err = <-s.Errors:
		if assert.IsType(&googleapi.Error{}, err) {
			assert.Equal(
				&googleapi.Error{Code: 501, Message: "m", Body: "b", Errors: []googleapi.ErrorItem{
					googleapi.ErrorItem{Reason: "r1", Message: "m1"},
					googleapi.ErrorItem{Reason: "r2", Message: "m2"}}},
				err.(*googleapi.Error))
		}
	default:
		assert.Fail("Logged error after GoogleAPI 500 insert error is missing")
	}

	// Test a non-GoogleAPI error, but a a generic error instead.
	assert.False(s.shouldRetryInsertAfterError(fmt.Errorf("Non-GoogleAPI error")),
		"Non-GoogleAPI error was marked as \"to be retried\"")

	// Test whether the error above was logged.
	select {
	case err = <-s.Errors:
		assert.EqualError(err, "Non-GoogleAPI error")
	default:
		assert.Fail("Logged error after Non-GoogleAPI error is missing")
	}
}

// TestRemoveRowsFromTable inserts rows, then removes a part of them.
// It then checks whether the correct rows where indeed removed.
func TestRemoveRowsFromTable(t *testing.T) {
	t.Parallel()

	assert := assert.New(t)
	require := require.New(t)

	// Make maxRows bigger (10) than the amount of rows we're going to insert
	// in this test (5), so flushing won't happen because of this.
	// Also, make maxDelay longer than the amount of time this test is going to take,
	// for the same reason.
	s, err := NewStreamer(nil, 10, 1*time.Minute, 1*time.Second, 10)
	require.NoError(err)

	// Split 10 rows equally to two tables.

	tb := table{
		&tableRow{rowID: "0", jsonValue: map[string]bigquery.JsonValue{"k10": "v10"}},
		&tableRow{rowID: "0", jsonValue: map[string]bigquery.JsonValue{"k11": "v11"}},
		&tableRow{rowID: "0", jsonValue: map[string]bigquery.JsonValue{"k12": "v12"}},
		&tableRow{rowID: "0", jsonValue: map[string]bigquery.JsonValue{"k13": "v13"}},
		&tableRow{rowID: "0", jsonValue: map[string]bigquery.JsonValue{"k14": "v14"}}}

	// Remove rows 0, 3, 4 from table #1.
	filtered := s.filterRowsFromTable([]int64{0, 3, 4}, tb)

	// This should leave rows 1, 2 now inside.
	// This should remove row 4, then 3, and then 0 and leave rows 1, 2 inside.
	// Removal is done in place, switching places with the last element in slice.
	// Thus row 2 should be first (switched with row 0 upon its deletion),
	// and row 1 afterwards.
	if val, ok := filtered[0].jsonValue["k12"]; assert.True(ok) {
		assert.Equal("v12", val)
	}

	if val, ok := filtered[1].jsonValue["k11"]; assert.True(ok) {
		assert.Equal("v11", val)
	}
}

// TestFilterRejectedRows inserts rows and mocks a rejected rows response.
// It then tests whether correct rows were removed and if the right removed
// indexes were returned by filterRejectedRows()
func TestFilterRejectedRows(t *testing.T) {
	t.Parallel()

	assert := assert.New(t)
	require := require.New(t)

	// Make maxRows bigger (10) than the amount of rows we're going to insert
	// in this test (5), so flushing won't happen because of this.
	// Also, make maxDelay longer than the amount of time this test is going to take,
	// for the same reason.
	s, err := NewStreamer(nil, 10, 1*time.Minute, 1*time.Second, 10)
	require.NoError(err)

	// Distribute 10 rows equally to 2 tables.
	d := map[string]table{
		"t1": table{
			&tableRow{rowID: "0", jsonValue: map[string]bigquery.JsonValue{"k10": "v10"}},
			&tableRow{rowID: "0", jsonValue: map[string]bigquery.JsonValue{"k11": "v11"}},
			&tableRow{rowID: "0", jsonValue: map[string]bigquery.JsonValue{"k12": "v12"}},
			&tableRow{rowID: "0", jsonValue: map[string]bigquery.JsonValue{"k13": "v13"}},
			&tableRow{rowID: "0", jsonValue: map[string]bigquery.JsonValue{"k14": "v14"}}},
		"t2": table{
			&tableRow{rowID: "0", jsonValue: map[string]bigquery.JsonValue{"k20": "v20"}},
			&tableRow{rowID: "0", jsonValue: map[string]bigquery.JsonValue{"k21": "v21"}},
			&tableRow{rowID: "0", jsonValue: map[string]bigquery.JsonValue{"k22": "v22"}},
			&tableRow{rowID: "0", jsonValue: map[string]bigquery.JsonValue{"k23": "v23"}},
			&tableRow{rowID: "0", jsonValue: map[string]bigquery.JsonValue{"k24": "v24"}}}}

	// Return rejected rows 0, 1, 3 for insert 1.
	r1 := &bigquery.TableDataInsertAllResponse{
		Kind: "bigquery",
		InsertErrors: []*bigquery.TableDataInsertAllResponseInsertErrors{
			&bigquery.TableDataInsertAllResponseInsertErrors{Index: 0, Errors: []*bigquery.ErrorProto{
				&bigquery.ErrorProto{Location: "l00", Message: "m00", Reason: "invalid"},
				&bigquery.ErrorProto{Location: "l01", Message: "m01", Reason: "r01"},
				&bigquery.ErrorProto{Location: "l02", Message: "m02", Reason: "invalid"},
				&bigquery.ErrorProto{Location: "l03", Message: "m03", Reason: "r03"},
				&bigquery.ErrorProto{Location: "l04", Message: "m04", Reason: "invalid"}}},
			&bigquery.TableDataInsertAllResponseInsertErrors{Index: 1, Errors: []*bigquery.ErrorProto{
				&bigquery.ErrorProto{Location: "l10", Message: "m10", Reason: "r10"},
				&bigquery.ErrorProto{Location: "l11", Message: "m11", Reason: "invalid"}}},
			// The following row is marked as "stopped", thus should be retried.
			&bigquery.TableDataInsertAllResponseInsertErrors{Index: 2, Errors: []*bigquery.ErrorProto{
				&bigquery.ErrorProto{Location: "l20", Message: "m20", Reason: "stopped"}}},
			&bigquery.TableDataInsertAllResponseInsertErrors{Index: 4, Errors: []*bigquery.ErrorProto{
				&bigquery.ErrorProto{Location: "l40", Message: "m40", Reason: "invalid"}}}}}

	// Return no rejected rows (i.e. successful insert) for insert 2.
	r2 := &bigquery.TableDataInsertAllResponse{}

	rejected1 := s.filterRejectedRows(r1, "p", "d", "t1", d)
	rejected2 := s.filterRejectedRows(r2, "p", "d", "t2", d)

	// Test the rejected rows' indexes is correct.
	require.Equal(3, len(rejected1))
	for i, e := range []int64{4, 2, 0} {
		assert.NotEqual(e, rejected1[i], i)
	}

	// This call's insert request should only include rows 2, 3 in that order.
	// This is because row 2 was marked as "stopped",
	// and row 3 wasn't marked by any error at all.
	//
	// So, test t1 remaining rows are as mentioned.
	require.Equal(2, len(d["t1"]))
	if val, ok := d["t1"][0].jsonValue["k12"]; assert.True(ok) {
		assert.Equal("v12", val)
	}

	if val, ok := d["t1"][1].jsonValue["k13"]; assert.True(ok) {
		assert.Equal("v13", val)
	}

	assert.Empty(rejected2)

	// Test t2 rows weren't changed at all.
	for i := 0; i < len(d["t2"]); i++ {
		k := fmt.Sprintf("k2%d", i)
		v := fmt.Sprintf("v2%d", i)
		if val, ok := d["t2"][i].jsonValue[k]; assert.True(ok) {
			assert.Equal(v, val)
		}
	}
}

// TestInsertAllWithServerErrorResponse tests if an insert failed with a server
// error (500, 503) triggers a retry insert.
func TestInsertAllWithServerErrorResponse(t *testing.T) {
	t.Parallel()

	assert := assert.New(t)
	require := require.New(t)

	// Set a row threshold to 5 so it will flush immediately on calling Start().
	// Also make retry sleep delay as small as possible and != 0.
	s, err := NewStreamer(nil, 5, 1*time.Minute, 1*time.Nanosecond, 10)
	require.NoError(err)

	// Queue 5 rows to the same table.
	for i := 0; i < 5; i++ {
		k := fmt.Sprintf("k%d", i)
		v := fmt.Sprintf("v%d", i)
		s.QueueRow("p", "d", "t", map[string]bigquery.JsonValue{k: v})
	}

	// Mock insertTable function to return mocked errors and notify table was mock "flushed".
	// Also count the times insertTable() is called to make sure it's retried exactly once.
	flushed := make(chan struct{})
	calledNum := 0
	s.insertTable = func(pId, dId, tId string, tt table) (r *bigquery.TableDataInsertAllResponse, err error) {
		// Return 503 server error on first call, and test if it triggered
		// a retry insert with exactly same data on the second call.
		switch calledNum {
		case 0:
			err = &googleapi.Error{Code: 503, Message: "m", Body: "b", Errors: []googleapi.ErrorItem{
				googleapi.ErrorItem{Reason: "r1", Message: "m1"},
				googleapi.ErrorItem{Reason: "r2", Message: "m2"}}}
		case 1:
			// This call to insertTable() should keep the table is it is,
			// and trigger retry after server error.
			require.Equal(5, len(tt))
			for i := 0; i < 5; i++ {
				k := fmt.Sprintf("k%d", i)
				v := fmt.Sprintf("v%d", i)
				if val, ok := tt[i].jsonValue[k]; assert.True(ok) {
					assert.Equal(v, val)
				}
			}
		default:
			assert.Fail("insertTable() was called more than 2 times")
		}

		// Notify that this table was mock "flushed".
		flushed <- struct{}{}

		calledNum++

		return
	}

	// Start streamer and wait for flush to happen.
	// Fail if flush takes too long.
	go s.Start()
	defer s.Stop()

	// Test HTTP 503 server error response triggered a retry.
	//
	// First loop is for testing initial insert,
	// second is for testing retry insert happened.
	for i := 0; i < 2; i++ {
		select {
		case <-flushed:
		case <-time.After(1 * time.Second):
			assert.Fail(fmt.Sprintf("insert %d didn't happen fast enough", i))
		}
	}

	// Make sure insertTable() wasn't called again i.e. no retry happened.
	// This is done by waiting for a fair amount of time
	// and testing no flush happened.
	select {
	case <-flushed:
		assert.Fail("retry insert happened another time after test was finished")
	case <-time.After(1 * time.Second):
	}
}

// TestInsertAllWithNonServerErrorResponse tests if an insert failed with an error
// which is NOT server error (503) does NOT trigger a retry insert.
func TestInsertAllWithNonServerErrorResponse(t *testing.T) {
	t.Parallel()

	assert := assert.New(t)
	require := require.New(t)

	// Set a row threshold to 5 so it will flush immediately on calling Start().
	// Also make retry sleep delay as small as possible and != 0.
	s, err := NewStreamer(nil, 5, 1*time.Minute, 1*time.Nanosecond, 10)
	require.NoError(err)

	// Queue 5 rows to the same table.
	for i := 0; i < 5; i++ {
		k := fmt.Sprintf("k%d", i)
		v := fmt.Sprintf("v%d", i)
		s.QueueRow("p", "d", "t", map[string]bigquery.JsonValue{k: v})
	}

	// Mock insertTable function to return mocked errors and notify table was mock "flushed".
	// Also count the times insertTable() is called to make sure it's retried exactly once.
	flushed := make(chan struct{})
	calledNum := 0
	s.insertTable = func(pId, dId, tId string, tt table) (r *bigquery.TableDataInsertAllResponse, err error) {
		// Return a 501 (not 500, 503) server error on first call,
		// and test if it did NOT trigger a retry insert.
		switch calledNum {
		case 0:
			err = &googleapi.Error{Code: 501, Message: "m", Body: "b", Errors: []googleapi.ErrorItem{
				googleapi.ErrorItem{Reason: "r1", Message: "m1"},
				googleapi.ErrorItem{Reason: "r2", Message: "m2"}}}
		default:
			require.Fail("insertTable() was called more than once")
		}

		// Notify that this table was mock "flushed".
		flushed <- struct{}{}

		calledNum++

		return
	}

	go s.Start()
	defer s.Stop()

	// Test initial insert.
	select {
	case <-flushed:
	case <-time.After(1 * time.Second):
		require.Fail("initial insert didn't happen fast enough")
	}

	// Make sure insertTable() wasn't called again i.e. no retry happened.
	select {
	case <-flushed:
		assert.Fail("retry insert happened even though server error != 503")
	case <-time.After(1 * time.Second):
	}
}

// TestMaxRetryInsert tests if a repeatedly failing insert attempt
// (failing with non-rejected rows errors) is eventually dropped and ignored,
// and streamer is moving on to the next table insert.
func TestMaxRetryInsert(t *testing.T) {
	t.Parallel()

	assert := assert.New(t)
	require := require.New(t)

	// Set a row threshold to 5 so it will flush immediately on calling Start().
	// Also make retry sleep delay as small as possible and != 0.
	s, err := NewStreamer(nil, 5, 1*time.Minute, 1*time.Nanosecond, 3)
	require.NoError(err)

	// Queue 5 rows to the same table.
	for i := 0; i < 5; i++ {
		k := fmt.Sprintf("k%d", i)
		v := fmt.Sprintf("v%d", i)
		s.QueueRow("p", "d", "t", map[string]bigquery.JsonValue{k: v})
	}

	// Mock insertTable function to return mocked errors and notify table was mock "flushed".
	// Also count the times insertTable() is called to make sure it's retried exactly 3 times.
	insertCalled := make(chan struct{})
	calledNum := 0
	s.insertTable = func(pId, dId, tId string, tt table) (r *bigquery.TableDataInsertAllResponse, err error) {
		// Return Google API HTTP 503 error on every call, which should be retried and logged.
		// Thus, we should expect 3 logged errors.
		if calledNum < 3 {
			err = &googleapi.Error{Code: 503, Message: "m", Body: "b", Errors: []googleapi.ErrorItem{
				googleapi.ErrorItem{Reason: "r1", Message: "m1"},
				googleapi.ErrorItem{Reason: "r2", Message: "m2"}}}
		} else {
			assert.Fail("insertTable() was called more than 3 times")
		}

		// Notify that this table was mock "flushed".
		insertCalled <- struct{}{}

		calledNum++

		return
	}

	// Start streamer and wait for flush to happen.
	// Fail if flush takes too long.
	go s.Start()
	defer s.Stop()

	// Test each insert logged an error.
	gerr := googleapi.Error{Code: 503, Message: "m", Body: "b", Errors: []googleapi.ErrorItem{
		googleapi.ErrorItem{Reason: "r1", Message: "m1"},
		googleapi.ErrorItem{Reason: "r2", Message: "m2"}}}
	for i := 0; i < 3; i++ {
		select {
		case <-time.After(1 * time.Second):
			assert.Fail("insert %d didn't happen fast enough", i)
		case <-insertCalled:
			// Wait a bit for the current insert table iteration to check for
			// errors and decide whether to retry or not.
			time.Sleep(1 * time.Millisecond)

			// Check that on each insert, an error was logged.
			// This should happen except for the 3rd insert,
			// which returns an HTTP 503.
			select {
			case err, ok := <-s.Errors:
				assert.True(ok, "Error channel is closed")
				assert.IsType(&googleapi.Error{}, err)
				assert.EqualError(err, gerr.Error())
			default:
				assert.Fail(fmt.Sprintf("Error %d is missing from error channel", i))
			}
		}
	}

	// Make sure insertTable() wasn't called again i.e. no retry happened.
	// This is done by waiting for a fair amount of time
	// and testing no flush happened.
	select {
	case <-insertCalled:
		assert.Fail("retry insert happened for the 4th time after test was finished")
	case <-time.After(1 * time.Second):
	}

	// Test "too many retries, dropping insert" error was logged.
	select {
	case err, ok := <-s.Errors:
		assert.True(ok, "Error channel is closed")
		assert.IsType(&TooManyFailedInsertRetriesError{}, err)
		assert.EqualError(err, "Insert table p.d.t retried 4 times, dropping insert and moving on")
	default:
		assert.Fail("Error \"too many retries\" is missing from error channel")
	}
}

// TestInsertAllWithRejectedResponse tests if an insert failed with rejected
// rows triggers a retry insert only with non-rejected rows.
func TestInsertAllWithRejectedResponse(t *testing.T) {
	t.Parallel()

	assert := assert.New(t)
	require := require.New(t)

	// Set a row threshold to 5 so it will flush immediately on calling Start().
	// Also make retry sleep delay as small as possible and != 0.
	s, err := NewStreamer(nil, 5, 1*time.Minute, 1*time.Nanosecond, 10)
	require.NoError(err)

	// Queue 5 rows to the same table.
	for i := 0; i < 5; i++ {
		k := fmt.Sprintf("k%d", i)
		v := fmt.Sprintf("v%d", i)
		s.QueueRow("p", "d", "t", map[string]bigquery.JsonValue{k: v})
	}

	// Mock insertTable function to return mocked errors and notify table was mock "flushed".
	// Also count the times insertTable() is called to make sure it's retried exactly once.
	flushed := make(chan struct{})
	calledNum := 0
	s.insertTable = func(pId, dId, tId string, tt table) (r *bigquery.TableDataInsertAllResponse, err error) {
		// Return a 500 (not 503) server error on first call,
		// and test if it did NOT trigger a retry insert.
		switch calledNum {
		case 0:
			// Return rejected rows 0, 1, 4.
			r = &bigquery.TableDataInsertAllResponse{
				Kind: "bigquery",
				InsertErrors: []*bigquery.TableDataInsertAllResponseInsertErrors{
					&bigquery.TableDataInsertAllResponseInsertErrors{Index: 0, Errors: []*bigquery.ErrorProto{
						&bigquery.ErrorProto{Location: "l00", Message: "m00", Reason: "invalid"},
						&bigquery.ErrorProto{Location: "l01", Message: "m01", Reason: "r01"},
						&bigquery.ErrorProto{Location: "l02", Message: "m02", Reason: "stopped"},
						&bigquery.ErrorProto{Location: "l03", Message: "m03", Reason: "r03"},
						&bigquery.ErrorProto{Location: "l04", Message: "m04", Reason: "invalid"}}},
					&bigquery.TableDataInsertAllResponseInsertErrors{Index: 1, Errors: []*bigquery.ErrorProto{
						&bigquery.ErrorProto{Location: "l10", Message: "m10", Reason: "r10"},
						&bigquery.ErrorProto{Location: "l11", Message: "m11", Reason: "invalid"}}},
					// The following row is marked as "stopped", thus should be retried.
					&bigquery.TableDataInsertAllResponseInsertErrors{Index: 2, Errors: []*bigquery.ErrorProto{
						&bigquery.ErrorProto{Location: "l20", Message: "m20", Reason: "stopped"}}},
					&bigquery.TableDataInsertAllResponseInsertErrors{Index: 4, Errors: []*bigquery.ErrorProto{
						&bigquery.ErrorProto{Location: "l40", Message: "m40", Reason: "timeout"}}}}}
		case 1:
			// This call's insert request should only include rows 3, 4, 2 in that order.
			// The order of indexes is changed because of the way the rows are
			// deleted (see filterRowsFromTable() function)
			//
			// Also, these are the indexes that are not filtered because rows 2,4 were marked
			// as "stopped" and "timeout", and row 3 wasn't marked by any error at all.
			assert.Equal(3, len(tt))
			if val, ok := tt[0].jsonValue["k3"]; assert.True(ok) {
				assert.Equal("v3", val)
			}

			if val, ok := tt[1].jsonValue["k4"]; assert.True(ok) {
				assert.Equal("v4", val)
			}

			if val, ok := tt[2].jsonValue["k2"]; assert.True(ok) {
				assert.Equal("v2", val)
			}

			// Return 0 insert errors.
			r = &bigquery.TableDataInsertAllResponse{}
		default:
			assert.Fail("insertTable() was called more than 3 times")
		}

		// Notify that this table was mock "flushed".
		flushed <- struct{}{}

		calledNum++

		return
	}

	go s.Start()
	defer s.Stop()

	// Test initial insert.
	select {
	case <-flushed:
	case <-time.After(1 * time.Second):
		assert.Fail("initial insert didn't happened fast enough")

	}

	// Test insertTable() is called again after rejected rows response.
	select {
	case <-flushed:
	case <-time.After(1 * time.Second):
		assert.Fail("retry insert didn't happen fast enough after rejected rows response")
	}

	// Make sure insertTable() wasn't called again i.e. no retry happened.
	select {
	case <-flushed:
		assert.Fail("retry insert happened even though no rows were rejected")
	case <-time.After(1 * time.Second):
	}
}

// TestInsertwithAllRowsRejected tests if an insert failed with all rows
// rejected doesn't trigger a retry insert.
func TestInsertwithAllRowsRejected(t *testing.T) {
	t.Parallel()

	assert := assert.New(t)
	require := require.New(t)

	// Set a row threshold to 5 so it will flush immediately on calling Start().
	// Also make retry sleep delay as small as possible and != 0.
	s, err := NewStreamer(nil, 5, 1*time.Minute, 1*time.Nanosecond, 10)
	require.NoError(err)

	// Queue 5 rows to the same table.
	for i := 0; i < 5; i++ {
		k := fmt.Sprintf("k%d", i)
		v := fmt.Sprintf("v%d", i)
		s.QueueRow("p", "d", "t", map[string]bigquery.JsonValue{k: v})
	}

	// Mock insertTable function to return mocked errors and notify table was mock "flushed".
	// Also count the times insertTable() is called to make sure it's retried exactly once.
	flushed := make(chan struct{})
	insertCalled := false
	s.insertTable = func(pId, dId, tId string, tt table) (r *bigquery.TableDataInsertAllResponse, err error) {
		// Return a 500 (not 503) server error on first call,
		// and test if it did NOT trigger a retry insert.
		if !insertCalled {
			// Return a reponse with all rows rejected.
			r = &bigquery.TableDataInsertAllResponse{
				Kind: "bigquery",
				InsertErrors: []*bigquery.TableDataInsertAllResponseInsertErrors{
					&bigquery.TableDataInsertAllResponseInsertErrors{Index: 0, Errors: []*bigquery.ErrorProto{
						&bigquery.ErrorProto{Location: "l00", Message: "m00", Reason: "invalid"}}},
					&bigquery.TableDataInsertAllResponseInsertErrors{Index: 1, Errors: []*bigquery.ErrorProto{
						&bigquery.ErrorProto{Location: "l10", Message: "m10", Reason: "invalid"}}},
					&bigquery.TableDataInsertAllResponseInsertErrors{Index: 2, Errors: []*bigquery.ErrorProto{
						&bigquery.ErrorProto{Location: "l20", Message: "m20", Reason: "invalid"}}},
					&bigquery.TableDataInsertAllResponseInsertErrors{Index: 3, Errors: []*bigquery.ErrorProto{
						&bigquery.ErrorProto{Location: "l30", Message: "m30", Reason: "invalid"}}},
					&bigquery.TableDataInsertAllResponseInsertErrors{Index: 4, Errors: []*bigquery.ErrorProto{
						&bigquery.ErrorProto{Location: "l40", Message: "m40", Reason: "invalid"}}}}}
		} else {
			assert.Fail("insertTable() was called more than 2 times")
		}

		// Notify that this table was mock "flushed".
		flushed <- struct{}{}

		insertCalled = true

		return
	}

	go s.Start()
	defer s.Stop()

	// Test initial insert.
	select {
	case <-flushed:
	case <-time.After(1 * time.Second):
		assert.Fail("initial insert didn't happened fast enough")

	}

	// Test insertTable() is NOT called again
	// after all rows were rejected on first insert.
	select {
	case <-flushed:
		assert.Fail("retry insert happened even though all rows were rejected")
	case <-time.After(1 * time.Second):
	}

	// Check that an error was logged for "all rows were rejected, moving on".
	select {
	case err, ok := <-s.Errors:
		assert.True(ok, "Error channel is closed")
		assert.IsType(&RowError{}, err)
		assert.Equal(`p.d.t.row[0]: invalid in l00: m00: {"k0":"v0"}`, err.Error())
	default:
		assert.Fail("Error \"row[0]\" is missing from error channel")
	}
	select {
	case err, ok := <-s.Errors:
		assert.True(ok, "Error channel is closed")
		assert.IsType(&RowError{}, err)
		assert.Equal(`p.d.t.row[1]: invalid in l10: m10: {"k1":"v1"}`, err.Error())
	default:
		assert.Fail("Error \"row[1]\" is missing from error channel")
	}
	select {
	case err, ok := <-s.Errors:
		assert.True(ok, "Error channel is closed")
		assert.IsType(&RowError{}, err)
		assert.Equal(`p.d.t.row[2]: invalid in l20: m20: {"k2":"v2"}`, err.Error())
	default:
		assert.Fail("Error \"row[2]\" is missing from error channel")
	}
	select {
	case err, ok := <-s.Errors:
		assert.True(ok, "Error channel is closed")
		assert.IsType(&RowError{}, err)
		assert.Equal(`p.d.t.row[3]: invalid in l30: m30: {"k3":"v3"}`, err.Error())
	default:
		assert.Fail("Error \"row[3]\" is missing from error channel")
	}
	select {
	case err, ok := <-s.Errors:
		assert.True(ok, "Error channel is closed")
		assert.IsType(&RowError{}, err)
		assert.Equal(`p.d.t.row[4]: invalid in l40: m40: {"k4":"v4"}`, err.Error())
	default:
		assert.Fail("Error \"row[4]\" is missing from error channel")
	}

	// Last error should indicate all rows have been rejected,
	// causing the insert to not be retried.
	select {
	case err, ok := <-s.Errors:
		assert.True(ok, "Error channel is closed")
		assert.IsType(&AllRowsRejectedError{}, err)
		assert.Equal(`All rows from p.d.t have been rejected, moving on`, err.Error())
	default:
		assert.Fail("Error \"all rows rejected\" is missing from error channel")
	}

	assert.Equal(0, len(s.Errors))
}

// TestInsertAllLogErrors inserts 5 rows and mocks 3 response errors for 3 of these rows,
// then tests whether these errors were forwarded to error channel.
// This function is very similar in structure to TestInsertAll().
func TestInsertAllLogErrors(t *testing.T) {
	t.Parallel()

	assert := assert.New(t)
	require := require.New(t)

	// Set a row threshold to 5 so it will flush immediately on calling Start().
	s, err := NewStreamer(nil, 5, 1*time.Minute, 1*time.Second, 10)
	require.NoError(err)

	// Mock insertTable function to return mocked errors and notify table was mock "flushed".
	// Will be used for testing errors were sent to error channel.
	flushed := make(chan struct{})

	insertCalled := false
	s.insertTable = func(pId, dId, tId string, tt table) (r *bigquery.TableDataInsertAllResponse, err error) {
		if !insertCalled {
			// Return response errors for line 1,3,5.
			// NOTE We're starting to count from 0 so that's actually 0,2,4.
			r = &bigquery.TableDataInsertAllResponse{
				Kind: "bigquery",
				InsertErrors: []*bigquery.TableDataInsertAllResponseInsertErrors{
					&bigquery.TableDataInsertAllResponseInsertErrors{
						Index: 0,
						Errors: []*bigquery.ErrorProto{
							&bigquery.ErrorProto{Location: "l11", Message: "m11", Reason: "r11"},
							&bigquery.ErrorProto{Location: "l12", Message: "m12", Reason: "r12"},
						},
					},
					&bigquery.TableDataInsertAllResponseInsertErrors{
						Index: 2,
						Errors: []*bigquery.ErrorProto{
							&bigquery.ErrorProto{Location: "l3", Message: "m3", Reason: "r3"},
						},
					},
					&bigquery.TableDataInsertAllResponseInsertErrors{
						Index: 4,
						Errors: []*bigquery.ErrorProto{
							&bigquery.ErrorProto{Location: "l5", Message: "m5", Reason: "r5"},
						},
					},
				},
			}
		} else {
			r = &bigquery.TableDataInsertAllResponse{}
		}

		// Notify that this table was mock "flushed".
		flushed <- struct{}{}

		insertCalled = true

		return
	}

	// Queue 5 rows to the same table.
	for i := 0; i < 5; i++ {
		k := fmt.Sprintf("k%d", i)
		v := fmt.Sprintf("v%d", i)
		s.QueueRow("p1", "d1", "t1", map[string]bigquery.JsonValue{k: v})
	}

	// Start streamer and wait for flush to happen.
	// Fail if flush takes too long.
	go s.Start()

	// First loop is for initial insert (with error response).
	// Second loop is for no errors (successul), so no retry insert would happen.
	for i := 0; i < 2; i++ {
		select {
		case <-flushed:
		case <-time.After(1 * time.Second):
			assert.Fail(fmt.Sprintf("insert %d wasn't called fast enough", i))
		}
	}

	s.Stop()

	// Test 4 errors were fetched: 2 for row 1, and each for row 3, 5.
	select {
	case err, ok := <-s.Errors:
		assert.True(ok, "Error channel is closed")
		assert.IsType(&RowError{}, err)
		assert.Equal(`p1.d1.t1.row[0]: r11 in l11: m11: {"k0":"v0"}`, err.Error())
	default:
		assert.Fail("Error \"row[0]\" error 1 is missing from error channel")
	}
	select {
	case err, ok := <-s.Errors:
		assert.True(ok, "Error channel is closed")
		assert.IsType(&RowError{}, err)
		assert.Equal(`p1.d1.t1.row[0]: r12 in l12: m12: {"k0":"v0"}`, err.Error())
	default:
		assert.Fail("Error \"row[0]\" error 2 is missing from error channel")
	}
	select {
	case err, ok := <-s.Errors:
		assert.True(ok, "Error channel is closed")
		assert.IsType(&RowError{}, err)
		assert.Equal(`p1.d1.t1.row[2]: r3 in l3: m3: {"k2":"v2"}`, err.Error())
	default:
		assert.Fail("Error \"row[2]\" is missing from error channel")
	}
	select {
	case err, ok := <-s.Errors:
		assert.True(ok, "Error channel is closed")
		assert.IsType(&RowError{}, err)
		assert.Equal(`p1.d1.t1.row[4]: r5 in l5: m5: {"k4":"v4"}`, err.Error())
	default:
		assert.Fail("Error \"row[4]\" is missing from error channel")
	}

	assert.Equal(0, len(s.Errors))
}
