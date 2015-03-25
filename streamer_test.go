package bqstreamer

import (
	"fmt"
	"testing"
	"time"

	bigquery "google.golang.org/api/bigquery/v2"
	"google.golang.org/api/googleapi"
)

// TestNewStreamer tests creating a new Streamer.
func TestNewStreamer(t *testing.T) {
	// Test sending bad arguments.
	s, err := NewStreamer(nil, 0, 1*time.Second, 1*time.Second, 10)
	if err == nil {
		t.Error("New streamer with 0 max rows should've failed")
	}

	s, err = NewStreamer(nil, -1, 1*time.Second, 1*time.Second, 10)
	if err == nil {
		t.Error("New streamer with -1 max rows should've failed")
	}

	s, err = NewStreamer(nil, 10, 0, 1*time.Second, 10)
	if err == nil {
		t.Error("New streamer with 0 max delay should've failed")
	}

	s, err = NewStreamer(nil, 10, -1*time.Nanosecond, 1*time.Second, 10)
	if err == nil {
		t.Error("New streamer with -1 nanosecond max delay should've failed")
	}

	s, err = NewStreamer(nil, 10, 1*time.Nanosecond, -1*time.Second, 10)
	if err == nil {
		t.Error("New multi-streamer with -1 second sleep before retry should've failed")
	}

	s, err = NewStreamer(nil, 10, 1*time.Nanosecond, -1*time.Second, -1)
	if err == nil {
		t.Error("New multi-streamer with -1 max retry insert sleep should've failed")
	}

	// Test sending valid arguments.
	s, err = NewStreamer(nil, 10, 1*time.Second, 1*time.Second, 10)
	if err != nil {
		t.Error(err)
	}

	if s.rowChannel == nil {
		t.Error("Row channel is nil")
	}
	if len(s.rowChannel) != 0 {
		t.Error("Row channel isn't empty")
	}
	if cap(s.rowChannel) != 10 {
		t.Errorf("cap(rowChannel) != 10 (is %d)", len(s.rowChannel))
	}

	if s.rows == nil {
		t.Error("rows is nil")
	}

	if s.rowIndex != 0 {
		t.Errorf("rowIndex != 0 (is %d)", s.rowIndex)
	}

	if s.maxDelay != 1*time.Second {
		t.Errorf("maxDelay != 1 second (is %.2f seconds)", s.maxDelay.Seconds())
	}

	if s.sleepBeforeRetry != 1*time.Second {
		t.Errorf("sleepBeforeRetry != 1 second (is %.2f seconds)", s.sleepBeforeRetry.Seconds())
	}

	if s.maxRetryInsert != 10 {
		t.Errorf("maxRetryInsert != 10 (is %d)", s.maxRetryInsert)
	}

	if s.stopChannel == nil {
		t.Error("Stop channel is nil")
	}
	if len(s.stopChannel) != 0 {
		t.Error("Stop channel isn't empty")
	}

	if s.Errors == nil {
		t.Error("Error channel is nil")
	}
	if len(s.Errors) != 0 {
		t.Error("Error channel isn't empty")
	}

	if s.Start == nil {
		t.Error("s.Start() is nil")
	}

	if s.Stop == nil {
		t.Error("s.Stop() is nil")
	}

	if s.flush == nil {
		t.Error("s.flush() is nil")
	}

	if s.insertAll == nil {
		t.Error("s.insretAll() is nil")
	}

	if s.insertTable == nil {
		t.Error("s.insretTable() is nil")
	}
}

// TestSTop calls Stop(), starts a streamer, and checks if it immidiately stopped.
func TestStop(t *testing.T) {
	// Set delay threshold to be large enough so we could test if stop message
	// caused streamer to stop and flush.
	s, err := NewStreamer(nil, 100, 1*time.Minute, 1*time.Second, 10)
	if err != nil {
		t.Fatal(err)
	}

	// Override flush function to just send a "flushed" notification,
	// instead of actually sending to BigQuery.
	flushed := make(chan bool)
	s.flush = func() {
		flushed <- true
	}

	go s.Stop()

	if stop := <-s.stopChannel; !stop {
		t.Error("No shutdown message passed in shutdown channel upon calling Stop()")
	}

	// Test if streamer flushes quickly after a stop signal is sent.
	go s.Stop()
	timer := time.NewTimer(1 * time.Second)
	go s.Start()
	select {
	case <-flushed:
	case <-timer.C:
		t.Error("flush() wasn't called fast enough")
	}
}

// TestQueueRow tests queueing a row.
func TestQueueRow(t *testing.T) {
	s, err := NewStreamer(nil, 10, 1*time.Second, 1*time.Second, 10)
	if err != nil {
		t.Fatal(err)
	}

	data := map[string]bigquery.JsonValue{"test_key": "test_value"}
	s.QueueRow("p", "d", "t", data)

	q := <-s.rowChannel

	if q.projectID != "p" {
		t.Errorf("Row's projectID %s is different than expected %s", q.projectID, "p")
	}

	if q.datasetID != "d" {
		t.Errorf("Row's datasetID %s is different than expected %s", q.datasetID, "d")
	}

	if q.tableID != "t" {
		t.Errorf("Row's tableID %s is different than expected %s", q.tableID, "t")
	}

	if v, ok := q.data["test_key"]; !ok {
		t.Error("Row's data key test_key non existent")
	} else if v != data["test_key"] {
		t.Errorf("Row's data value %s different than expected %s", v, data["test_key"])
	}
}

// TestMaxDelayFlushCall tests streamer is calling flush() to BigQuery when maxDelay
// timer expires.
func TestMaxDelayFlushCall(t *testing.T) {
	// Set maxRows = 10 and insert a single line, so we can be sure flushing
	// occured by delay timer expiring.
	s, err := NewStreamer(nil, 10, 1*time.Second, 1*time.Second, 10)
	if err != nil {
		t.Fatal(err)
	}

	// Override flush function to just send a "flushed" notification,
	// instead of actually sending to BigQuery.
	flushed := make(chan bool)
	s.flush = func() {
		flushed <- true
	}

	data := map[string]bigquery.JsonValue{"test_key": "test_value"}
	r := row{"p", "d", "t", data}

	s.QueueRow(r.projectID, r.datasetID, r.tableID, r.data)

	// Start streamer and measure time it should take to flush by maxDelay.
	// Add a small interval to timer to avoid failing when our timer expired
	// just a moment before streamer's timer.
	timer := time.NewTimer(s.maxDelay + 1*time.Millisecond)
	go s.Start()

	// Fail if no flush happened in maxDelay time.
	select {
	case <-flushed:
	case <-timer.C:
		t.Errorf("flush() wasn't called in maxDelay time (%.2f seconds)", s.maxDelay.Seconds())
	}

	s.Stop()

	// Flushing doesn't actually flush in this test, so s.rows still holds what
	// we inserted.
	if s.rows[0].projectID != r.projectID {
		t.Errorf("Inserted row %s wasn't queued in rows (was %s)", r, *s.rows[0])
	}
}

// TestMaxRowsFlushCal tests streamer is calling flush() to insert BigQuery when maxRows have
// been queued.
func TestMaxRowsFlushCall(t *testing.T) {
	// Set a long delay before flushing, so we can be sure flushing occured due
	// to rows filling up.
	s, err := NewStreamer(nil, 10, 1*time.Minute, 1*time.Second, 10)
	if err != nil {
		t.Fatal(err)
	}

	// Override flush function to just send a "flushed" notification,
	// instead of actually sending to BigQuery.
	flushed := make(chan bool)
	s.flush = func() {
		flushed <- true
	}

	data := map[string]bigquery.JsonValue{"test_key": "test_value"}
	r := row{"p", "d", "t", data}

	// Start streamer and measure time it should take to flush by maxRows.
	timer := time.NewTimer(1 * time.Microsecond)
	go s.Start()

	// Insert 10 rows to force flushing by maxRows.
	for i := 0; i <= 9; i++ {
		s.QueueRow(r.projectID, r.datasetID, r.tableID, r.data)
	}

	// Test if flushing happened almost immediately, forced by rows queue being filled.
	select {
	case <-flushed:
	case <-timer.C:
		t.Error("flush() wasn't called by rows queue getting filled")
	}

	s.Stop()

	// Flushing doesn't actually flush in this test, so s.rows still holds what
	// we inserted.
	for i := 0; i <= 9; i++ {
		if s.rows[i].projectID != r.projectID {
			t.Errorf("Inserted row %s wasn't queued in rows (was %s)", r, *s.rows[i])
		}
	}
}

// TestInsertAll queues 20 rows to 4 tables, 5 to each row,
// and tests if rows were inserted to a mock project-dataset-table-rows type.
func TestInsertAll(t *testing.T) {
	// We intend to insert 20 rows in this test, so set maxRows = 20 and delay
	// to be long enough so flush will occur due to rows queue filling up.
	s, err := NewStreamer(nil, 20, 1*time.Minute, 1*time.Second, 10)
	if err != nil {
		t.Fatal(err)
	}

	// Mock insertTable function to cache all rows from all tables to a local variable.
	// Will be used for testing which rows were actually "inserted".
	ps := map[string]project{}
	flushed := make(chan bool)

	s.insertTable = func(pId, dId, tId string, tt table) (r *bigquery.TableDataInsertAllResponse, err error) {
		// Add all table rows to a local projects-datasets-table map,
		// mocking rows that were inserted to BigQuery, which we will test against.
		for i, tr := range tt {
			if tr == nil {
				t.Errorf("%s.%s.%s.row[%d] is nil", pId, dId, tId, i)
			}

			// Mock "insert row" to table: Create project, dataset and table
			// if uninitalized.
			createTableIfNotExists(ps, pId, dId, tId)
			ps[pId][dId][tId] = append(ps[pId][dId][tId], tr)
		}

		// Return 0 insert errors.
		r = &bigquery.TableDataInsertAllResponse{}

		// Notify that this table was mock "flushed".
		flushed <- true

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
	timer := time.NewTimer(1 * time.Millisecond)
	go s.Start()
	for i := 0; i < 4; i++ {
		select {
		case <-flushed:
		case <-timer.C:
			t.Fatalf("flush() %d wasn't called fast enough", i)
		}
	}
	s.Stop()

	if s.rowIndex != 0 {
		t.Error("rowIndex not reset to 0 after all flushes were done")
	}

	// Check all queued projects, datasets, and tables were actually mock "inserted".
	if _, ok := ps["p1"]; !ok {
		t.Error("p1 project missing from inserted projects")
	}
	if _, ok := ps["p2"]; !ok {
		t.Error("p1 project missing from inserted projects")
	}
	if _, ok := ps["p1"]["d1"]; !ok {
		t.Error("p1.d1 dataset missing from inserted datasets")
	}
	if _, ok := ps["p1"]["d2"]; !ok {
		t.Error("p1.d2 dataset missing from inserted datasets")
	}
	if _, ok := ps["p1"]["d1"]["t1"]; !ok {
		t.Error("p1.d1.t1 table missing from inserted datasets")
	}
	if _, ok := ps["p1"]["d1"]["t2"]; !ok {
		t.Error("p1.d1.t2 table missing from inserted datasets")
	}

	// Check no more than 2 projects, 2 datasets, and 2 tables were mock "inserted".
	if len(ps) != 2 {
		t.Errorf("There were more than 2 projects inserted (were %d)", len(ps))
	}
	if len(ps["p1"]) != 2 {
		t.Errorf("There were more than 2 datasets inserted (were %d)", len(ps["p1"]))
	}
	if len(ps["p1"]["d1"]) != 2 {
		t.Errorf("There were more than 2 tables inserted (were %d)", len(ps["p1"]["d1"]))
	}

	// Check all rows from all tables were mock "inserted".
	checkRows := func(ps map[string]project, p, d, tt string, num int) {
		for i := 0; i < num; i++ {
			r := ps[p][d][tt][i]
			if r == nil {
				t.Errorf("Row %d missing from table %s.%s.%s", i, p, d, tt)
			}

			if v, ok := r.jsonValue[fmt.Sprintf("k%d", i)]; !ok {
				t.Errorf("Key k%d missing from %s.%s.%s.row[%d] (row is %s)", i, p, d, tt, i, r)
			} else if v != fmt.Sprintf("v%d", i) {
				t.Errorf("%s.%s.%s.row[%d] key's value is not v%d (is %s)", p, d, tt, i, i, v)
			}
		}

		if len(ps[p][d][tt]) > num {
			t.Errorf("There are more rows in %s.%s.%s than expected (there are %d)", p, d, tt, len(ps[p][d][tt]))
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
	s, err := NewStreamer(nil, 5, 1*time.Minute, 1*time.Second, 10)
	if err != nil {
		t.Fatal(err)
	}

	// Test whether a GoogleAPI HTTP 503 error returns true.
	// i.e. the function decides the request related to this error should be retried.
	if !s.shouldRetryInsertAfterError(
		&googleapi.Error{Code: 503, Message: "m", Body: "b", Errors: []googleapi.ErrorItem{
			googleapi.ErrorItem{Reason: "r1", Message: "m1"},
			googleapi.ErrorItem{Reason: "r2", Message: "m2"}}}) {
		t.Error("GoogleAPI HTTP 503 insert error wasn't marked as \"to be retried\"")
	}

	// Test whether errors above were logged.
	select {
	case err = <-s.Errors:
		if gerr, ok := err.(*googleapi.Error); !ok {
			t.Error("Logged error after GoogleAPI HTTP 503 insert error wasn't a GoogleAPI error")
		} else {
			if gerr.Code != 503 {
				t.Error("Logged GoogleAPI HTTP 503 error code != 503")
			}
			if gerr.Message != "m" {
				t.Error("Logged GoogleAPI HTTP 503 error code != m")
			}
			if gerr.Body != "b" {
				t.Error("Logged GoogleAPI HTTP 503 error code != b")
			}

			if len(gerr.Errors) != 2 {
				t.Errorf("Logged GoogleAPI HTTP 503 error code errors length != 2, is: %d", len(gerr.Errors))
			} else {
				if gerr.Errors[0].Reason != "r1" {
					t.Error("Logged GoogleAPI HTTP 503 error code 1st error item reason != r1")
				}
				if gerr.Errors[0].Message != "m1" {
					t.Error("Logged GoogleAPI HTTP 503 error code 1st error item message != m1")
				}
				if gerr.Errors[1].Reason != "r2" {
					t.Error("Logged GoogleAPI HTTP 503 error code 2st error item reason != r2")
				}
				if gerr.Errors[1].Message != "m2" {
					t.Error("Logged GoogleAPI HTTP 503 error code 2st error item message != m2")
				}
			}
		}
	default:
		t.Error("Logged error after GoogleAPI 503 insert error is missing")
	}

	// Test another GoogleAPI response with a different status code,
	// and check the function returns false.
	if s.shouldRetryInsertAfterError(
		&googleapi.Error{Code: 501, Message: "m", Body: "b", Errors: []googleapi.ErrorItem{
			googleapi.ErrorItem{Reason: "r1", Message: "m1"},
			googleapi.ErrorItem{Reason: "r2", Message: "m2"}}}) {
		t.Error("GoogleAPI HTTP 500 insert error was marked as \"to be retried\"")
	}

	// Test whether errors above were logged.
	select {
	case err = <-s.Errors:
		if gerr, ok := err.(*googleapi.Error); !ok {
			t.Error("Logged error after GoogleAPI HTTP 503 insert error wasn't a GoogleAPI error")
		} else {
			if gerr.Code != 501 {
				t.Error("Logged GoogleAPI HTTP 503 error code != 503")
			}
			if gerr.Message != "m" {
				t.Error("Logged GoogleAPI HTTP 503 error code != m")
			}
			if gerr.Body != "b" {
				t.Error("Logged GoogleAPI HTTP 503 error code != b")
			}

			if len(gerr.Errors) != 2 {
				t.Errorf("Logged GoogleAPI HTTP 503 error code errors length != 2, is: %d", len(gerr.Errors))
			} else {
				if gerr.Errors[0].Reason != "r1" {
					t.Error("Logged GoogleAPI HTTP 503 error code 1st error item reason != r1")
				}
				if gerr.Errors[0].Message != "m1" {
					t.Error("Logged GoogleAPI HTTP 503 error code 1st error item message != m1")
				}
				if gerr.Errors[1].Reason != "r2" {
					t.Error("Logged GoogleAPI HTTP 503 error code 2st error item reason != r2")
				}
				if gerr.Errors[1].Message != "m2" {
					t.Error("Logged GoogleAPI HTTP 503 error code 2st error item message != m2")
				}
			}
		}
	default:
		t.Error("Logged error after GoogleAPI 500 insert error is missing")
	}

	// Test a non-GoogleAPI error, but a a generic error instead.
	if s.shouldRetryInsertAfterError(fmt.Errorf("Non-GoogleAPI error")) {
		t.Error("Non-GoogleAPI error was marked as \"to be retried\"")
	}

	// Test whether the error above was logged.
	select {
	case err = <-s.Errors:
		if _, ok := err.(*googleapi.Error); ok {
			t.Error("Logged error after Non-GoogleAPI error is of *googleapi.Error type")
		} else if err.Error() != "Non-GoogleAPI error" {
			t.Error("Logged non-GoogleAPI error != \"Non-GoogleAPI error\"")
		}
	default:
		t.Error("Logged error after Non-GoogleAPI error is missing")
	}
}

// TestRemoveRowsFromTable inserts rows, then removes a part of them.
// It then checks whether the correct rows where indeed removed.
func TestRemoveRowsFromTable(t *testing.T) {
	// Make maxRows bigger (10) than the amount of rows we're going to insert
	// in this test (5), so flushing won't happen because of this.
	// Also, make maxDelay longer than the amount of time this test is going to take,
	// for the same reason.
	s, err := NewStreamer(nil, 10, 1*time.Minute, 1*time.Second, 10)
	if err != nil {
		t.Fatal(err)
	}

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
	if val, ok := filtered[0].jsonValue["k12"]; !ok {
		t.Errorf("Key k12 is not in row filtered[0]: %s", filtered)
	} else if val != "v12" {
		t.Errorf("row filtered[0][k12] != v12 is: %s", filtered[0].jsonValue["k12"])
	}

	if val, ok := filtered[1].jsonValue["k11"]; !ok {
		t.Errorf("Key k11 is not in row filtered[1]: %s", filtered)
	} else if val != "v11" {
		t.Errorf("row filtered[1][k11] != v11 is: %s", filtered[1].jsonValue["k11"])
	}
}

// TestFilterRejectedRows inserts rows and mocks a rejected rows response.
// It then tests whether correct rows were removed and if the right removed
// indexes were returned by filterRejectedRows()
func TestFilterRejectedRows(t *testing.T) {
	// Make maxRows bigger (10) than the amount of rows we're going to insert
	// in this test (5), so flushing won't happen because of this.
	// Also, make maxDelay longer than the amount of time this test is going to take,
	// for the same reason.
	s, err := NewStreamer(nil, 10, 1*time.Minute, 1*time.Second, 10)
	if err != nil {
		t.Fatal(err)
	}

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

	if len(rejected1) != 3 {
		t.Errorf("len() != 3 for insert 1 rejected indexes, is: %d", len(rejected1))
	} else {
		// Test the rejected rows' indexes is correct.
		for i, e := range []int64{4, 2, 0} {
			if rejected1[i] == e {
				t.Errorf("rejected1[%d] != %d, is: %d", i, e, rejected1[i])
			}
		}

		// This call's insert request should only include rows 2, 3 in that order.
		// This is because row 2 was marked as "stopped",
		// and row 3 wasn't marked by any error at all.
		//
		// So, test t1 remaining rows are as mentioned.
		if len(d["t1"]) != 2 {
			t.Errorf("len(t1) != 2, is: %d", len(d["t1"]))
		} else {
			if val, ok := d["t1"][0].jsonValue["k12"]; !ok {
				t.Errorf("Key k12 is not in row d[t1][0]: %s", d["t1"])
			} else if val != "v12" {
				t.Errorf("row d[t1][0][k12] != v12 is: %s", d["t1"][0].jsonValue["k12"])
			}

			if val, ok := d["t1"][1].jsonValue["k13"]; !ok {
				t.Errorf("Key k13 is not in row d[t1][1]: %s", d["t1"])
			} else if val != "v13" {
				t.Errorf("row d[t1][1][k13] != v13 is: %s", d["t1"][1].jsonValue["k13"])
			}
		}
	}

	if len(rejected2) != 0 {
		t.Errorf("len() != 0 for insert 2 rejected indexes, is: %d", len(rejected2))
	}

	// Test t2 rows weren't changed at all.
	for i := 0; i < len(d["t2"]); i++ {
		k := fmt.Sprintf("k2%d", i)
		v := fmt.Sprintf("v2%d", i)
		if val, ok := d["t2"][i].jsonValue[k]; !ok {
			t.Errorf("Key %s is not in row d[t2][%d]: %s", k, i, d["t2"])
		} else if val != v {
			t.Errorf("row d[t2][%d][%s] != v is: %s", i, k, d["t2"][i].jsonValue["k"])
		}
	}
}

// TestInsertAllWithServerErrorResponse tests if an insert failed with a server
// error (500, 503) triggers a retry insert.
func TestInsertAllWithServerErrorResponse(t *testing.T) {
	// Set a row threshold to 5 so it will flush immediately on calling Start().
	// Also make retry sleep delay as small as possible and != 0.
	s, err := NewStreamer(nil, 5, 1*time.Minute, 1*time.Nanosecond, 10)
	if err != nil {
		t.Fatal(err)
	}

	// Queue 5 rows to the same table.
	for i := 0; i < 5; i++ {
		k := fmt.Sprintf("k%d", i)
		v := fmt.Sprintf("v%d", i)
		s.QueueRow("p", "d", "t", map[string]bigquery.JsonValue{k: v})
	}

	// Mock insertTable function to return mocked errors and notify table was mock "flushed".
	// Also count the times insertTable() is called to make sure it's retried exactly once.
	flushed := make(chan bool)
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
			if len(tt) != 5 {
				t.Error("len(tt) != 5")
			} else {
				for i := 0; i < 5; i++ {
					k := fmt.Sprintf("k%d", i)
					v := fmt.Sprintf("v%d", i)
					if val, ok := tt[i].jsonValue[k]; !ok {
						t.Errorf("Key %s is not in row tt[%d]: %s", k, i, tt)
					} else if val != v {
						t.Errorf("row tt[%d][%s] != %s is: %s", i, k, v, tt[i].jsonValue[k])
					}
				}
			}
		default:
			t.Error("insertTable() was called more than 2 times")
		}

		// Notify that this table was mock "flushed".
		flushed <- true

		calledNum++

		return
	}

	// Start streamer and wait for flush to happen.
	// Fail if flush takes too long.
	go s.Start()

	// Test HTTP 503 server error response triggered a retry.
	timer := time.NewTimer(1 * time.Microsecond)

	// First loop is for testing initial insert,
	// second is for testing retry insert happened.
	for i := 0; i < 2; i++ {
		select {
		case <-flushed:
		case <-timer.C:
			t.Errorf("insert %d didn't happen fast enough", i)
		}
		// FIXME for some reason a lot of time is necessary for this iteration to finish.
		timer.Reset(1 * time.Second)
	}

	// Make sure insertTable() wasn't called again i.e. no retry happened.
	// This is done by waiting for a fair amount of time
	// and testing no flush happened.
	timer.Reset(1 * time.Second)
	select {
	case <-flushed:
		t.Error("retry insert happened another time after test was finished")
	case <-timer.C:
	}

	s.Stop()
}

// TestInsertAllWithNonServerErrorResponse tests if an insert failed with an error
// which is NOT server error (503) does NOT trigger a retry insert.
func TestInsertAllWithNonServerErrorResponse(t *testing.T) {
	// Set a row threshold to 5 so it will flush immediately on calling Start().
	// Also make retry sleep delay as small as possible and != 0.
	s, err := NewStreamer(nil, 5, 1*time.Minute, 1*time.Nanosecond, 10)
	if err != nil {
		t.Fatal(err)
	}

	// Queue 5 rows to the same table.
	for i := 0; i < 5; i++ {
		k := fmt.Sprintf("k%d", i)
		v := fmt.Sprintf("v%d", i)
		s.QueueRow("p", "d", "t", map[string]bigquery.JsonValue{k: v})
	}

	// Mock insertTable function to return mocked errors and notify table was mock "flushed".
	// Also count the times insertTable() is called to make sure it's retried exactly once.
	flushed := make(chan bool)
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
			t.Error("insertTable() was called more than once")
		}

		// Notify that this table was mock "flushed".
		flushed <- true

		calledNum++

		return
	}

	go s.Start()

	// Test initial insert.
	timer := time.NewTimer(1 * time.Microsecond)
	select {
	case <-flushed:
	case <-timer.C:
		t.Errorf("initial insert didn't happen fast enough")
	}

	// Make sure insertTable() wasn't called again i.e. no retry happened.
	timer.Reset(1 * time.Second)
	select {
	case <-flushed:
		t.Error("retry insert happened even though server error != 503")
	case <-timer.C:
	}

	s.Stop()
}

// TestMaxRetryInsert tests if a repeatedly failing insert attempt
// (failing with non-rejected rows errors) is eventually dropped and ignored,
// and streamer is moving on to the next table insert.
func TestMaxRetryInsert(t *testing.T) {
	// Set a row threshold to 5 so it will flush immediately on calling Start().
	// Also make retry sleep delay as small as possible and != 0.
	s, err := NewStreamer(nil, 5, 1*time.Minute, 1*time.Nanosecond, 3)
	if err != nil {
		t.Fatal(err)
	}

	// Queue 5 rows to the same table.
	for i := 0; i < 5; i++ {
		k := fmt.Sprintf("k%d", i)
		v := fmt.Sprintf("v%d", i)
		s.QueueRow("p", "d", "t", map[string]bigquery.JsonValue{k: v})
	}

	// Mock insertTable function to return mocked errors and notify table was mock "flushed".
	// Also count the times insertTable() is called to make sure it's retried exactly 3 times.
	insertCalled := make(chan bool)
	calledNum := 0
	s.insertTable = func(pId, dId, tId string, tt table) (r *bigquery.TableDataInsertAllResponse, err error) {
		// Return Google API HTTP 503 error on every call, which should be retried and logged.
		// Thus, we should expect 3 logged errors.
		if calledNum < 3 {
			err = &googleapi.Error{
				Code:    503,
				Message: "m",
				Body:    "b",
				Errors: []googleapi.ErrorItem{
					googleapi.ErrorItem{Reason: "r1", Message: "m1"},
					googleapi.ErrorItem{Reason: "r2", Message: "m2"}}}
		} else {
			t.Error("insertTable() was called more than 3 times")
		}

		// Notify that this table was mock "flushed".
		insertCalled <- true

		calledNum++

		return
	}

	// Start streamer and wait for flush to happen.
	// Fail if flush takes too long.
	go s.Start()

	// Test each insert logged an error.
	//
	// FIXME for some reason a lof time is necessary for this test to finish successfully.
	// Otherwise timer expires before insert table is called.
	// See other FIXME at the end of this block.
	timer := time.NewTimer(1 * time.Second)
	for i := 0; i < 3; i++ {
		select {
		case <-timer.C:
			t.Errorf("insert %d didn't happen fast enough", i)
		case <-insertCalled:
			// Wait a bit for the current insert table iteration to check for
			// errorrs and decide whether to retry or not.
			time.Sleep(1 * time.Millisecond)

			// Check that on each insert, an error was logged.
			// This should happen except for the 3rd insert,
			// which returns an HTTP 503.
			select {
			case err, ok := <-s.Errors:
				if !ok {
					t.Fatal("Error channel is closed")
				}
				t.Log("Mocked errors (this is ok):", err)
			default:
				t.Errorf("Error %d is missing from error channel", i)
			}
		}

		// FIXME for some reason a lot of time is necessary for this iteration to finish.
		timer.Reset(1 * time.Second)
	}

	// Make sure insertTable() wasn't called again i.e. no retry happened.
	// This is done by waiting for a fair amount of time
	// and testing no flush happened.
	timer.Reset(1 * time.Second)
	select {
	case <-insertCalled:
		t.Error("retry insert happened for the 4th time after test was finished")
	case <-timer.C:
	}

	// Test "too many retries, dropping insert" error was logged.
	select {
	case err, ok := <-s.Errors:
		if !ok {
			t.Fatal("Error channel is closed")
		}
		t.Log("Mocked errors (this is ok):", err)
	default:
		t.Error("Error \"too many retries\" is missing from error channel")
	}

	s.Stop()
}

// TestInsertAllWithRejectedResponse tests if an insert failed with rejected
// rows triggers a retry insert only with non-rejected rows.
func TestInsertAllWithRejectedResponse(t *testing.T) {
	// Set a row threshold to 5 so it will flush immediately on calling Start().
	// Also make retry sleep delay as small as possible and != 0.
	s, err := NewStreamer(nil, 5, 1*time.Minute, 1*time.Nanosecond, 10)
	if err != nil {
		t.Fatal(err)
	}

	// Queue 5 rows to the same table.
	for i := 0; i < 5; i++ {
		k := fmt.Sprintf("k%d", i)
		v := fmt.Sprintf("v%d", i)
		s.QueueRow("p", "d", "t", map[string]bigquery.JsonValue{k: v})
	}

	// Mock insertTable function to return mocked errors and notify table was mock "flushed".
	// Also count the times insertTable() is called to make sure it's retried exactly once.
	flushed := make(chan bool)
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
			if len(tt) != 3 {
				t.Errorf("len(tt) != 3, is: %s", tt)
			} else {
				if val, ok := tt[0].jsonValue["k3"]; !ok {
					t.Errorf("Key k3 is not in row tt[0]: %s", tt)
				} else if val != "v3" {
					t.Errorf("row tt[0][k3] != v3 is: %s", tt[0].jsonValue["k3"])
				}

				if val, ok := tt[1].jsonValue["k4"]; !ok {
					t.Errorf("Key k4 is not in row tt[1]: %s", tt)
				} else if val != "v4" {
					t.Errorf("row tt[1][k4] != v4 is: %s", tt[1].jsonValue["k4"])
				}

				if val, ok := tt[2].jsonValue["k2"]; !ok {
					t.Errorf("Key k2 is not in row tt[2]: %s", tt)
				} else if val != "v2" {
					t.Errorf("row tt[2][k2] != v2 is: %s", tt[2].jsonValue["k2"])
				}
			}

			// Return 0 insert errors.
			r = &bigquery.TableDataInsertAllResponse{}
		default:
			t.Error("insertTable() was called more than 3 times")
		}

		// Notify that this table was mock "flushed".
		flushed <- true

		calledNum++

		return
	}

	go s.Start()

	// Test initial insert.
	timer := time.NewTimer(1 * time.Microsecond)
	select {
	case <-flushed:
	case <-timer.C:
		t.Error("initial insert didn't happened fast enough")

	}

	// Test insertTable() is called again after rejected rows response.
	timer.Reset(1 * time.Microsecond)
	select {
	case <-flushed:
	case <-timer.C:
		t.Error("retry insert didn't happen fast enough after rejected rows response")
	}

	// Make sure insertTable() wasn't called again i.e. no retry happened.
	timer.Reset(1 * time.Second)
	select {
	case <-flushed:
		t.Error("retry insert happened even though no rows were rejected")
	case <-timer.C:
	}

	s.Stop()
}

// TestInsertwithAllRowsRejected tests if an insert failed with all rows
// rejected doesn't trigger a retry insert.
func TestInsertwithAllRowsRejected(t *testing.T) {
	// Set a row threshold to 5 so it will flush immediately on calling Start().
	// Also make retry sleep delay as small as possible and != 0.
	s, err := NewStreamer(nil, 5, 1*time.Minute, 1*time.Nanosecond, 10)
	if err != nil {
		t.Fatal(err)
	}

	// Queue 5 rows to the same table.
	for i := 0; i < 5; i++ {
		k := fmt.Sprintf("k%d", i)
		v := fmt.Sprintf("v%d", i)
		s.QueueRow("p", "d", "t", map[string]bigquery.JsonValue{k: v})
	}

	// Mock insertTable function to return mocked errors and notify table was mock "flushed".
	// Also count the times insertTable() is called to make sure it's retried exactly once.
	flushed := make(chan bool)
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
			t.Error("insertTable() was called more than 2 times")
		}

		// Notify that this table was mock "flushed".
		flushed <- true

		insertCalled = true

		return
	}

	go s.Start()

	// Test initial insert.
	timer := time.NewTimer(1 * time.Microsecond)
	select {
	case <-flushed:
	case <-timer.C:
		t.Error("initial insert didn't happened fast enough")

	}

	// Test insertTable() is NOT called again
	// after all rows were rejected on first insert.
	timer.Reset(1 * time.Second)
	select {
	case <-flushed:
		t.Error("retry insert happened even though all rows were rejected")
	case <-timer.C:
	}

	// Check that an error was logged for "all rows were rejected, moving on".
	select {
	case err, ok := <-s.Errors:
		if !ok {
			t.Fatal("Error channel is closed")
		}
		t.Log("Mocked errors (this is ok):", err)
	default:
		t.Errorf("Error \"all rows rejected\" is missing from error channel")
	}

	s.Stop()
}

// TestInsertAllLogErrors inserts 5 rows and mocks 3 response errors for 3 of these rows,
// then tests whether these errors were forwarded to error channel.
// This function is very similar in structure to TestInsertAll().
func TestInsertAllLogErrors(t *testing.T) {
	// Set a row threshold to 5 so it will flush immediately on calling Start().
	s, err := NewStreamer(nil, 5, 1*time.Minute, 1*time.Second, 10)
	if err != nil {
		t.Fatal(err)
	}

	// Mock insertTable function to return mocked errors and notify table was mock "flushed".
	// Will be used for testing errors were sent to error channel.
	flushed := make(chan bool)

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
		flushed <- true

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
	timer := time.NewTimer(1 * time.Microsecond)
	go s.Start()

	// First loop is for initial insert (with error response).
	// Second loop is for no errors (successul), so no retry insert would happen.
	for i := 0; i < 2; i++ {
		select {
		case <-flushed:
		case <-timer.C:
			t.Errorf("insert %d wasn't called fast enough", i)
		}
		timer.Reset(1 * time.Microsecond)
	}

	s.Stop()

	// Test 4 errors were fetched: 2 for row 1, and each for row 3, 5.
	for i := 0; i < 4; i++ {
		select {
		case err, ok := <-s.Errors:
			if !ok {
				t.Fatal("Error channel is closed")
			}

			t.Log("Mocked errors (this is ok):", err)
		default:
			t.Errorf("Error %d is missing from error channel", i)
		}
	}
}
