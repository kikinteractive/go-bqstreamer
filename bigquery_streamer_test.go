package main

import (
	"fmt"
	"testing"
	"time"

	bigquery "google.golang.org/api/bigquery/v2"
)

// TestNewBigQueryStreamer tests creating a new BigQueryStreamer.
func TestNewBigQueryStreamer(t *testing.T) {
	// Test sending bad arguments.
	s, err := NewBigQueryStreamer(nil, 0, 1*time.Second)
	if err == nil {
		t.Error("New streamer with 0 max rows should've failed")
	}

	s, err = NewBigQueryStreamer(nil, -1, 1*time.Second)
	if err == nil {
		t.Error("New streamer with -1 max rows should've failed")
	}

	s, err = NewBigQueryStreamer(nil, 10, 0)
	if err == nil {
		t.Error("New streamer with 0 max delay should've failed")
	}

	s, err = NewBigQueryStreamer(nil, 10, -1*time.Nanosecond)
	if err == nil {
		t.Error("New streamer with -1 nanosecond max delay should've failed")
	}

	// Test sending valid arguments.
	s, err = NewBigQueryStreamer(nil, 10, 1*time.Second)
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
		t.Errorf("MaxDelay != 1 second (is %.2f seconds)", s.maxDelay.Seconds())
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
	s, err := NewBigQueryStreamer(nil, 100, 1*time.Minute)
	if err != nil {
		t.Error(err)
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
	s, err := NewBigQueryStreamer(nil, 10, 1*time.Second)
	if err != nil {
		t.Error(err)
	}

	data := map[string]bigquery.JsonValue{"test_key": "test_value"}
	r := row{"p", "d", "t", data}

	go s.QueueRow(r.projectID, r.datasetID, r.tableID, r.data)

	q := <-s.rowChannel

	if q.projectID != r.projectID {
		t.Errorf("Row's projectID %s is different than expected %s", q.projectID, r.projectID)
	}

	if q.datasetID != r.datasetID {
		t.Errorf("Row's datasetID %s is different than expected %s", q.datasetID, r.datasetID)
	}

	if q.tableID != r.tableID {
		t.Errorf("Row's tableID %s is different than expected %s", q.tableID, r.tableID)
	}

	if v, ok := q.data["test_key"]; !ok {
		t.Error("Row's data key test_key non existent")
	} else if v != r.data["test_key"] {
		t.Errorf("Row's data value %s different than expected %s", v, r.data["test_key"])
	}
}

// TestMaxDelayFlushCall tests streamer is calling flush() to BigQuery when maxDelay
// timer expires.
func TestMaxDelayFlushCall(t *testing.T) {
	// Set maxRows = 10 and insert a single line, so we can be sure flushing
	// occured by delay timer expiring.
	s, err := NewBigQueryStreamer(nil, 10, 1*time.Second)
	if err != nil {
		t.Error(err)
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
	s, err := NewBigQueryStreamer(nil, 10, 1*time.Minute)
	if err != nil {
		t.Error(err)
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
	s, err := NewBigQueryStreamer(nil, 20, 1*time.Minute)
	if err != nil {
		t.Error(err)
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

			if v, ok := r[fmt.Sprintf("k%d", i)]; !ok {
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

// TestInsertAllErrors inserts 5 rows and mocks 3 response errors for 3 of these rows,
// then tests whether these errors were forwarded to error channel.
// This function is very similar in structure to TestInsertAll().
func TestInsertAllErrors(t *testing.T) {
	// Set a row threshold to 5 so it will flush immediately on calling Start().
	s, err := NewBigQueryStreamer(nil, 5, 1*time.Minute)
	if err != nil {
		t.Error(err)
	}

	// Mock insertTable function to return mocked errors and notify table was mock "flushed".
	// Will be used for testing errors were sent to error channel.
	flushed := make(chan bool)

	s.insertTable = func(pId, dId, tId string, tt table) (r *bigquery.TableDataInsertAllResponse, err error) {
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

		// Notify that this table was mock "flushed".
		flushed <- true

		return
	}

	// Distribute 5 rows to to the same table.
	for i := 0; i < 5; i++ {
		k := fmt.Sprintf("k%d", i)
		v := fmt.Sprintf("v%d", i)
		s.QueueRow("p1", "d1", "t1", map[string]bigquery.JsonValue{k: v})
	}

	// Start streamer and wait for flush to happen.
	// Fail if flushes take too long.
	timer := time.NewTimer(1 * time.Microsecond)
	go s.Start()
	select {
	case <-flushed:
	case <-timer.C:
		t.Error("flush() wasn't called fast enough")
	}
	s.Stop()

	// Test 4 errors were fetched: 2 for row 1, and each for row 3, 5.
	for i := 0; i < 4; i++ {
		select {
		case err, ok := <-s.Errors:
			if !ok {
				t.Fatal("Error channel is closed")
			}

			t.Log("Mocked errors (this is ok): ", err)
		default:
			t.Errorf("Error %d is missing from error channel", i)
		}
	}
}
