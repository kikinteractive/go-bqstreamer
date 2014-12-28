package main

import (
	"fmt"
	"time"

	bigquery "google.golang.org/api/bigquery/v2"
)

// A BigQueryStreamer is a BigQuery streamer, queuing rows and streaming to
// BigQuery in bulk by calling InsertAll().
type BigQueryStreamer struct {
	// BigQuery client connection.
	service *bigquery.Service

	// Upon invokeing Start() streamer will fetch rows from this channel,
	// and queue it into an internal rows queue.
	rowChannel chan *row

	// Internal list to queue rows for stream insert.
	rows []*row

	// Rows index to queue next row into.
	rowIndex int // 0

	// Max delay between flushes to BigQuery.
	maxDelay time.Duration

	// Shutdown channel to stop Start() execution.
	stopChannel chan bool

	// Errors are reported to this channel.
	Errors chan error

	// The following functions can be overriden for unit testing.

	// Start read-queue-stream loop function.
	Start func()

	// Stop read-queue-stream loop function.
	Stop func()

	// Flush to BigQuery function.
	flush func()

	// Insert all tables to bigquery function.
	insertAll func()

	// Insert table to BigQuery function.
	insertTable func(projectId, datasetId, tableId string, t table) (r *bigquery.TableDataInsertAllResponse, err error)
}

// NewBigQueryStreamer returns a new BigQueryStreamer.
func NewBigQueryStreamer(service *bigquery.Service, maxRows int, maxDelay time.Duration) (b *BigQueryStreamer, err error) {
	// TODO add testing for nil bigquery.Service (this will intervene with tests though,
	// maybe find a way to mock this type somehow?)

	// TODO maybe return error if maxRows > 500?

	if maxRows <= 0 {
		err = fmt.Errorf("maxRows must be positive int > 0")
		return
	}

	if maxDelay <= 0*time.Nanosecond {
		err = fmt.Errorf("maxDelay must be positive time.Duration >= 1 * time.Nanosecond")
		return
	}

	b = &BigQueryStreamer{
		service:     service,
		rowChannel:  make(chan *row, maxRows),
		rows:        make([]*row, maxRows),
		rowIndex:    0,
		maxDelay:    maxDelay,
		stopChannel: make(chan bool),
		Errors:      make(chan error, errorBufferSize),
	}

	// Assign function defaults.
	b.Start = b.start
	b.Stop = b.stop
	b.flush = b.flushToBigQuery
	b.insertAll = b.insertAllToBigQuery
	b.insertTable = b.insertTableToBigQuery

	return
}

// startStreamer infinitely reads rows from rowChannel and queues them internaly.
// It flushes to BigQuery when queue is filled (according to maxRows) or timer has expired (according to maxDelay).
//
// This function is assigned to BigQueryStreamer.Start member.
// It is overridable so we can test BigQueryMultiStreamer without actually
// starting the streamers.
//
// Note the read-insert-flush loop will never stop, so this function should be
// executed in a goroutine, and stopped via calling Stop().
func (b *BigQueryStreamer) start() {
	t := time.NewTimer(b.maxDelay)
	toStop := false
	for {
		// Flush and reset timer when one of the following signals (channels) fire:
		select {
		case toStop = <-b.stopChannel:
		case <-t.C:
		case r := <-b.rowChannel:
			// Insert row to queue.
			b.rows[b.rowIndex] = r
			b.rowIndex++

			// Don't flush if rows queue isn't full.
			if b.rowIndex < len(b.rows) {
				continue
			}
		}

		b.flush()

		if !toStop {
			t.Reset(b.maxDelay)
		} else {
			t.Stop()
			return
		}
	}
}

// stopStreamer sends a stop message to stop channel, causing Start() infinite loop to stop.
func (b *BigQueryStreamer) stop() {
	b.stopChannel <- true
}

// flushAll streams all queued rows to BigQuery and resets rows queue by
// creating a new queue.
//
// This function is assigned to BigQueryStreamer.flush member.
// It is overridable so we can test BigQueryStreamer without actually flushing
// to BigQuery.
//
// TODO Consider making this public. If so, we should use a mutex to lock the object,
// otherwise if the object is running in another goroutine it can call this in parallel.
func (b *BigQueryStreamer) flushToBigQuery() {
	b.insertAll()

	// Init (reset) a new rows queue.
	b.rows = make([]*row, len(b.rows))
	b.rowIndex = 0
}

// QueueRow sends a single row to the row channel, which will be queued and inserted in bulk with other queued rows.
func (b *BigQueryStreamer) QueueRow(projectID, datasetID, tableID string, jsonRow map[string]bigquery.JsonValue) {
	b.rowChannel <- &row{projectID, datasetID, tableID, jsonRow}
}

// BigQuery data types.
type project map[string]dataset
type dataset map[string]table
type table []tableRow
type tableRow map[string]bigquery.JsonValue

// createTableIfNotExists initializes given project, dataset, and table
// in project map if they haven't been initialized yet.
func createTableIfNotExists(ps map[string]project, p, d, t string) {
	// Create table's project if non-existent.
	if _, ok := ps[p]; !ok {
		ps[p] = project{}
	}

	// Create table's dataset if non-existent.
	if _, ok := ps[p][d]; !ok {
		ps[p][d] = dataset{}
	}

	// Create table if non-existent.
	if _, ok := ps[p][d][t]; !ok {
		ps[p][d][t] = table{}
	}
}

// insertAllToBigQuery inserts all rows from all tables to BigQuery.
// Each table is inserted separately, according to BigQuery's requirements.
// Insert errors are reported to error channel.
func (b *BigQueryStreamer) insertAllToBigQuery() {
	// Sort rows by project->dataset->table
	// Necessary because each InsertAll() has to be for a single table.
	ps := map[string]project{}
	for i := 0; i < b.rowIndex; i++ {
		r := b.rows[i]

		p, d, t := r.projectID, r.datasetID, r.tableID

		// Create project, dataset and table if uninitalized.
		createTableIfNotExists(ps, p, d, t)

		// Assign row to table.
		ps[p][d][t] = append(ps[p][d][t], r.data)
	}

	// Stream insert each table to BigQuery.
	for pID, p := range ps {
		for dID, d := range p {
			for tID, t := range d {
				responses, err := b.insertTable(pID, dID, tID, t)
				if err != nil {
					b.Errors <- err
				} else {
					for _, e := range responses.InsertErrors {
						// Fetch error index and error protocol.
						i := e.Index
						for _, ep := range e.Errors {
							v := *ep
							b.Errors <- fmt.Errorf(
								"%s.%s.%s.row[%d]: %s in %s: %s: %s",
								pID, dID, tID, i, v.Reason, v.Location, v.Message, ps[pID][dID][tID][i])
						}
					}
				}
			}
		}
	}
}

// insertTableToBigQuery inserts a single table to BigQuery using BigQuery's InsertAll request.
//
// This function is assigned to BigQueryStreamer.insertTable member.
// It is overridable so we can test BigQueryStreamer without actually inserting anythin to BigQuery.
func (b *BigQueryStreamer) insertTableToBigQuery(projectID, datasetID, tableID string, t table) (
	r *bigquery.TableDataInsertAllResponse, err error) {
	// Convert all rows to bigquery table rows.
	rows := make([]*bigquery.TableDataInsertAllRequestRows, len(t))
	for i, json := range t {
		rows[i] = &bigquery.TableDataInsertAllRequestRows{Json: json}
	}

	// Generate request, tabledata and send.
	request := bigquery.TableDataInsertAllRequest{Kind: "bigquery#tableDataInsertAllRequest", Rows: rows}

	// TODO might be better to cache table services somehow, instead of re-creating them on every flush.
	tableService := bigquery.NewTabledataService(b.service)

	r, err = tableService.InsertAll(projectID, datasetID, tableID, &request).Do()

	return
}
