// Here be a synchronous worker implementation for stream inserting into BigQuery.

package bqstreamer

import (
	"net/http"
	"time"

	bigquery "google.golang.org/api/bigquery/v2"
	"google.golang.org/api/googleapi"
)

// An estimated size for queued rows before inserting to BigQuery.
//
// https://cloud.google.com/bigquery/quota-policy#streaminginserts
//
// TODO make this configurable
const rowSize = 500

// SyncWorker streams rows to BigQuery in bulk using synchronous calls.
type SyncWorker struct {
	// BigQuery client connection.
	service *bigquery.Service

	// Internal list to queue rows for stream insert.
	rows []Row

	// Sleep delay after a rejected insert and before a retry insert attempt.
	retryInterval time.Duration

	// Maximum retry insert attempts for non-rejected row insert
	// e.g. GoogleAPI HTTP errors, generic HTTP errors, etc.
	maxRetries int

	// Accept rows that contain values that do not match the schema.
	// The unknown values are ignored.
	// Default is false, which treats unknown values as errors.
	ignoreUnknownValues bool

	// Insert all valid rows of a request, even if invalid rows exist.
	// The default value is false, which causes the entire request
	// to fail if any invalid rows exist.
	skipInvalidRows bool
}

// NewSyncWorker returns a new SyncWorker.
func NewSyncWorker(client *http.Client, options ...SyncOptionFunc) (*SyncWorker, error) {
	service, err := bigquery.New(client)
	if err != nil {
		return nil, err
	}

	w := SyncWorker{
		service:       service,
		rows:          make([]Row, 0, rowSize),
		retryInterval: DefaultSyncRetryInterval,
		maxRetries:    DefaultSyncMaxRetries,
	}

	// Override defaults with options if given.
	for _, option := range options {
		if err := option(&w); err != nil {
			return nil, err
		}
	}

	return &w, nil
}

// Enqueue enqueues rows for insert in bulk.
func (w *SyncWorker) Enqueue(row Row) {
	w.rows = append(w.rows, row)
}

// RowLen returns the number of enqueued rows in the worker,
// which haven't been inserted into BigQuery yet.
func (w *SyncWorker) RowLen() int {
	return len(w.rows)
}

// Insert executes an insert operation in bulk.
// It sorts rows by tables, and inserts them using separate insert requests.
// It also splits rows for the same table if too many rows have been queued,
// according to BigQuery quota policy.
//
// The insert blocks until a response is returned.
// The response contains insert and row errors for the inserted tables.
func (w *SyncWorker) Insert() *InsertErrors {
	insertErrs := w.insertAll(w.insertTable)
	return insertErrs
}

// InsertWithRetry is similar to Insert(),
// but retries an insert operation multiple times on BigQuery server errors.
//
// See the following article for more info:
// https://cloud.google.com/bigquery/troubleshooting-errors
func (w *SyncWorker) InsertWithRetry() *InsertErrors {
	insertErrs := w.insertAll(w.insertTableWithRetry)
	return insertErrs
}

// insertAll takes all rows, sorts them across projects, datasets, and tables,
// then inserts them to their respectable tables in BigQuery using InsertAll().
func (w *SyncWorker) insertAll(insertFunc func(projectID, datasetID, tableID string, tbl table) *TableInsertErrors) *InsertErrors {
	// Reset rows queue when finished.
	defer func() { w.rows = w.rows[:0] }()

	// Sort rows by project -> dataset -> table heirarchy.
	// Necessary because each InsertAll() request has to be for a single table.
	ps := projects{}
	for _, r := range w.rows {
		p, d, t := r.ProjectID, r.DatasetID, r.TableID

		// Create project, dataset and table if uninitalized.
		initTableIfNotExists(ps, p, d, t)

		// Append row to table,
		// and generate random row ID of 16 character length, for de-duplication purposes.
		ps[p][d][t] = append(ps[p][d][t], &bigquery.TableDataInsertAllRequestRows{
			InsertId: r.InsertID,
			Json:     r.Data,
		})
	}

	// Stream insert each table to BigQuery.
	//
	// TODO insert concurrently
	var insertErrs InsertErrors
	for pID, p := range ps {
		for dID, d := range p {
			for tID := range d {
				tableErrs := insertFunc(pID, dID, tID, d[tID])
				insertErrs.Tables = append(insertErrs.Tables, tableErrs)
			}
		}
	}

	return &insertErrs
}

// insertTable inserts a single table to BigQuery using InsertAll().
//
// It returns tableErrors contains information about rows that were not inserted.
//
// TODO cache bigquery service instead of creating a new one every insertTable() call
// TODO add support for SkipInvalidRows, IgnoreUnknownValues
func (w *SyncWorker) insertTable(projectID, datasetID, tableID string, tbl table) *TableInsertErrors {
	res, err := bigquery.NewTabledataService(w.service).
		InsertAll(
			projectID, datasetID, tableID,
			&bigquery.TableDataInsertAllRequest{
				Kind:                "bigquery#tableDataInsertAllRequest",
				Rows:                tbl,
				IgnoreUnknownValues: w.ignoreUnknownValues,
				SkipInvalidRows:     w.skipInvalidRows,
			}).
		Do()

	var rows []*bigquery.TableDataInsertAllResponseInsertErrors
	if res != nil {
		rows = res.InsertErrors
	}

	insertIDs := make([]string, 0, len(tbl))
	for _, row := range tbl {
		insertIDs = append(insertIDs, row.InsertId)
	}

	// Return response as a single table insert attempt.
	return &TableInsertErrors{
		InsertAttempts: []*TableInsertAttemptErrors{
			&TableInsertAttemptErrors{
				err:       err,
				rows:      rows,
				insertIDs: insertIDs,
				Table:     tableID,
				Dataset:   datasetID,
				Project:   projectID,
			},
		},
	}
}

// insertTableWithRetry is similar to insertTable,
// but also retries insert operations on certain conditions.
func (w *SyncWorker) insertTableWithRetry(projectID, datasetID, tableID string, tbl table) *TableInsertErrors {
	var tableInsertErrs TableInsertErrors

	numRetries := 0
	for {
		numRetries++

		// Abort if retries and failed too many times.
		if numRetries > w.maxRetries {
			err := &TooManyFailedInsertRetriesError{
				NumFailedRetries: numRetries,
				Project:          projectID,
				Dataset:          datasetID,
				Table:            tableID,
			}
			tableInsertErrs.InsertAttempts = append(tableInsertErrs.InsertAttempts, &TableInsertAttemptErrors{err: err})
			return &tableInsertErrs
		}

		// Push this table's insert attempt as an additional one
		// in insert attempts slice.
		currTableInsertErrs := w.insertTable(projectID, datasetID, tableID, tbl)
		currInsertAttempt := currTableInsertErrs.InsertAttempts[0]
		tableInsertErrs.InsertAttempts = append(tableInsertErrs.InsertAttempts, currInsertAttempt)

		// Retry on certain HTTP responses.
		if w.shouldRetryInsert(currInsertAttempt.err) {
			continue
		}

		// If we reached here, it means the insert operation was successful.
		// Thus, it is not required to retry the insert operation.
		//
		// Return all accumulated errors.
		break
	}

	return &tableInsertErrs
}

// shouldRetryInsert checks for given insert HTTP response error,
// and returns true if the insert should be retried.
//
// It also sleeps if the return value is true,
// as a backoff mechanism.
//
// There are various cases where a retry is or is not necessary.
// See the following article for more info:
// https://cloud.google.com/bigquery/troubleshooting-errors
//
// TODO add exponential backoff
func (w *SyncWorker) shouldRetryInsert(err error) bool {
	if err != nil {
		// Retry on GoogleAPI HTTP server errors 500, 503.
		if gerr, ok := err.(*googleapi.Error); ok {
			switch gerr.Code {
			case 500, 503:
				time.Sleep(w.retryInterval)
				return true
			}
		}
	}
	return false
}

// initTableIfNotExists initializes given project, dataset, and table
// in project map if not yet initialized.
func initTableIfNotExists(ps map[string]project, p, d, t string) {
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
