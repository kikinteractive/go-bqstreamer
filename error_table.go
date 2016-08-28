package bqstreamer

import (
	"strconv"
	"strings"

	bigquery "google.golang.org/api/bigquery/v2"
)

// TableInsertErrors contains errors relating to a specific table
// from a bulk insert operation.
type TableInsertErrors struct {
	InsertAttempts []*TableInsertAttemptErrors
}

// Attempts returns all insert attempts for a single table,
// in the order they were executed.
// All but the last attempts in the returned slice have failed.
// Only the last one might have succeeded, or failed as well,
// as indicated by Error().
func (table *TableInsertErrors) Attempts() []*TableInsertAttemptErrors {
	attempts := make([]*TableInsertAttemptErrors, 0, len(table.InsertAttempts))
	for i := range table.InsertAttempts {
		attempts = append(attempts, table.InsertAttempts[i])
	}
	return attempts
}

// TableInsertAttemptErrors contains errors relating to a single table insert attempt.
//
// It implements the error interface.
type TableInsertAttemptErrors struct {
	err  error
	rows []*bigquery.TableDataInsertAllResponseInsertErrors

	// Used for identifying rows by their insertIDs instead of their index.
	insertIDs []string

	// The table name associated with the insert attempt.
	Table string

	// The  dataset name associated with the insert attempt.
	Dataset string

	// The project associated with the insert attempt.
	Project string
}

// Next iterates over the attempt's rows once,
// returning a single row  every call.
// Calling Next() multiple times will consequently return more rows,
// until all have been returned.
//
// The function returns true if a non-nil value was fetched.
// Once the iterator has been exhausted, (nil, false) will be returned
// on every subsequent call.
func (table *TableInsertAttemptErrors) Next() (*RowErrors, bool) {
	if len(table.rows) == 0 {
		return nil, false
	}

	// Return elements in reverse order for memory efficiency.
	var errors bigquery.TableDataInsertAllResponseInsertErrors
	errors, table.rows = *table.rows[len(table.rows)-1], table.rows[:len(table.rows)-1]

	row := RowErrors{
		tableDataInsertAllResponseInsertErrors: errors,
		InsertID: table.insertIDs[errors.Index],
	}

	return &row, true
}

// All returns all remaining row errors (those that have not been
// interated over using Next()).
//
// Calling Next() or All() again afterwards will yield a failed (empty)
// result.
func (table *TableInsertAttemptErrors) All() []*RowErrors {
	rows := make([]*RowErrors, 0, len(table.rows))
	for {
		row, ok := table.Next()
		if !ok {
			break
		}
		rows = append(rows, row)
	}
	return rows
}

// Error returns a non-nil value when the table's insert attempt has failed
// completely.
//
// NOTE that an attempt can have no error, but still not insert the rows.
// This can happen for example if the request includes malformed rows with
// SkipInvalidRows set to false.
func (table *TableInsertAttemptErrors) Error() error { return table.err }

// TooManyFailedInsertAttemptsError is returned when a specific insert attempt
// has been retried and failed multiple times,
// causing the worker to stop retrying and drop that table's insert operation
// entirely.
//
// It implements the error interface.
type TooManyFailedInsertRetriesError struct {
	// Number of failed retries.
	NumFailedRetries int

	// The table name associated with the insert attempt.
	Table string

	// The  dataset name associated with the insert attempt.
	Dataset string

	// The project associated with the insert attempt.
	Project string
}

func (err *TooManyFailedInsertRetriesError) Error() string {
	// The equivalent format is "Insert table %s.%s.%s retried %d times,
	// dropping insert and moving on"
	return strings.Join(
		[]string{
			"Insert table ",
			err.Project, ".",
			err.Dataset, ".",
			err.Table, " ",
			"retried ", strconv.Itoa(err.NumFailedRetries),
			" times, dropping insert and moving on",
		},
		"")
}
