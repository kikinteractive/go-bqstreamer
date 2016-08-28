package bqstreamer

import bigquery "google.golang.org/api/bigquery/v2"

// RowErrors contains errors relating to a single row.
// Each row can have multiple errors associated with it.
type RowErrors struct {
	// A table insert operation can be split into multiple requests
	// if too many rows have been queued. This means that rows
	// containing errors cannot be identified by their table index.
	// Therefore, each row can be identified by its insert ID instead.
	InsertID string

	tableDataInsertAllResponseInsertErrors bigquery.TableDataInsertAllResponseInsertErrors
}

// Next iterates over all row errors once,
// returning a single row error every call.
// Calling Next() multiple times will consequently return more row errors,
// until all row errors have been returned.
//
// The function returns true if a non-nil value was fetched.
// Once the iterator has been exhausted, (nil, false) will be returned
// on every subsequent call.
func (row *RowErrors) Next() (*bigquery.ErrorProto, bool) {
	errs := row.tableDataInsertAllResponseInsertErrors.Errors

	if len(errs) == 0 {
		return nil, false
	}

	// Return elements in reverse order for memory efficiency.
	var err *bigquery.ErrorProto
	err, row.tableDataInsertAllResponseInsertErrors.Errors = errs[len(row.tableDataInsertAllResponseInsertErrors.Errors)-1], errs[:len(row.tableDataInsertAllResponseInsertErrors.Errors)-1]

	return err, true
}

// All returns all remaining row errors (those that have not been iterated over using
// Next()).
//
// Calling Next() or All() again afterwards will yield a failed (empty)
// result.
func (row *RowErrors) All() []*bigquery.ErrorProto {
	var errors []*bigquery.ErrorProto
	for {
		err, ok := row.Next()
		if !ok {
			break
		}
		errors = append(errors, err)
	}
	return errors
}
