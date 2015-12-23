// Here be various error types sent by the streamer.

package bqstreamer

import (
	"encoding/json"
	"strconv"
	"strings"

	bigquery "google.golang.org/api/bigquery/v2"
)

// TooManyFailedInsertRetriesError is returned when an insert failed several
// time, and the streamer stops retrying it.
type TooManyFailedInsertRetriesError struct {
	numFailedRetries int

	projectID,
	datasetID,
	tableID string
}

func (err *TooManyFailedInsertRetriesError) Error() string {
	// The equivalent format is "Insert table %s.%s.%s retried %d times, dropping nsert and moving on"
	return strings.Join(
		[]string{
			"Insert table ",
			err.projectID, ".",
			err.datasetID, ".",
			err.tableID, " ",
			"retried ", strconv.Itoa(err.numFailedRetries),
			" times, dropping insert and moving on"},
		"")
}

func newTooManyFailedInsertRetriesError(
	numFailedRetries int,
	projectID, datasetID, tableID string) *TooManyFailedInsertRetriesError {

	return &TooManyFailedInsertRetriesError{
		numFailedRetries: numFailedRetries,

		projectID: projectID,
		datasetID: datasetID,
		tableID:   tableID,
	}
}

func (err *TooManyFailedInsertRetriesError) NumFailedRetries() int { return err.numFailedRetries }
func (err *TooManyFailedInsertRetriesError) TableID() string       { return err.tableID }
func (err *TooManyFailedInsertRetriesError) DatasetID() string     { return err.datasetID }
func (err *TooManyFailedInsertRetriesError) ProjectID() string     { return err.projectID }

// AllRowsRejectedError is returned when all rows in an insert have been rejected,
// meaning no insert retry will occur.
type AllRowsRejectedError struct {
	projectID,
	datasetID,
	tableID string
}

func (err *AllRowsRejectedError) Error() string {
	// The equivalent format is "All rows from %s.%s.%s have been rejected, moving on"
	return strings.Join(
		[]string{
			"All rows from ",
			err.projectID, ".",
			err.datasetID, ".",
			err.tableID, " ",
			"have been rejected, moving on"},
		"")
}

func newAllRowsRejectedError(projectID, datasetID, tableID string) *AllRowsRejectedError {
	return &AllRowsRejectedError{
		projectID: projectID,
		datasetID: datasetID,
		tableID:   tableID}
}

func (err *AllRowsRejectedError) TableID() string   { return err.tableID }
func (err *AllRowsRejectedError) DatasetID() string { return err.datasetID }
func (err *AllRowsRejectedError) ProjectID() string { return err.projectID }

// RowError is a specific row insert error,
// returned after inserting multiple rows.
type RowError struct {
	// Original error information returned from BigQuery.
	bqError bigquery.ErrorProto

	index int64

	projectID,
	datasetID,
	tableID string

	// Row Value
	jsonValue map[string]bigquery.JsonValue
}

func (err *RowError) Error() string {
	// The equivalent format is "%s.%s.%s.row[%d]: %s in %s: %s: %s"
	return strings.Join(
		[]string{
			err.projectID, ".",
			err.datasetID, ".",
			err.tableID, ".",
			"row[", strconv.FormatInt(err.index, 10), "]: ",
			err.bqError.Reason, " ",
			"in ", err.bqError.Location, ": ",
			err.bqError.Message, ": ",
			err.jsonValueToString()},
		"")
}

func newRowError(
	bqError bigquery.ErrorProto,
	index int64,
	projectID, datasetID, tableID string,
	jsonValue map[string]bigquery.JsonValue) *RowError {

	return &RowError{
		bqError: bqError,

		index: index,

		projectID: projectID,
		datasetID: datasetID,
		tableID:   tableID,

		jsonValue: jsonValue,
	}
}

func (err *RowError) BQError() bigquery.ErrorProto             { return err.bqError }
func (err *RowError) Index() int64                             { return err.index }
func (err *RowError) TableID() string                          { return err.tableID }
func (err *RowError) DatasetID() string                        { return err.datasetID }
func (err *RowError) ProjectID() string                        { return err.projectID }
func (err *RowError) JsonValue() map[string]bigquery.JsonValue { return err.jsonValue }

// jsonValueToString is a helper function that marshals err.jsonValue to a JSON
// string.
// It's used in Error() string representation.
// Returns an empty string on error.
func (err *RowError) jsonValueToString() string {
	var b []byte
	var e error
	if b, e = json.Marshal(err.jsonValue); e != nil {
		return ""
	}
	return string(b)
}
