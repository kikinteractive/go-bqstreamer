package bqstreamer

import (
	"github.com/dchest/uniuri"
	bigquery "google.golang.org/api/bigquery/v2"
)

// Row associates a single BigQuery table row to a project, dataset and table.
type Row struct {
	ProjectID,
	DatasetID,
	TableID string
	Data map[string]bigquery.JsonValue

	// Used for deduplication:
	// https://cloud.google.com/bigquery/streaming-data-into-bigquery#dataconsistency
	InsertID string
}

// NewRow returns a new Row instance, with an automatically generated insert ID
// used for deduplication purposes.
func NewRow(projectID, datasetID, tableID string, data map[string]bigquery.JsonValue) Row {
	return NewRowWithID(
		projectID,
		datasetID,
		tableID,
		uniuri.NewLen(uniuri.UUIDLen),
		data,
	)
}

// NewRowWithID returns a new Row instance with given insert ID.
func NewRowWithID(projectID, datasetID, tableID, insertID string, data map[string]bigquery.JsonValue) Row {
	return Row{
		ProjectID: projectID,
		DatasetID: datasetID,
		TableID:   tableID,
		InsertID:  insertID,
		Data:      data,
	}
}
