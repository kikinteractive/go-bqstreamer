// Here be option functions for constructing a new Worker.

package bqstreamer

import (
	"errors"
	"time"
)

const (
	// BigQuery has a quota policy regarding how big and often inserts should
	// be. See the following article for more info:
	//
	// https://cloud.google.com/bigquery/quota-policy#streaminginserts
	DefaultSyncMaxRetries    = 3
	DefaultSyncRetryInterval = 5 * time.Second
)

type SyncOptionFunc func(*SyncWorker) error

// SetSyncMaxRetries sets the maximum amount of retries a failed insert
// operation is allowed to retry,
// before dropping the rows and giving up on the insert operation entirely.
//
// NOTE value must be a non-negative int.
func SetSyncMaxRetries(retries int) SyncOptionFunc {
	return func(w *SyncWorker) error {
		if retries < 0 {
			return errors.New("max retries value must be a non-negative int")
		}
		w.maxRetries = retries
		return nil
	}
}

// SetSyncRetryInterval sets the time delay before retrying a failed insert
// operation (if required).
//
// NOTE value must be a positive time.Duration.
func SetSyncRetryInterval(sleep time.Duration) SyncOptionFunc {
	return func(w *SyncWorker) error {
		if sleep <= 0 {
			return errors.New("retry interval value must be a positive time.Duration")
		}
		w.retryInterval = sleep
		return nil
	}
}

// SetSyncIgnoreUnknownValues sets whether to accept rows that contain values
// that do not match the table schema.  The unknown values are ignored.
// Default is false, which treats unknown values as errors.
func SetSyncIgnoreUnknownValues(ignore bool) SyncOptionFunc {
	return func(w *SyncWorker) error {
		w.ignoreUnknownValues = ignore
		return nil
	}
}

// SetSyncSkipInvalidRows sets whether to insert all valid rows of a request,
// even if invalid rows exist. The default value is false,
// which causes the entire request to fail if any invalid rows exist.
func SetSyncSkipInvalidRows(skip bool) SyncOptionFunc {
	return func(w *SyncWorker) error {
		w.skipInvalidRows = skip
		return nil
	}
}
