// Here be option functions for constructing a new Streamer.

package bqstreamer

import (
	"errors"
	"time"
)

const (
	DefaultAsyncNumWorkerss = 10
	DefaultAsyncMaxRows     = 500
	DefaultAsyncMaxDelay    = 5 * time.Second
)

type AsyncOptionFunc func(*AsyncWorkerGroup) error

// SetAsyncNumWorkers sets the amount of background workers.
//
// NOTE value must be a positive int.
func SetAsyncNumWorkers(workers int) AsyncOptionFunc {
	return func(s *AsyncWorkerGroup) error {
		if workers <= 0 {
			return errors.New("number of workers must be a positive int")
		}
		s.numWorkers = workers
		return nil
	}
}

// SetAsyncErrorChannel sets the asynchronous workers' error channel.
//
// Use this option when you want all workers to report errors
// to a unified channel.
//
// NOTE the error channel is not closed when the AsyncWorkerGroup closes.
// It is the responsibilty of the user to close it.
func SetAsyncErrorChannel(errChan chan *InsertErrors) AsyncOptionFunc {
	return func(w *AsyncWorkerGroup) error {
		if errChan == nil {
			return errors.New("error channel is nil")
		}
		w.errorChan = errChan
		return nil
	}
}

// SetAsyncMaxRetries sets the maximum amount of retries a failed insert
// operation can be retried,
// before dropping the rows and giving up on the insert operation entirely.
//
// NOTE value must be a non-negative int.
func SetAsyncMaxRetries(retries int) AsyncOptionFunc {
	return func(s *AsyncWorkerGroup) error {
		if retries < 0 {
			return errors.New("max retry insert must be a non-negative int")
		}
		s.maxRetries = retries
		return nil
	}
}

// SetAsyncMaxRows sets the maximum amount of rows a worker can enqueue
// before an insert operation is executed.
//
// NOTE this threshold is not per-table,
// but the entire amount of rows overall enqueued by a single worker.
//
// NOTE value must be a non-negative int.
func SetAsyncMaxRows(rowLen int) AsyncOptionFunc {
	return func(s *AsyncWorkerGroup) error {
		if rowLen <= 0 {
			return errors.New("max rows must be non-negative int")
		}
		s.maxRows = rowLen
		return nil
	}
}

// SetAsyncMaxDelay sets the maximum time delay a worker should wait
// before an insert operation is executed.
//
// NOTE value must be a positive time.Duration.
func SetAsyncMaxDelay(delay time.Duration) AsyncOptionFunc {
	return func(s *AsyncWorkerGroup) error {
		if delay <= 0 {
			return errors.New("max delay must be a positive time.Duration")
		}
		s.maxDelay = delay
		return nil
	}
}

// SetAsyncRetryInterval sets the time delay before retrying a failed insert
// operation (if required).
//
// NOTE value must be a positive time.Duration.
func SetAsyncRetryInterval(sleep time.Duration) AsyncOptionFunc {
	return func(s *AsyncWorkerGroup) error {
		if sleep <= 0 {
			return errors.New("sleep before retry must be a positive time.Duration")
		}
		s.retryInterval = sleep
		return nil
	}
}

// SetAsyncIgnoreUnknownValues sets whether to accept rows that contain values
// that do not match the table schema.  The unknown values are ignored.
//
// Default is false, which treats unknown values as errors.
func SetAsyncIgnoreUnknownValues(ignore bool) AsyncOptionFunc {
	return func(w *AsyncWorkerGroup) error {
		w.ignoreUnknownValues = ignore
		return nil
	}
}

// SetAsyncSkipInvalidRows sets whether to insert all valid rows of a request,
// even if invalid rows exist.
//
// The default value is false,
// which causes the entire request to fail if any invalid rows exist.
func SetAsyncSkipInvalidRows(skip bool) AsyncOptionFunc {
	return func(w *AsyncWorkerGroup) error {
		w.skipInvalidRows = skip
		return nil
	}
}
