package bqstreamer

import "time"

// AsyncWorker implements an asynchronous streamer,
// by wrapping around SyncWorker.
type asyncWorker struct {
	worker *SyncWorker

	// Upon invoking Start(), the worker will fetch rows from this channel,
	// and enqueue them into an internal rows queue (provided by SyncWorker).
	rowChan chan Row

	// Errors are reported to this channel.
	errorChan chan *InsertErrors

	// Max amount of rows to enqueue before executing an insert operation to BigQuery.
	maxRows int

	// Max delay between insert operations to BigQuery.
	maxDelay time.Duration

	// Shutdown channel to stop Start() execution.
	done chan struct{}

	// Used to notify the Start() loop has stopped and returned.
	closedChan chan struct{}
}

// Start reads rows from rowChan and enqueues them internally.
// It performs an insert operation to BigQuery when the queue has been
// filled or a timer has expired, according to configuration.
//
// NOTE This function is executed in a goroutine,
// and is stopped via calling Close().
func (w *asyncWorker) Start() {
	go func(w *asyncWorker) {
		// Notify on return.
		defer func(stopped chan<- struct{}) {
			close(stopped)
		}(w.closedChan)

		for {
			// Perform an insert operation and reset timer
			// when one of the following signals is triggered:
			select {
			case <-w.done:
				// Worker should close.
				w.insert()
				return
			case <-time.After(w.maxDelay):
				// Time delay between previous insert operation
				// has passed
				w.insert()
			case r := <-w.rowChan:
				// A row has been enqueued.
				// Insert row to queue.
				w.worker.Enqueue(r)

				// Don't insert yet if not enough rows have been enqueued.
				if len(w.worker.row) < w.maxRows {
					continue
				}

				w.insert()
			}
		}
	}(w)
}

// Close closes the "done" channel, causing Start()'s infinite loop to stop.
// It returns a channel, which will be closed once the Start()
// loop has returned.
func (w *asyncWorker) Close() <-chan struct{} {
	// Notify Start() loop to return.
	close(w.done)
	return w.closedChan
}

// insert performs an insert operation to BigQuery
// using the internal SyncWorker.
func (w *asyncWorker) insert() {
	// No-op if no lines have been enqueued.
	if len(w.worker.row) == 0 {
		return
	}

	insertErrs := w.worker.InsertWithRetry()

	// Report errors to error channel if set.
	if w.errorChan != nil {
		w.errorChan <- insertErrs
	}
}
