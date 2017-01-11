package bqstreamer

import (
	"net/http"
	"sync"
	"time"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"golang.org/x/oauth2/jwt"
	"google.golang.org/api/bigquery/v2"
)

// AsyncWorkerGroup asynchronously streams rows to BigQuery in bulk.
type AsyncWorkerGroup struct {
	// Sync worker slice.
	workers []*asyncWorker

	// Channel for sending rows to background Workers.
	rowChan chan Row

	// Insert errors are reported to this channel.
	errorChan chan *InsertErrors

	// Amount of background workers to use.
	numWorkers int

	// Max amount of rows to queue before flushing to BigQuery.
	maxRows int

	// Max delay between insert operations to BigQuery.
	maxDelay time.Duration

	// Maximum insert operation retries for non-rejected rows,
	// e.g. GoogleAPI HTTP errors, generic HTTP errors, etc.
	maxRetries int

	// Sleep delay after a rejected insert,
	// before retrying an insert operation.
	retryInterval time.Duration

	// Accept rows that contain values that do not match the schema.
	// The unknown values are ignored.
	// Default is false, which treats unknown values as errors.
	ignoreUnknownValues bool

	// Insert all valid rows of a request, even if invalid rows exist.
	// The default value is false, which causes the entire request
	// to fail if any invalid rows exist.
	skipInvalidRows bool
}

// New returns a new AsyncWorkerGroup using given OAuth2/JWT configuration.
// Set jwtConfig to nil if your system corresponds to either of the following conditions:
// - a system that has called "gcloud auth application-default login"
// - a system running in Google Application Engine
// - a system running in Google Compute Engine
// ref: https://developers.google.com/identity/protocols/application-default-credentials
func NewAsyncWorkerGroup(jwtConfig *jwt.Config, options ...AsyncOptionFunc) (*AsyncWorkerGroup, error) {
	if jwtConfig == nil {
		ctx := oauth2.NoContext
		client, err := google.DefaultClient(ctx, bigquery.BigqueryInsertdataScope)
		if err != nil {
			return nil, err
		}
		newHTTPClient := func() *http.Client { return client }
		return newAsyncWorkerGroup(newHTTPClient, options...)
	}
	// Create a new Streamer, with OAuth2/JWT http.Client constructor function.
	newHTTPClient := func() *http.Client { return jwtConfig.Client(oauth2.NoContext) }
	return newAsyncWorkerGroup(newHTTPClient, options...)
}

// newAsyncWorkerGroup returns a new AsyncWorkerGroup.
//
// It recieves an http.Client constructor, which is used to return an
// authenticated OAuth2/JWT client, or a no-op client for unit tests.
func newAsyncWorkerGroup(newHTTPClient func() *http.Client, options ...AsyncOptionFunc) (*AsyncWorkerGroup, error) {
	m := AsyncWorkerGroup{}

	// Override configuration defaults with options if given.
	for _, option := range options {
		if err := option(&m); err != nil {
			return nil, err
		}
	}
	m.rowChan = make(chan Row, m.maxRows*m.numWorkers)
	m.workers = make([]*asyncWorker, m.numWorkers)

	// Initialize workers and assign them a common row and error channel.
	//
	// NOTE AsyncWorkerGroup row length is set as following to avoid filling up
	// in case workers get delayed with insert retries.
	for i := 0; i < m.numWorkers; i++ {
		syncWorker, err := NewSyncWorker(
			newHTTPClient(),
			SetSyncMaxRetries(m.maxRetries),
			SetSyncRetryInterval(m.retryInterval),
			SetSyncIgnoreUnknownValues(m.ignoreUnknownValues),
			SetSyncSkipInvalidRows(m.skipInvalidRows),
		)
		if err != nil {
			return nil, err
		}

		m.workers[i] = &asyncWorker{
			worker: syncWorker,

			rowChan:   m.rowChan,
			errorChan: m.errorChan,

			maxRows:  m.maxRows,
			maxDelay: m.maxDelay,

			done:       make(chan struct{}),
			closedChan: make(chan struct{}),
		}
		if err != nil {
			return nil, err
		}
	}

	return &m, nil
}

// Start starts all background workers.
//
// Workers read enqueued rows,
// and insert them to BigQuery until one of the following happens:
//  - Enough time has passed according to configuration.
//  - Amount of rows has been enqueued by a worker, also configurable.
//
// Insert errors will be reported to the error channel if set.
func (s *AsyncWorkerGroup) Start() {
	for _, w := range s.workers {
		w.Start()
	}
}

// Close inserts any remaining rows enqueue by all workers,
// then closes them.
//
// NOTE that the AsyncWorkerGroup cannot be restarted.
// If you wish to perform any additional inserts to BigQuery,
// a new one must be initialized.
func (s *AsyncWorkerGroup) Close() {
	var wg sync.WaitGroup
	for _, w := range s.workers {
		wg.Add(1)
		go func(w *asyncWorker) {
			defer wg.Done()
			// Block until worker has closed.
			<-w.Close()
		}(w)
	}
	wg.Wait()
}

func (s *AsyncWorkerGroup) Enqueue(row Row) {
	s.rowChan <- row
}
