package bqstreamer

import (
	"fmt"
	"time"

	"golang.org/x/oauth2/jwt"
	bigquery "google.golang.org/api/bigquery/v2"
)

// A MultiStreamer operates multiple Streamers, also called workers, or sub-streamers.
// The MultiStreamer feeds rows to a single rowChannel, and all sub-streamers read from it together.
// This improves scalability by allowing a higher message throuhput.
//
// TODO Improve by managing a channel of sub-streamers, notifying
// multi-streamer when they're ready to read rows, and then letting them read
// one-by-one (the first will read a chunk and go streaming, then the next one
// will read and stream, etc.)
// Right now they're all reading together simultaniously, reacing for messages.
// See here: http://nesv.github.io/golang/2014/02/25/worker-queues-in-go.html
type MultiStreamer struct {
	// BigQuery sub-streamers (i.e. "workers") slice.
	streamers []*Streamer

	// Channel for sending rows to background streamer.
	rowChannel chan *row

	// OAuth2/JWT authentication configuration.
	jwtConfig *jwt.Config

	// Errors are reported to this channel.
	Errors chan error
}

// NewMultiStreamer returns a new MultiStreamer.
func NewMultiStreamer(
	jwtConfig *jwt.Config,
	numStreamers int,
	maxRows int,
	maxDelay time.Duration,
	sleepBeforeRetry time.Duration,
	maxRetryInsert int) (*MultiStreamer, error) {

	// Create a new streamer, with standard service creation function.
	// That function (service creation) is overridable for unit testing.
	return newMultiStreamer(
		NewBigQueryService, jwtConfig, numStreamers, maxRows, maxDelay, sleepBeforeRetry, maxRetryInsert)
}

// newMultiStreamer returns a new MultiStreamer.
// It recieves a NewBigQueryService function, which can be set as "always
// return a nil service" for unit testing.
func newMultiStreamer(
	newBigQueryService func(c *jwt.Config) (service *bigquery.Service, err error),
	jwtConfig *jwt.Config,
	numStreamers int,
	maxRows int,
	maxDelay time.Duration,
	sleepBeforeRetry time.Duration,
	maxRetryInsert int) (b *MultiStreamer, err error) {

	if numStreamers <= 0 {
		err = fmt.Errorf("numStreamers must be positive int > 0")
		return
	}

	if maxRows <= 0 {
		err = fmt.Errorf("maxRows must be positive int > 0")
		return
	}

	// Initialize sub-streamers and assign them a common row and error channel.
	// Multi-streamer row length is set as following to avoid filling up
	// in case sub-streamers get delayed with insert retries.
	rowChannel := make(chan *row, maxRows*numStreamers)
	errors := make(chan error, errorBufferSize)

	streamers := make([]*Streamer, numStreamers)
	for i := 0; i < numStreamers; i++ {
		var service *bigquery.Service
		service, err = newBigQueryService(jwtConfig)
		if err != nil {
			return
		}

		streamers[i], err = NewStreamer(service, maxRows, maxDelay, sleepBeforeRetry, maxRetryInsert)
		if err != nil {
			return
		}

		streamers[i].rowChannel = rowChannel
		streamers[i].Errors = errors
	}

	b = &MultiStreamer{
		streamers:  streamers,
		rowChannel: rowChannel,
		jwtConfig:  jwtConfig,
		Errors:     errors,
	}

	return
}

// Start starts the sub-streamers, making them read from a common row channel,
// and output to the same error channel.
func (b *MultiStreamer) Start() {
	for _, s := range b.streamers {
		go s.Start()
	}
}

// Stop stops all sub-streamers.
// Note all sub-streamers will flush to BigQuery before stopping.
func (b *MultiStreamer) Stop() {
	for _, s := range b.streamers {
		s.Stop()
	}
}

// QueueRow queues a single row, which will be read and inserted by one of the sub-streamers.
func (b *MultiStreamer) QueueRow(projectID, datasetID, tableID string, jsonRow map[string]bigquery.JsonValue) {
	b.rowChannel <- &row{projectID, datasetID, tableID, jsonRow}
}
