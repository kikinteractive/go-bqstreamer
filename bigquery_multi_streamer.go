package main

import (
	"fmt"
	"time"

	"code.google.com/p/goauth2/oauth/jwt" // TODO Deprecated, use google.golang.org
	bigquery "google.golang.org/api/bigquery/v2"
)

// A BigQueryMultiStreamer manages several BigQueryStreamers.
// The multi-streamer feeds rows to a single rowChannel and all sub-streamers
// read from it together.
//
// This improves scalability by allowing a higher message throuhput.
//
// TODO Improve by managing a channel of sub-streamers, notifying
// multi-streamer when they're ready to read rows, and then letting them read
// one-by-one (the first will read a chunk and go streaming, then the next one
// will read and stream, etc.)
// Right now they're all reading together simultaniously, reacing for messages.
// See here: http://nesv.github.io/golang/2014/02/25/worker-queues-in-go.html
type BigQueryMultiStreamer struct {
	// BigQuery sub-streamers (i.e. "workers") slice.
	streamers []*BigQueryStreamer

	// Channel for sending rows to background streamer.
	rowChannel chan *row

	// JWT/OAuth2 authentication token.
	token *jwt.Token

	// Errors are reported to this channel.
	Errors chan error
}

// NewBigQueryMultiStreamer returns a new BigQueryMultiStreamer.
func NewBigQueryMultiStreamer(
	token *jwt.Token,
	numStreamers int,
	maxRows int,
	maxDelay time.Duration) (*BigQueryMultiStreamer, error) {

	// Create a new streamer, with standard service creation function.
	// That function (service creation) is overridable for unit testing.
	return newBigQueryMultiStreamer(
		NewBigQueryService, token, numStreamers, maxRows, maxDelay)
}

// newBigQueryMultiStreamer returns a new BigQueryMultiStreamer.
// It recieves a NewBigQueryService function, which can be set as "always
// return a nil service" for unit testing.
func newBigQueryMultiStreamer(
	newBigQueryService func(t *jwt.Token) (service *bigquery.Service, err error),
	token *jwt.Token,
	numStreamers int,
	maxRows int,
	maxDelay time.Duration) (b *BigQueryMultiStreamer, err error) {

	if numStreamers <= 0 {
		err = fmt.Errorf("numStreamers must be positive int > 0")
		return
	}

	if maxRows <= 0 {
		err = fmt.Errorf("maxRows must be positive int > 0")
		return
	}

	// Initialize sub-streamers and assign them a common row and error channel.
	rowChannel := make(chan *row, maxRows)
	errors := make(chan error, errorBufferSize)

	streamers := make([]*BigQueryStreamer, numStreamers)
	for i := 0; i < numStreamers; i++ {
		var service *bigquery.Service
		service, err = newBigQueryService(token)
		if err != nil {
			return
		}

		streamers[i], err = NewBigQueryStreamer(service, maxRows, maxDelay)
		if err != nil {
			return
		}

		streamers[i].rowChannel = rowChannel
		streamers[i].Errors = errors
	}

	b = &BigQueryMultiStreamer{
		streamers:  streamers,
		rowChannel: rowChannel,
		token:      token,
		Errors:     errors,
	}

	return
}

// Start starts the sub-streamers, making them read from a common row channel,
// and output to the same error channel.
func (b *BigQueryMultiStreamer) Start() {
	for _, s := range b.streamers {
		go s.Start()
	}
}

// Stop stops all sub-streamers.
// Note all sub-streamers will flush to BigQuery before stopping.
func (b *BigQueryMultiStreamer) Stop() {
	for _, s := range b.streamers {
		s.Stop()
	}
}

// QueueRow queues a single row, which will be read and inserted by one of the sub-streamers.
func (b *BigQueryMultiStreamer) QueueRow(projectID, datasetID, tableID string, jsonRow map[string]bigquery.JsonValue) {
	b.rowChannel <- &row{projectID, datasetID, tableID, jsonRow}
}
