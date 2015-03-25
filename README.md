# BigQuery Streamer

Stream insert data into BigQuery *fast* and *concurrently*, using `InsertAll()`.

## How?

There are two primary types used, `Streamer` and `MultiStreamer`.

A `Streamer` is a single worker which reads rows, queues them, and inserts them
in bulk into BigQuery once a certain threshold is reached.

A `MultiStreamer` operates multiple `Streamer`s concurrently. It reads rows and
distributes them to the `Streamers`.

## Getting Started

1. Install Go, version should be at least 1.3. We recommend using [gvm][gvm] to
   manage your Go versions.
2. Execute `go get -t ./...` to download all necessary packages.
3. Run this example `main.go`:

```go
package main

import (
    "log"
	"google.golang.org/api/bigquery/v2"
    "github.com/rounds/go-bqstreamer"
)

func main() {
    // Init OAuth2/JWT. See the following URLs for more info:
    // https://cloud.google.com/bigquery/authorization
    // https://developers.google.com/console/help/new/#generatingoauth2
	jwtConfig, err := bqstreamer.NewJWTConfig("path_to_key.json")
	if err != nil {
		log.Fatalln(err)
	}

    numStreamers := 10  // Number of concurrent streamers.
    maxRows := 500  // Row threshold to use for each streamer, before insert.
    maxDelay := 1  //  Time threshold to use for each streamer, before insert.
    sleepBeforeRetry := 1  // Time to wait between failed retries.
    maxRetryInsert := 10 // Maximum amount of failed retry attempts before discarding rows and moving on.

    // Init a new multi-streamer.
    ms, err = bqstreamer.NewMultiStreamer(
            jwtConfig, numStreamer, maxRows, maxDelay, sleepBeforeRetry, maxRetryInsert)

    // Start multi-streamer and workers.
	ms.Start()
	defer ms.Stop()

    // Worker errors will be reported to this channel.
	StreamerShutdownErrorsChannel = make(chan bool)
	go func () {
        var err error

        readErrors := true
        for readErrors {
            select {
            case <-StreamerShutdownErrorsChannel:
                readErrors = false
            case err = <-Streamer.Errors:
                log.Println(err)
            }
        }
    }
	defer func() { StreamerShutdownErrorsChannel <- true }()

    Streamer.QueueRow(
        "project-id", "dataset-id", "table-id",
        map[string]bigquery.JsonValue{"key": "value"}
    )
}
```


[gvm]: https://github.com/moovweb/gvm
