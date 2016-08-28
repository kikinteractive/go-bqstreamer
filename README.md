# BigQuery Streamer <img src="bigquery.png" alt="BigQuery" width="32"> [![GoDoc][godoc image]][godoc] [![Build Status][travis image]][travis] [![Coverage Status][codecov image]][codecov]

[Stream insert][stream insert] data into [BigQuery][bigquery] *fast* and *concurrently*,
using [`InsertAll()`][InsertAll()].

## Features

- Insert rows from multiple tables, datasets, and projects, and insert them
  bulk. No need to manage data structures and sort rows by tables -
  bqstreamer does it for you.
- Multiple background workers (i.e. goroutines) to enqueue and insert rows.
- Insert can be done in a blocking or in the background (asynchronously).
- Perform insert operations in predefined set sizes, according to BigQuery's
  [quota policy][quota policy].
- Handle and retry BigQuery server errors.
- Backoff interval between failed insert operations.
- Error reporting.
- Production ready, and thoroughly tested. We - at [Rounds][rounds] - are [using it in our data gathering workflow][blog post].
- Thorough testing and documentation for great good!

## Getting Started

1. Install Go, version should be at least 1.5.
1. Clone this repository and download dependencies:
  1. Version v2: `go get gopkg.in/rounds/go-bqstreamer.v2`
  1. Version v1: `go get gopkg.in/rounds/go-bqstreamer.v1`
1. [Acquire Google OAuth2/JWT credentials][credentials], so you can authenticate with BigQuery.

## How Does It Work?

There are two types of inserters you can use:

 1. `SyncWorker`, which is a single blocking (synchronous) worker.
  1. It enqueues rows and performs insert operations in a blocking manner.
 1. `AsyncWorkerGroup`, which employes multiple background `SyncWorker`s.
  1. The `AsyncWorkerGroup` enqueues rows, and its background workers pull and
     insert in a fan-out model.
  1. An insert operation is executed according to row amount or time thresholds
      for each background worker.
  1. Errors are reported to an error channel for processing by the user.
  1. This provides a higher insert throughput for larger scale scenarios.

## Examples

Check the GoDoc [examples][examples] section.

## Contribute

 1. Please check the [issues][issues] page.
 1. File new bugs and ask for improvements.
 1. Pull requests welcome!

### Test

```bash
# Run unit tests and check coverage.
$ make test

# Run integration tests.
# This requires an active project, dataset and pem key.
$ export BQSTREAMER_PROJECT=my-project
$ export BQSTREAMER_DATASET=my-dataset
$ export BQSTREAMER_TABLE=my-table
$ export BQSTREAMER_KEY=my-key.json
$ make testintegration
```


[godoc]: https://godoc.org/github.com/rounds/go-bqstreamer
[godoc image]: https://godoc.org/github.com/rounds/go-bqstreamer?status.svg

[travis image]: https://travis-ci.org/rounds/go-bqstreamer.svg
[travis]: https://travis-ci.org/rounds/go-bqstreamer

[codecov image]: https://codecov.io/gh/rounds/go-bqstreamer/branch/master/graph/badge.svg
[codecov]: https://codecov.io/gh/rounds/go-bqstreamer

[stream insert]: https://cloud.google.com/bigquery/streaming-data-into-bigquery
[bigquery]: https://cloud.google.com/bigquery/
[InsertAll()]: https://cloud.google.com/bigquery/docs/reference/v2/tabledata/insertAll
[quota policy]: https://cloud.google.com/bigquery/quota-policy#streaminginserts
[credentials]: https://cloud.google.com/bigquery/authorization

[rounds]: http://rounds.com/
[blog post]: http://rounds.com/blog/collecting-user-data-and-usage/
[examples]: https://godoc.org/github.com/rounds/go-bqstreamer#pkg-examples
[issues]: https://github.com/rounds/go-bqstreamer/issues
