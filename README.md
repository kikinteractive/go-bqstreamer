# BigQuery Streamer [![GoDoc][godoc image]][godoc] [![Build Status][travis image]][travis] [![Coverage Status][coveralls image]][coveralls]

[Stream insert][stream insert] data into [BigQuery][bigquery] *fast* and *concurrently*, using `InsertAll()`.

## Features

- Inserts multiple rows in bulk.
- Uses configurable multiple workers (i.e. goroutines) to queue and insert rows.
- Production ready, and thoroughly tested. We - at [Rounds][rounds] - are [using it in our data gathering workflow][blog post].
- BigQuery errors are sent to a unified channel so you can read and decide how to handle them.

## Getting Started

1. Install Go, version should be at least 1.3. We recommend using [gvm][gvm] to manage your Go versions.
2. Execute `go get -t ./...` to download all necessary packages.
3. [Acquire Google OAuth2/JWT credentials][credentials], so you can connect to BigQuery.
4. Copy and run one of the examples: [MultiStreamer][multi-streamer example] and [Streamer][streamer example].

## How Does It Work?

There are two types you can use: `Streamer` and `MultiStreamer`.

### Streamer

A `Streamer` is a single worker which reads rows, queues them, and inserts them
(also called *flushing*) in bulk into BigQuery once a certain threshold is reached.
Thresholds can be either an amount of rows queued, or based on time - inserting once a certain time has passed.

This provides flush control, inserting in set sizes and quickly enough.
Please note Google has [quota policies on size and frequency of inserts][quota policy].

In addition, the Streamer knows how to handle BigQuery server errors (HTTP 500 and the like),
and attempts to retry insertions several times on such failures.

It also sends errors on an error channel, which can be read an handled.

### MultiStreamer

A `MultiStreamer` operates multiple `Streamer`s concurrently (i.e. workers).
It reads rows and distributes them to the `Streamers`.

This allows insertion with a higher insert throughput,
where numerous workers are queueing rows and inserting concurrenctly.

Like `Streamer`, errors are reported from each worker and sent to a unified error channel,
where you can decide to read and handle them if necessary.

## Contribute

Please check the [issues][issues] page which might have some TODOs.
Feel free to file new bugs and ask for improvements. We welcome pull requests!

### Test

```bash
# Run unit tests, and check coverage.
$ go test -v -cover

# Run integration tests. This requires an active project, dataset and pem key.
# Make sure you edit the project, dataset, and table name in the .sh file.
$ ./integration_test.sh
$ ./multi_integration_test.sh
```


[godoc]: https://godoc.org/github.com/rounds/go-bqstreamer
[godoc image]: https://godoc.org/github.com/rounds/go-bqstreamer?status.svg

[travis image]: https://travis-ci.org/rounds/go-bqstreamer.svg
[travis]: https://travis-ci.org/rounds/go-bqstreamer

[coveralls image]: https://coveralls.io/repos/rounds/go-bqstreamer/badge.svg
[coveralls]: https://coveralls.io/r/rounds/go-bqstreamer

[stream insert]: https://cloud.google.com/bigquery/streaming-data-into-bigquery
[bigquery]: https://cloud.google.com/bigquery/
[quota policy]: https://cloud.google.com/bigquery/streaming-data-into-bigquery#quota
[credentials]: https://cloud.google.com/bigquery/authorization

[rounds]: http://rounds.com/
[blog post]: http://rounds.com/blog/collecting-user-data-and-usage/
[issues]: https://github.com/rounds/go-bqstreamer/issues

[gvm]: https://github.com/moovweb/gvm

[multi-streamer example]: multi_streamer_example_test.go
[streamer example]: streamer_example_test.go
