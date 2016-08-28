// Package bqstreamer implements synchronous and asynchronous stream-inserters for Google BigQuery.
//
// https://cloud.google.com/bigquery/
//
// Stream-insert is performed using InsertAll():
// https://cloud.google.com/bigquery/docs/reference/v2/tabledata/insertAll
//
// This packages provides two inserter types that can be used to insert rows
// and tables:
//
// 1. SyncWorker
//  - A single blocking (synchronous) worker.
//  - Enqueues rows and performs insert operations in a blocking manner.
// 2. AsyncWorkerGroup
//  - Wraps multiple SyncWorkers.
//  - Enqueues rows and performs insert operations in the background.
//  - Background workers execute insert operations according to amount
//    of enqueued rows or time thresholds.
//  - Errors are reported to an error channel for processing by the user.
//  - This provides a higher insert throughput for larger scale scenarios.
package bqstreamer
