package bqstreamer

import bigquery "google.golang.org/api/bigquery/v2"

type projects map[string]project
type project map[string]dataset
type dataset map[string]table
type table []*bigquery.TableDataInsertAllRequestRows
