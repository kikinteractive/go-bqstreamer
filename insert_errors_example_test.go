package bqstreamer

import (
	"log"

	"golang.org/x/oauth2"
	bigquery "google.golang.org/api/bigquery/v2"
)

// This example demonstrates how to handle insert errors,
// returned from an insert operation.
func ExampleInsertErrors() {
	jwtConfig, err := NewJWTConfig("path_to_key.json")
	if err != nil {
		log.Fatalln(err)
	}

	w, err := NewSyncWorker(jwtConfig.Client(oauth2.NoContext))
	if err != nil {
		log.Fatalln(err)
	}

	// Enqueue rows for multiple tables.
	w.Enqueue(NewRow("my-project", "my-dataset", "my-table-1", map[string]bigquery.JsonValue{"key-1": "value-1"}))
	w.Enqueue(NewRow("my-project", "my-dataset", "my-table-2", map[string]bigquery.JsonValue{"key-2": "value-2"}))

	// Perform an insert operation
	insertErrs := w.Insert()

	// Go over all tables' insert attempts, and log all errors.
	for _, table := range insertErrs.All() {
		for _, attempt := range table.Attempts() {
			// Log insert attempt error.
			if err := attempt.Error(); err != nil {
				log.Printf("%s.%s.%s: %s\n", attempt.Project, attempt.Dataset, attempt.Table, err)
			}

			// Iterate over all rows in attempt.
			for _, row := range attempt.All() {
				// Iterate over all errors in row and log.
				for _, err := range row.All() {
					log.Printf("%s.%s.%s[%s]: %s\n", attempt.Project, attempt.Dataset, attempt.Table, row.InsertID, err)
				}
			}
		}
	}
}
