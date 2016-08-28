package bqstreamer

// InsertErrors is returned from an insert attempt.
// It provides functions to iterate over errors relating to an insert
// operation.
//
// BigQuery has a complex error hierarchy:
//
//  Insert --> Tables --> Table Insert Attempt --> Rows --> Row Error
//
// During an insert operation, multiple tables are inserted in bulk
// into BigQuery using a separate request for each table.
// Each table-specific insert operation is comprised from multiple
// insert attempts (requests).
// Every table insert can be retried until an attempt is successful
// or too many attempts have failed. Failures can occur for various
// reasons e.g. server errors, malformed payload, etc.
//
// This interface allows to iterate over all tables, attempts, rows,
// and row errors associated with an insert operation.
type InsertErrors struct {
	Tables []*TableInsertErrors
}

// Next iterates over all inserted tables once,
// returning a single TableInsertErrors every call.
// Calling Next() multiple times will consequently return more tables,
// until all have been returned.
//
// The function returns true if a non-nil value was fetched.
// Once the iterator has been exhausted, (nil, false) will be returned
// on every subsequent call.
func (insert *InsertErrors) Next() (*TableInsertErrors, bool) {
	if len(insert.Tables) == 0 {
		return nil, false
	}

	// Return elements in reverse order for memory efficiency.
	var table *TableInsertErrors
	table, insert.Tables = insert.Tables[len(insert.Tables)-1], insert.Tables[:len(insert.Tables)-1]
	return table, true
}

// All returns all remaining tables
// (those that have not been iterated over using Next()).
//
// Calling Next() or All() again afterwards will yield a failed (empty)
// result.
func (insert *InsertErrors) All() []*TableInsertErrors {
	var tables []*TableInsertErrors
	for {
		table, ok := insert.Next()
		if !ok {
			break
		}
		tables = append(tables, table)
	}
	return tables
}
