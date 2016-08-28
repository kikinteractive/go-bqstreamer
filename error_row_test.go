package bqstreamer

import (
	"testing"

	bigquery "google.golang.org/api/bigquery/v2"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRowErrors(t *testing.T) {
	t.Parallel()

	rowErrs := RowErrors{
		InsertID: "insertID",
		tableDataInsertAllResponseInsertErrors: bigquery.TableDataInsertAllResponseInsertErrors{
			Index: 1,
			Errors: []*bigquery.ErrorProto{
				&bigquery.ErrorProto{Location: "l1", Message: "m1", Reason: "r1"},
				&bigquery.ErrorProto{Location: "l2", Message: "m2", Reason: "r2"},
			}}}

	rowErrsCopy := rowErrs

	errs := rowErrs.All()
	require.Len(t, errs, 2)
	// Second call to All() should return an empty slice
	assert.Empty(t, rowErrs.All())

	// Compare all row errors.
	for i, err := range errs {
		// Errors are returned in reverse order.
		errCmp := rowErrsCopy.tableDataInsertAllResponseInsertErrors.Errors[len(errs)-1-i]

		assert.Equal(t, errCmp.DebugInfo, err.DebugInfo, i)
		assert.Equal(t, errCmp.Location, err.Location, i)
		assert.Equal(t, errCmp.Message, err.Message, i)
		assert.Equal(t, errCmp.Reason, err.Reason, i)
	}
}
