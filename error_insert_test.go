package bqstreamer

import (
	errs "errors"
	"strconv"
	"testing"

	bigquery "google.golang.org/api/bigquery/v2"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInsertErrors(t *testing.T) {
	t.Parallel()

	assert := assert.New(t)
	require := require.New(t)

	generateInsertErrors := func() *InsertErrors {
		// Insert errors include two tables,
		// where the first ones has two insert attempts,
		// and the second table has a single insert attempt.
		return &InsertErrors{
			[]*TableInsertErrors{
				&TableInsertErrors{
					InsertAttempts: []*TableInsertAttemptErrors{
						&TableInsertAttemptErrors{
							err:       errs.New("test error t1a1"),
							rows:      nil, // No row errors.
							insertIDs: nil,
							Table:     "t1",
							Dataset:   "d1",
							Project:   "p1",
						},
						&TableInsertAttemptErrors{
							err:       nil,
							rows:      nil, // No row errors.
							insertIDs: nil,
							Table:     "t1",
							Dataset:   "d1",
							Project:   "p1",
						},
					},
				},
				&TableInsertErrors{
					InsertAttempts: []*TableInsertAttemptErrors{
						&TableInsertAttemptErrors{
							err:     errs.New("test error t2a1"),
							Table:   "t2",
							Dataset: "d1",
							Project: "p1",
							// Errors for rows 0, 1, 4.
							insertIDs: []string{"0", "1", "2", "3", "4"},
							rows: []*bigquery.TableDataInsertAllResponseInsertErrors{
								&bigquery.TableDataInsertAllResponseInsertErrors{
									Index: 0,
									Errors: []*bigquery.ErrorProto{
										&bigquery.ErrorProto{Location: "l00", Message: "m00", Reason: "invalid"},
										&bigquery.ErrorProto{Location: "l01", Message: "m01", Reason: "r01"},
										&bigquery.ErrorProto{Location: "l02", Message: "m02", Reason: "invalid"},
										&bigquery.ErrorProto{Location: "l03", Message: "m03", Reason: "r03"},
										&bigquery.ErrorProto{Location: "l04", Message: "m04", Reason: "invalid"},
									}},

								&bigquery.TableDataInsertAllResponseInsertErrors{
									Index: 1,
									Errors: []*bigquery.ErrorProto{
										&bigquery.ErrorProto{Location: "l10", Message: "m10", Reason: "r10"},
										&bigquery.ErrorProto{Location: "l11", Message: "m11", Reason: "invalid"},
									}},

								&bigquery.TableDataInsertAllResponseInsertErrors{
									Index: 2,
									Errors: []*bigquery.ErrorProto{
										&bigquery.ErrorProto{Location: "l20", Message: "m20", Reason: "stopped"},
									},
								},

								&bigquery.TableDataInsertAllResponseInsertErrors{
									Index: 3,
									Errors: []*bigquery.ErrorProto{
										&bigquery.ErrorProto{Location: "l30", Message: "m30", Reason: "stopped"},
									},
								},

								&bigquery.TableDataInsertAllResponseInsertErrors{
									Index: 4,
									Errors: []*bigquery.ErrorProto{
										&bigquery.ErrorProto{Location: "l40", Message: "m40", Reason: "invalid"},
									},
								},
							},
						},
					},
				},
			},
		}
	}

	insertErrs := generateInsertErrors()
	tables := insertErrs.All()
	// Second call to All() should return an empty slice
	assert.Empty(insertErrs.All())

	// Iterate over both tables
	// and construct a new insertErrors object using their interface All()
	// functions.
	//
	// This practically deep copies the insert errors using interface functions
	// and compares the result to the original insert errors.
	require.Len(tables, 2)
	var ts []*TableInsertErrors
	for i, table := range tables {
		ts = append(ts, &TableInsertErrors{})

		tableNum := len(tables) - 1 - i // Tables are returned in reverse order.
		switch tableNum {
		case 0:
			require.Len(table.Attempts(), 2, strconv.Itoa(tableNum))
		case 1:
			require.Len(table.Attempts(), 1, strconv.Itoa(tableNum))
		}

		for _, attempt := range table.Attempts() {
			rows := attempt.All()
			// Second call to All() should return an empty slice
			assert.Empty(attempt.All(), strconv.Itoa(i))

			var (
				rs  []*bigquery.TableDataInsertAllResponseInsertErrors
				ids []string
			)
			for _, row := range rows {
				rs = append(rs, &row.tableDataInsertAllResponseInsertErrors)
				ids = append(ids, row.InsertID)
			}
			// Reverse row and insert order, because results from TableErrors are returned in reverse order.
			for l, r := 0, len(rs)-1; l < r; l, r = l+1, r-1 {
				rs[l], rs[r] = rs[r], rs[l]
			}
			for l, r := 0, len(ids)-1; l < r; l, r = l+1, r-1 {
				ids[l], ids[r] = ids[r], ids[l]
			}

			// Push current table insert attempt to table inserts.
			ts[i].InsertAttempts = append(ts[i].InsertAttempts, &TableInsertAttemptErrors{
				err:       attempt.Error(),
				rows:      rs,
				insertIDs: ids,
				Table:     attempt.Table,
				Dataset:   attempt.Dataset,
				Project:   attempt.Project,
			})
		}
	}

	// Reverse tables for the same reason as above.
	for l, r := 0, len(ts)-1; l < r; l, r = l+1, r-1 {
		ts[l], ts[r] = ts[r], ts[l]
	}

	// Remove the good, non-failed rows before comparing.
	cmpInsertErrs := generateInsertErrors()
	cmpInsertErrs.Tables[1].InsertAttempts[0].insertIDs = []string{"0", "1", "2", "3", "4"}

	// Compare constructed insertErrors and the original one.
	assert.EqualValues(cmpInsertErrs, &InsertErrors{ts})
}
