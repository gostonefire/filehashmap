package scres

import (
	"fmt"
	"github.com/gostonefire/filehashmap/internal/model"
	"github.com/gostonefire/filehashmap/storage"
)

// OverflowRecords - Is used to iterate over overflow records one by one.
type OverflowRecords struct {
	scFiles         *SCFiles
	overflowAddress int64
}

// newOverflowRecords - Returns a pointer to a new OverflowRecords struct
func newOverflowRecords(scFiles *SCFiles, overflowAddress int64) *OverflowRecords {

	return &OverflowRecords{
		scFiles:         scFiles,
		overflowAddress: overflowAddress,
	}
}

// HasNext - Returns true if there are more records to be fetched from a call to Next.
func (O *OverflowRecords) HasNext() bool {
	return O.overflowAddress != 0
}

// Next - Returns record.
// It returns:
//   - record is the next overflow record.
//   - err is either a standard error or if there are no more records when calling this function an error of type fhmerrors.NoRecordFound is returned.
func (O *OverflowRecords) Next() (record model.Record, err error) {
	if O.overflowAddress == 0 {
		err = storage.NoRecordFound{}
		return
	}

	record, err = O.scFiles.getOverflowRecord(O.overflowAddress)
	if err != nil {
		err = fmt.Errorf("error while retrieving record from overflow file: %s", err)
		return
	}

	O.overflowAddress = record.NextOverflow

	return
}
