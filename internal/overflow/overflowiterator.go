package overflow

import (
	"fmt"
	"github.com/gostonefire/filehashmap/crt"
	"github.com/gostonefire/filehashmap/internal/model"
)

// Records - Is used to iterate over overflow records one by one.
type Records struct {
	getOvflFunc     func(int64) (model.Record, error)
	overflowAddress int64
}

// NewRecords - Returns a pointer to a new Records struct
func NewRecords(getOvflFunc func(int64) (model.Record, error), overflowAddress int64) *Records {

	return &Records{
		getOvflFunc:     getOvflFunc,
		overflowAddress: overflowAddress,
	}
}

// HasNext - Returns true if there are more records to be fetched from a call to Next.
func (O *Records) HasNext() bool {
	return O.overflowAddress != 0
}

// Next - Returns record.
// It returns:
//   - record is the next overflow record.
//   - err is either a standard error or if there are no more records when calling this function an error of type fhmerrors.NoRecordFound is returned.
func (O *Records) Next() (record model.Record, err error) {
	if O.overflowAddress == 0 {
		err = crt.NoRecordFound{}
		return
	}

	record, err = O.getOvflFunc(O.overflowAddress)
	if err != nil {
		err = fmt.Errorf("error while retrieving record from overflow file: %s", err)
		return
	}

	O.overflowAddress = record.NextOverflow

	return
}
