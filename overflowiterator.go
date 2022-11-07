package filehashmap

import (
	"fmt"
	"github.com/gostonefire/filehashmap/internal/file"
	"os"
)

// OverflowRecords - Is used to iterate over overflow records one by one.
type OverflowRecords struct {
	ovflFile        *os.File
	keyLength       int64
	valueLength     int64
	overflowAddress int64
}

// newOverflowRecords - Returns a pointer to a new OverflowRecords struct
func newOverflowRecords(ovflFile *os.File, overflowAddress, keyLength, valueLength int64) *OverflowRecords {

	return &OverflowRecords{
		ovflFile:        ovflFile,
		keyLength:       keyLength,
		valueLength:     valueLength,
		overflowAddress: overflowAddress,
	}
}

// hasNext - Returns true if there are more records to be fetched from a call to Next.
func (O *OverflowRecords) hasNext() bool {
	return O.overflowAddress != 0
}

// next - Returns record.
// It returns:
//   - record is the next overflow record.
//   - err is either a standard error or if there are no more records when calling this function an error of type fhmerrors.NoRecordFound is returned.
func (O *OverflowRecords) next() (record file.Record, err error) {
	if O.overflowAddress == 0 {
		err = NoRecordFound{}
		return
	}

	record, err = file.GetOverflowRecord(O.ovflFile, O.overflowAddress, O.keyLength, O.valueLength)
	if err != nil {
		err = fmt.Errorf("error while retrieving record from overflow file: %s", err)
		return
	}

	O.overflowAddress = record.NextOverflow

	return
}
