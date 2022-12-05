package separatechaining

import (
	"encoding/binary"
	"fmt"
	"github.com/gostonefire/filehashmap/internal/model"
)

// bytesToBucket - Converts bucket raw data to a Bucket struct
func bytesToBucket(buf []byte, bucketAddress, keyLength, valueLength int64) (bucket model.Bucket, err error) {
	overFlowAddress := int64(binary.LittleEndian.Uint64(buf[bucketOverflowAddressOffset:]))

	recordStart := bucketHeaderLength
	keyStart := 1 + recordStart // First byte is record state
	valueStart := keyStart + keyLength

	key := make([]byte, keyLength)
	value := make([]byte, valueLength)
	_ = copy(key, buf[keyStart:keyStart+keyLength])
	_ = copy(value, buf[valueStart:valueStart+valueLength])

	bucket = model.Bucket{
		Record: model.Record{
			State:         buf[recordStart],
			RecordAddress: bucketAddress + recordStart,
			Key:           key,
			Value:         value,
		},
		BucketAddress:   bucketAddress,
		OverflowAddress: overFlowAddress,
		HasOverflow:     overFlowAddress > 0,
	}

	return
}

// overflowBytesToRecord - Converts record raw data for overflow to Record struct
func overflowBytesToRecord(buf []byte, recordAddress, keyLength, valueLength int64) (record model.Record, err error) {
	actual := int64(len(buf))
	trueRecordLength := 1 + keyLength + valueLength // First byte is record state
	expected := trueRecordLength + overflowAddressLength

	if expected > actual {
		err = fmt.Errorf("length of data in buf (%d) less than overflow record size (%d)", actual, expected)
	}

	keyStart := 1 + overflowAddressLength // First byte is record state
	keyEnd := keyStart + keyLength
	valueStart := keyEnd

	key := make([]byte, keyLength)
	value := make([]byte, valueLength)
	_ = copy(key, buf[keyStart:keyStart+keyLength])
	_ = copy(value, buf[valueStart:valueStart+valueLength])

	record = model.Record{
		State:         buf[overflowAddressLength],
		IsOverflow:    true,
		RecordAddress: recordAddress,
		NextOverflow:  int64(binary.LittleEndian.Uint64(buf)),
		Key:           key,
		Value:         value,
	}

	return
}

// recordToOverflowBytes - Converts a Record struct for overflow to bytes
func recordToOverflowBytes(record model.Record, keyLength, valueLength int64) (buf []byte) {
	buf = make([]byte, 1+overflowAddressLength, keyLength+valueLength+overflowAddressLength) // First byte is record state
	binary.LittleEndian.PutUint64(buf, uint64(record.NextOverflow))
	buf[overflowAddressLength] = record.State
	buf = append(buf, record.Key...)
	buf = append(buf, record.Value...)

	return
}
