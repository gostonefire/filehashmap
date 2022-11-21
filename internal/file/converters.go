package file

import (
	"encoding/binary"
	"fmt"
	"github.com/gostonefire/filehashmap/internal/conf"
	"github.com/gostonefire/filehashmap/internal/model"
)

// bytesToHeader - Converts a slice of bytes to a Header struct
func bytesToHeader(buf []byte) (header model.Header) {
	header = model.Header{
		InternalAlg:       buf[conf.BucketAlgorithmOffset] == 1,
		InitialUniqueKeys: int64(binary.LittleEndian.Uint64(buf[conf.InitialUniqueKeysOffset:])),
		KeyLength:         int64(binary.LittleEndian.Uint32(buf[conf.KeyLengthOffset:])),
		ValueLength:       int64(binary.LittleEndian.Uint32(buf[conf.ValueLengthOffset:])),
		RecordsPerBucket:  int64(binary.LittleEndian.Uint16(buf[conf.RecordsPerBucketOffset:])),
		NumberOfBuckets:   int64(binary.LittleEndian.Uint64(buf[conf.NumberOfBucketsOffset:])),
		MinBucketNo:       int64(binary.LittleEndian.Uint64(buf[conf.MinBucketNoOffset:])),
		MaxBucketNo:       int64(binary.LittleEndian.Uint64(buf[conf.MaxBucketNoOffset:])),
		FileSize:          int64(binary.LittleEndian.Uint64(buf[conf.FileSizeOffset:])),
	}

	return
}

// headerToBytes - Converts a Header struct to a slice of bytes
func headerToBytes(header model.Header) (buf []byte) {
	// Create byte buffer
	buf = make([]byte, conf.MapFileHeaderLength)

	if header.InternalAlg {
		buf[conf.BucketAlgorithmOffset] = 1
	}

	binary.LittleEndian.PutUint64(buf[conf.InitialUniqueKeysOffset:], uint64(header.InitialUniqueKeys))
	binary.LittleEndian.PutUint32(buf[conf.KeyLengthOffset:], uint32(header.KeyLength))
	binary.LittleEndian.PutUint32(buf[conf.ValueLengthOffset:], uint32(header.ValueLength))
	binary.LittleEndian.PutUint16(buf[conf.RecordsPerBucketOffset:], uint16(header.RecordsPerBucket))
	binary.LittleEndian.PutUint64(buf[conf.NumberOfBucketsOffset:], uint64(header.NumberOfBuckets))
	binary.LittleEndian.PutUint64(buf[conf.MinBucketNoOffset:], uint64(header.MinBucketNo))
	binary.LittleEndian.PutUint64(buf[conf.MaxBucketNoOffset:], uint64(header.MaxBucketNo))
	binary.LittleEndian.PutUint64(buf[conf.FileSizeOffset:], uint64(header.FileSize))

	return
}

// bytesToBucket - Converts bucket raw data to a Bucket struct
func bytesToBucket(buf []byte, bucketAddress, keyLength, valueLength, recordsPerBucket int64) (bucket model.Bucket, err error) {
	actual := int64(len(buf))
	trueRecordLength := keyLength + valueLength + conf.InUseFlagBytes
	expected := trueRecordLength*recordsPerBucket + conf.BucketHeaderLength

	if expected > actual {
		err = fmt.Errorf("length of data in buf (%d) less than bucket size (%d)", actual, expected)
	}

	overFlowAddress := int64(binary.LittleEndian.Uint64(buf[conf.BucketOverflowAddressOffset:]))

	recordStart := conf.BucketHeaderLength
	keyStart := recordStart + conf.InUseFlagBytes
	valueStart := keyStart + keyLength
	records := make([]model.Record, recordsPerBucket)
	for i := int64(0); i < recordsPerBucket; i++ {
		key := make([]byte, keyLength)
		value := make([]byte, valueLength)
		_ = copy(key, buf[keyStart:keyStart+keyLength])
		_ = copy(value, buf[valueStart:valueStart+valueLength])

		records[i] = model.Record{
			InUse:         buf[recordStart] == conf.RecordInUse,
			RecordAddress: bucketAddress + recordStart,
			Key:           key,
			Value:         value,
		}

		recordStart += trueRecordLength
		keyStart += trueRecordLength
		valueStart += trueRecordLength
	}

	bucket = model.Bucket{
		Records:         records,
		BucketAddress:   bucketAddress,
		OverflowAddress: overFlowAddress,
		HasOverflow:     overFlowAddress > 0,
	}

	return
}

// overflowBytesToRecord - Converts record raw data for overflow to Record struct
func overflowBytesToRecord(buf []byte, recordAddress, keyLength, valueLength int64) (record model.Record, err error) {
	actual := int64(len(buf))
	trueRecordLength := keyLength + valueLength + conf.InUseFlagBytes
	expected := trueRecordLength + conf.OverflowAddressLength

	if expected > actual {
		err = fmt.Errorf("length of data in buf (%d) less than overflow record size (%d)", actual, expected)
	}

	keyStart := conf.OverflowAddressLength + conf.InUseFlagBytes
	keyEnd := keyStart + keyLength
	valueStart := keyEnd

	key := make([]byte, keyLength)
	value := make([]byte, valueLength)
	_ = copy(key, buf[keyStart:keyStart+keyLength])
	_ = copy(value, buf[valueStart:valueStart+valueLength])

	record = model.Record{
		InUse:         buf[conf.OverflowAddressLength] == conf.RecordInUse,
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
	buf = make([]byte, conf.OverflowAddressLength+conf.InUseFlagBytes, keyLength+valueLength+conf.OverflowAddressLength)
	binary.LittleEndian.PutUint64(buf, uint64(record.NextOverflow))
	if record.InUse {
		buf[conf.OverflowAddressLength] = conf.RecordInUse
	}
	buf = append(buf, record.Key...)
	buf = append(buf, record.Value...)

	return
}
