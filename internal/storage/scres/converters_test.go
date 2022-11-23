//go:build unit

package scres

import (
	"encoding/binary"
	"github.com/gostonefire/filehashmap/internal/model"
	"github.com/gostonefire/filehashmap/internal/utils"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBytesToHeader(t *testing.T) {
	t.Run("converts between bytes and Header struct", func(t *testing.T) {
		// Prepare
		buf := make([]byte, mapFileHeaderLength)
		buf[bucketAlgorithmOffset] = 1
		binary.LittleEndian.PutUint64(buf[initialUniqueKeysOffset:], 1000)
		binary.LittleEndian.PutUint32(buf[keyLengthOffset:], 16)
		binary.LittleEndian.PutUint32(buf[valueLengthOffset:], 10)
		binary.LittleEndian.PutUint16(buf[recordsPerBucketOffset:], 2)
		binary.LittleEndian.PutUint64(buf[numberOfBucketsOffset:], 500)
		binary.LittleEndian.PutUint64(buf[minBucketNoOffset:], 0)
		binary.LittleEndian.PutUint64(buf[maxBucketNoOffset:], 499)
		binary.LittleEndian.PutUint64(buf[fileSizeOffset:], 100000)

		// execute
		header := bytesToHeader(buf)

		// Check
		assert.True(t, header.InternalAlg)
		assert.Equal(t, int64(1000), header.InitialUniqueKeys)
		assert.Equal(t, int64(16), header.KeyLength)
		assert.Equal(t, int64(10), header.ValueLength)
		assert.Equal(t, int64(2), header.RecordsPerBucket)
		assert.Equal(t, int64(500), header.NumberOfBuckets)
		assert.Equal(t, int64(0), header.MinBucketNo)
		assert.Equal(t, int64(499), header.MaxBucketNo)
		assert.Equal(t, int64(100000), header.FileSize)
	})
}

func TestHeaderToBytes(t *testing.T) {
	t.Run("converts between bytes and Header struct", func(t *testing.T) {
		// Prepare
		header := model.Header{
			InternalAlg:       true,
			InitialUniqueKeys: 1000,
			KeyLength:         16,
			RecordsPerBucket:  2,
			NumberOfBuckets:   500,
			MinBucketNo:       0,
			MaxBucketNo:       499,
			FileSize:          100000,
		}

		// Execute
		buf := headerToBytes(header)

		// Check
		internalHash := buf[bucketAlgorithmOffset] == 1
		initialUniqueValues := int64(binary.LittleEndian.Uint64(buf[initialUniqueKeysOffset:]))
		keyLength := int64(binary.LittleEndian.Uint32(buf[keyLengthOffset:]))
		valueLength := int64(binary.LittleEndian.Uint32(buf[valueLengthOffset:]))
		recordsPerBucket := int64(binary.LittleEndian.Uint16(buf[recordsPerBucketOffset:]))
		numberOfBuckets := int64(binary.LittleEndian.Uint64(buf[numberOfBucketsOffset:]))
		minBucketNo := int64(binary.LittleEndian.Uint64(buf[minBucketNoOffset:]))
		maxBucketNo := int64(binary.LittleEndian.Uint64(buf[maxBucketNoOffset:]))
		fileSize := int64(binary.LittleEndian.Uint64(buf[fileSizeOffset:]))

		assert.True(t, internalHash)
		assert.Equal(t, header.InitialUniqueKeys, initialUniqueValues)
		assert.Equal(t, header.KeyLength, keyLength)
		assert.Equal(t, header.ValueLength, valueLength)
		assert.Equal(t, header.RecordsPerBucket, recordsPerBucket)
		assert.Equal(t, header.NumberOfBuckets, numberOfBuckets)
		assert.Equal(t, header.MinBucketNo, minBucketNo)
		assert.Equal(t, header.MaxBucketNo, maxBucketNo)
		assert.Equal(t, header.FileSize, fileSize)
	})
}

func TestBytesToBucket(t *testing.T) {
	t.Run("converts between bytes and Bucket struct", func(t *testing.T) {
		// Prepare
		buf := []byte{1, 0, 0, 0, 0, 0, 0, 0,
			1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25,
			0, 25, 24, 23, 22, 21, 20, 19, 18, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0}

		// execute
		bucket, err := bytesToBucket(buf, 1000, 16, 10, 2)

		// Check
		assert.NoError(t, err, "convert bytes to Bucket struct")
		assert.Equal(t, int64(1), bucket.OverflowAddress)
		assert.True(t, bucket.HasOverflow)
		assert.Equal(t, 2, len(bucket.Records), "two records in bucket")
		assert.True(t, bucket.Records[0].InUse)
		assert.Equal(t, 1000+bucketHeaderLength, bucket.Records[0].RecordAddress)

		keyStart := bucketHeaderLength + 1
		keyEnd := keyStart + 16
		valueStart := keyEnd
		valueEnd := valueStart + 10
		assert.True(t, utils.IsEqual(buf[keyStart:keyEnd], bucket.Records[0].Key), "key is correct in record")
		assert.True(t, utils.IsEqual(buf[valueStart:valueEnd], bucket.Records[0].Value), "value is correct in record")

		assert.False(t, bucket.Records[1].InUse)
		assert.Equal(t, 1000+bucketHeaderLength+27, bucket.Records[1].RecordAddress)

		keyStart = bucketHeaderLength + 1 + 27
		keyEnd = keyStart + 16
		valueStart = keyEnd
		valueEnd = valueStart + 10
		assert.True(t, utils.IsEqual(buf[keyStart:keyEnd], bucket.Records[1].Key), "key is correct in record")
		assert.True(t, utils.IsEqual(buf[valueStart:valueEnd], bucket.Records[1].Value), "value is correct in record")
	})
}

func TestOverflowBytesToRecord(t *testing.T) {
	t.Run("converts overflow bytes to Record struct", func(t *testing.T) {
		// Prepare
		buf := []byte{1, 0, 0, 0, 0, 0, 0, 0,
			1,
			0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25}

		// Execute
		record, err := overflowBytesToRecord(buf, 1000, 16, 10)

		// Check
		assert.NoError(t, err, "convert bytes to Record struct")
		assert.True(t, record.InUse)
		assert.True(t, record.IsOverflow)
		assert.Equal(t, int64(1), record.NextOverflow)

		keyStart := overflowAddressLength + inUseFlagBytes
		keyEnd := keyStart + 16
		valueStart := keyEnd
		valueEnd := valueStart + 10
		assert.True(t, utils.IsEqual(buf[keyStart:keyEnd], record.Key), "key is correct in record")
		assert.True(t, utils.IsEqual(buf[valueStart:valueEnd], record.Value), "value is correct in record")
	})
}

func TestRecordToOverflowBytes(t *testing.T) {
	t.Run("converts Record struct to overflow bytes", func(t *testing.T) {
		// Prepare
		record := model.Record{
			InUse:         true,
			IsOverflow:    true,
			RecordAddress: 1000,
			NextOverflow:  2000,
			Key:           []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
			Value:         []byte{16, 17, 18, 19, 20, 21, 22, 23, 24, 25},
		}

		// Execute
		buf2 := recordToOverflowBytes(record, 16, 10)
		assert.Equal(t, recordInUse, buf2[overflowAddressLength])
		assert.Equal(t, uint64(2000), binary.LittleEndian.Uint64(buf2))

		keyStart := overflowAddressLength + inUseFlagBytes
		keyEnd := keyStart + 16
		valueStart := keyEnd
		valueEnd := valueStart + 10
		assert.True(t, utils.IsEqual(buf2[keyStart:keyEnd], record.Key), "key is correct in record")
		assert.True(t, utils.IsEqual(buf2[valueStart:valueEnd], record.Value), "value is correct in record")
	})
}
