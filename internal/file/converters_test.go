//go:build unit

package file

import (
	"encoding/binary"
	"github.com/gostonefire/filehashmap/internal/conf"
	"github.com/gostonefire/filehashmap/internal/utils"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBytesToHeader(t *testing.T) {
	t.Run("converts between bytes and Header struct", func(t *testing.T) {
		// Prepare
		buf := make([]byte, conf.MapFileHeaderLength)
		buf[conf.BucketAlgorithmOffset] = 1
		binary.LittleEndian.PutUint64(buf[conf.InitialUniqueKeysOffset:], 1000)
		binary.LittleEndian.PutUint32(buf[conf.KeyLengthOffset:], 16)
		binary.LittleEndian.PutUint32(buf[conf.ValueLengthOffset:], 10)
		binary.LittleEndian.PutUint16(buf[conf.RecordsPerBucketOffset:], 2)
		binary.LittleEndian.PutUint64(buf[conf.NumberOfBucketsOffset:], 500)
		binary.LittleEndian.PutUint64(buf[conf.MinBucketNoOffset:], 0)
		binary.LittleEndian.PutUint64(buf[conf.MaxBucketNoOffset:], 499)
		binary.LittleEndian.PutUint64(buf[conf.FileSizeOffset:], 100000)

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
		header := Header{
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
		internalHash := buf[conf.BucketAlgorithmOffset] == 1
		initialUniqueValues := int64(binary.LittleEndian.Uint64(buf[conf.InitialUniqueKeysOffset:]))
		keyLength := int64(binary.LittleEndian.Uint32(buf[conf.KeyLengthOffset:]))
		valueLength := int64(binary.LittleEndian.Uint32(buf[conf.ValueLengthOffset:]))
		recordsPerBucket := int64(binary.LittleEndian.Uint16(buf[conf.RecordsPerBucketOffset:]))
		numberOfBuckets := int64(binary.LittleEndian.Uint64(buf[conf.NumberOfBucketsOffset:]))
		minBucketNo := int64(binary.LittleEndian.Uint64(buf[conf.MinBucketNoOffset:]))
		maxBucketNo := int64(binary.LittleEndian.Uint64(buf[conf.MaxBucketNoOffset:]))
		fileSize := int64(binary.LittleEndian.Uint64(buf[conf.FileSizeOffset:]))

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
		assert.Equal(t, 1000+conf.BucketHeaderLength, bucket.Records[0].RecordAddress)

		keyStart := conf.BucketHeaderLength + 1
		keyEnd := keyStart + 16
		valueStart := keyEnd
		valueEnd := valueStart + 10
		assert.True(t, utils.IsEqual(buf[keyStart:keyEnd], bucket.Records[0].Key), "key is correct in record")
		assert.True(t, utils.IsEqual(buf[valueStart:valueEnd], bucket.Records[0].Value), "value is correct in record")

		assert.False(t, bucket.Records[1].InUse)
		assert.Equal(t, 1000+conf.BucketHeaderLength+27, bucket.Records[1].RecordAddress)

		keyStart = conf.BucketHeaderLength + 1 + 27
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

		keyStart := conf.OverflowAddressLength + conf.InUseFlagBytes
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
		record := Record{
			InUse:         true,
			IsOverflow:    true,
			RecordAddress: 1000,
			NextOverflow:  2000,
			Key:           []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
			Value:         []byte{16, 17, 18, 19, 20, 21, 22, 23, 24, 25},
		}

		// Execute
		buf2 := recordToOverflowBytes(record, 16, 10)
		assert.Equal(t, conf.RecordInUse, buf2[conf.OverflowAddressLength])
		assert.Equal(t, uint64(2000), binary.LittleEndian.Uint64(buf2))

		keyStart := conf.OverflowAddressLength + conf.InUseFlagBytes
		keyEnd := keyStart + 16
		valueStart := keyEnd
		valueEnd := valueStart + 10
		assert.True(t, utils.IsEqual(buf2[keyStart:keyEnd], record.Key), "key is correct in record")
		assert.True(t, utils.IsEqual(buf2[valueStart:valueEnd], record.Value), "value is correct in record")
	})
}
