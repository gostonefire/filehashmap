//go:build unit

package separatechaining

import (
	"encoding/binary"
	"github.com/gostonefire/filehashmap/internal/model"
	"github.com/gostonefire/filehashmap/internal/utils"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBytesToBucket(t *testing.T) {
	t.Run("converts between bytes and Bucket struct", func(t *testing.T) {
		// Prepare
		buf := []byte{1, 0, 0, 0, 0, 0, 0, 0,
			1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25,
			1, 25, 24, 23, 22, 21, 20, 19, 18, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0}

		// execute
		bucket, err := bytesToBucket(buf, 1000, 2, 16, 10)

		// Check
		assert.NoError(t, err, "convert bytes to Bucket struct")
		assert.Equal(t, int64(1), bucket.OverflowAddress)
		assert.True(t, bucket.HasOverflow)
		assert.Equal(t, model.RecordOccupied, bucket.Records[0].State)
		assert.Equal(t, 1000+bucketHeaderLength, bucket.Records[0].RecordAddress)
		assert.Equal(t, model.RecordOccupied, bucket.Records[1].State)
		assert.Equal(t, 1000+bucketHeaderLength+27, bucket.Records[1].RecordAddress)

		keyStart := bucketHeaderLength + 1
		keyEnd := keyStart + 16
		valueStart := keyEnd
		valueEnd := valueStart + 10
		assert.True(t, utils.IsEqual(buf[keyStart:keyEnd], bucket.Records[0].Key), "key is correct in record")
		assert.True(t, utils.IsEqual(buf[valueStart:valueEnd], bucket.Records[0].Value), "value is correct in record")
		keyStart += 27
		keyEnd += 27
		valueStart += 27
		valueEnd += 27
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
		assert.Equal(t, model.RecordOccupied, record.State)
		assert.True(t, record.IsOverflow)
		assert.Equal(t, int64(1), record.NextOverflow)

		keyStart := overflowAddressLength + 1
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
			State:         model.RecordOccupied,
			IsOverflow:    true,
			RecordAddress: 1000,
			NextOverflow:  2000,
			Key:           []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
			Value:         []byte{16, 17, 18, 19, 20, 21, 22, 23, 24, 25},
		}

		// Execute
		buf2 := recordToOverflowBytes(record, 16, 10)
		assert.Equal(t, model.RecordOccupied, buf2[overflowAddressLength])
		assert.Equal(t, uint64(2000), binary.LittleEndian.Uint64(buf2))

		keyStart := overflowAddressLength + 1
		keyEnd := keyStart + 16
		valueStart := keyEnd
		valueEnd := valueStart + 10
		assert.True(t, utils.IsEqual(buf2[keyStart:keyEnd], record.Key), "key is correct in record")
		assert.True(t, utils.IsEqual(buf2[valueStart:valueEnd], record.Value), "value is correct in record")
	})
}
