//go:build unit

package scres

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
			1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25}

		// execute
		bucket, err := bytesToBucket(buf, 1000, 16, 10)

		// Check
		assert.NoError(t, err, "convert bytes to Bucket struct")
		assert.Equal(t, int64(1), bucket.OverflowAddress)
		assert.True(t, bucket.HasOverflow)
		assert.Equal(t, model.RecordOccupied, bucket.Record.State)
		assert.Equal(t, 1000+bucketHeaderLength, bucket.Record.RecordAddress)

		keyStart := bucketHeaderLength + stateBytes
		keyEnd := keyStart + 16
		valueStart := keyEnd
		valueEnd := valueStart + 10
		assert.True(t, utils.IsEqual(buf[keyStart:keyEnd], bucket.Record.Key), "key is correct in record")
		assert.True(t, utils.IsEqual(buf[valueStart:valueEnd], bucket.Record.Value), "value is correct in record")
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

		keyStart := overflowAddressLength + stateBytes
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

		keyStart := overflowAddressLength + stateBytes
		keyEnd := keyStart + 16
		valueStart := keyEnd
		valueEnd := valueStart + 10
		assert.True(t, utils.IsEqual(buf2[keyStart:keyEnd], record.Key), "key is correct in record")
		assert.True(t, utils.IsEqual(buf2[valueStart:valueEnd], record.Value), "value is correct in record")
	})
}
