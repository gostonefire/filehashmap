//go:build unit

package lpres

import (
	"github.com/gostonefire/filehashmap/internal/model"
	"github.com/gostonefire/filehashmap/internal/utils"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBytesToBucket(t *testing.T) {
	t.Run("converts between bytes and Bucket struct", func(t *testing.T) {
		// Prepare
		buf := []byte{1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25}

		// execute
		bucket, err := bytesToBucket(buf, 1000, 16, 10)

		// Check
		assert.NoError(t, err, "convert bytes to Bucket struct")
		assert.Equal(t, int64(0), bucket.OverflowAddress)
		assert.False(t, bucket.HasOverflow)
		assert.Equal(t, model.RecordOccupied, bucket.Record.State)
		assert.Equal(t, int64(1000), bucket.Record.RecordAddress)

		keyStart := stateBytes
		keyEnd := keyStart + 16
		valueStart := keyEnd
		valueEnd := valueStart + 10
		assert.True(t, utils.IsEqual(buf[keyStart:keyEnd], bucket.Record.Key), "key is correct in record")
		assert.True(t, utils.IsEqual(buf[valueStart:valueEnd], bucket.Record.Value), "value is correct in record")
	})
}
