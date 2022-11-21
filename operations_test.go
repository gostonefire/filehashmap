//go:build integration

package filehashmap

import (
	"fmt"
	"github.com/gostonefire/filehashmap/internal/conf"
	"github.com/gostonefire/filehashmap/internal/model"
	"github.com/gostonefire/filehashmap/internal/utils"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"os"
	"testing"
)

func TestSet(t *testing.T) {
	t.Run("sets a new record to file", func(t *testing.T) {
		// Prepare
		fhm, _, err := NewFileHashMap(testHashMap, 10000, 16, 10, nil)
		assert.NoError(t, err, "create new file hash map struct")

		err = fhm.CreateNewFiles()
		assert.NoError(t, err, "create new files")

		buf := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25}

		// Execute
		err = fhm.Set(buf[:16], buf[16:])

		// Check
		assert.NoError(t, err, "set a record to file")

		record, err := fhm.get(buf[:16])
		assert.NoError(t, err, "get records from file")
		assert.True(t, record.InUse, "first record is in use")
		assert.True(t, utils.IsEqual(buf[:16], record.Key), "key is correct")
		assert.True(t, utils.IsEqual(buf[16:], record.Value), "value is correct")

		// Clean up
		err = fhm.RemoveFiles()
		assert.NoError(t, err, "files can be removed after close")
		_, err = os.Stat(fhm.ovflFileName)
		assert.Error(t, err, "overflow file is removed")
		_, err = os.Stat(fhm.mapFileName)
		assert.Error(t, err, "map file is removed")
	})

	t.Run("updates an existing record in file", func(t *testing.T) {
		// Prepare
		fhm, _, err := NewFileHashMap(testHashMap, 10000, 16, 10, nil)
		assert.NoError(t, err, "create new file hash map struct")

		err = fhm.CreateNewFiles()
		assert.NoError(t, err, "create new files")

		buf1 := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25}
		buf2 := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 25, 24, 23, 22, 21, 20, 19, 18, 17, 16}

		err = fhm.Set(buf1[:16], buf1[16:])
		assert.NoError(t, err, "set a record to file")

		// Execute
		err = fhm.Set(buf2[:16], buf2[16:])

		// Check
		assert.NoError(t, err, "update an existing record in file")

		record, err := fhm.get(buf1[:16])
		assert.NoError(t, err, "get records from file")
		assert.True(t, record.InUse, "first record is in use")
		assert.True(t, utils.IsEqual(buf1[:16], record.Key), "key is correct")
		assert.True(t, utils.IsEqual(buf2[16:], record.Value), "value is correct")

		// Clean up
		err = fhm.RemoveFiles()
		assert.NoError(t, err, "files can be removed after close")
		_, err = os.Stat(fhm.ovflFileName)
		assert.Error(t, err, "overflow file is removed")
		_, err = os.Stat(fhm.mapFileName)
		assert.Error(t, err, "map file is removed")
	})

	t.Run("sets records to overflow file", func(t *testing.T) {
		// Prepare
		fhm, _, err := NewFileHashMap(testHashMap, 10, 16, 10, nil)
		assert.NoError(t, err, "create new file hash map struct")

		err = fhm.CreateNewFiles()
		assert.NoError(t, err, "create new files")

		buf0 := []byte{107, 68, 126, 11, 54, 204, 206, 242, 54, 35, 190, 163, 22, 221, 129, 95, 14, 31, 161, 110, 20, 250, 111, 111, 75, 195}
		buf1 := []byte{97, 130, 71, 240, 237, 168, 227, 220, 123, 204, 120, 55, 166, 202, 244, 133, 75, 246, 148, 41, 29, 111, 226, 56, 159, 48}
		buf2 := []byte{135, 25, 58, 216, 126, 252, 30, 22, 119, 3, 70, 135, 75, 111, 50, 236, 249, 241, 208, 233, 107, 199, 142, 168, 77, 12}
		buf3 := []byte{88, 153, 223, 103, 228, 229, 153, 24, 227, 106, 50, 192, 116, 195, 253, 157, 14, 123, 150, 14, 62, 159, 4, 145, 78, 68}
		buf4 := []byte{172, 117, 190, 175, 40, 161, 42, 110, 8, 91, 95, 184, 30, 18, 157, 240, 38, 158, 200, 22, 146, 167, 179, 94, 244, 178}
		buf := [5][]byte{buf0, buf1, buf2, buf3, buf4}

		// Execute
		for i, b := range buf {
			err = fhm.Set(b[:16], b[16:])
			assert.NoError(t, err, fmt.Sprintf("set record %d in file", i))
		}

		// Check
		var record model.Record
		for i := 0; i < 2; i++ {
			record, err = fhm.get(buf[i][:16])
			assert.NoError(t, err, fmt.Sprintf("get record %d in file", i))
			assert.True(t, record.InUse, fmt.Sprintf("record %d in file in use", i))
			assert.False(t, record.IsOverflow, fmt.Sprintf("record %d not overflow", i))
			assert.NotEqual(t, int64(0), record.RecordAddress, fmt.Sprintf("record %d in file has address", i))
			assert.Equal(t, int64(0), record.NextOverflow, fmt.Sprintf("record %d has no overflow address", i))
			assert.Truef(t, utils.IsEqual(buf[i][:16], record.Key), "key is correct in record %d", i)
			assert.Truef(t, utils.IsEqual(buf[i][16:], record.Value), "value is correct in record %d", i)
		}
		for i := 2; i < 5; i++ {
			record, err = fhm.get(buf[i][:16])
			assert.NoError(t, err, fmt.Sprintf("get record %d in overflow file", i))
			assert.True(t, record.InUse, fmt.Sprintf("record %d in overflow file in use", i))
			assert.True(t, record.IsOverflow, fmt.Sprintf("record %d is overflow", i))
			assert.NotEqual(t, int64(0), record.RecordAddress, fmt.Sprintf("record %d in overflow file has address", i))
			if i < 4 {
				assert.NotEqual(t, int64(0), record.NextOverflow, fmt.Sprintf("record %d has overflow address", i))
			} else {
				assert.Equal(t, int64(0), record.NextOverflow, fmt.Sprintf("record %d has no overflow address", i))
			}
			assert.Truef(t, utils.IsEqual(buf[i][:16], record.Key), "key is correct in record %d", i)
			assert.Truef(t, utils.IsEqual(buf[i][16:], record.Value), "value is correct in record %d", i)
		}

		// Clean up
		err = fhm.RemoveFiles()
		assert.NoError(t, err, "files can be removed after close")
		_, err = os.Stat(fhm.ovflFileName)
		assert.Error(t, err, "overflow file is removed")
		_, err = os.Stat(fhm.mapFileName)
		assert.Error(t, err, "map file is removed")
	})

	t.Run("throws correct error when key is not found", func(t *testing.T) {
		// Prepare
		fhm, _, err := NewFileHashMap(testHashMap, 10000, 16, 10, nil)
		assert.NoError(t, err, "create new file hash map struct")

		err = fhm.CreateNewFiles()
		assert.NoError(t, err, "create new files")

		buf := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25}

		// Execute
		_, err = fhm.Get(buf[:16])

		// Check
		assert.ErrorIs(t, err, NoRecordFound{}, "get correct error")

		// Clean up
		err = fhm.RemoveFiles()
		assert.NoError(t, err, "files can be removed after close")
		_, err = os.Stat(fhm.ovflFileName)
		assert.Error(t, err, "overflow file is removed")
		_, err = os.Stat(fhm.mapFileName)
		assert.Error(t, err, "map file is removed")
	})

}

func TestGetBucketNo(t *testing.T) {
	t.Run("gets a correct bucket number", func(t *testing.T) {
		// Prepare
		fhm, _, err := NewFileHashMap(testHashMap, 10000, 16, 10, nil)
		assert.NoError(t, err)

		buf := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}

		// Execute
		bucketNo, err := fhm.GetBucketNo(buf)

		// Check
		assert.NoError(t, err, "get bucket number")
		assert.Equal(t, int64(648), bucketNo, "correct bucket number")
	})
}

func TestGetBucket(t *testing.T) {
	t.Run("gets a bucket from file", func(t *testing.T) {
		// Prepare
		fhm, _, err := NewFileHashMap(testHashMap, 10, 16, 10, nil)
		assert.NoError(t, err, "create new file hash map struct")

		err = fhm.CreateNewFiles()
		assert.NoError(t, err, "create new files")

		buf1 := []byte{40, 207, 246, 26, 160, 210, 125, 145, 180, 100, 230, 237, 179, 24, 48, 155, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
		buf2 := []byte{139, 165, 195, 217, 169, 202, 224, 215, 169, 0, 225, 188, 167, 128, 40, 233, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0}

		err = fhm.Set(buf1[:16], buf1[16:])
		assert.NoError(t, err, "set a new record in file")
		err = fhm.Set(buf2[:16], buf2[16:])
		assert.NoError(t, err, "set a second new record in file")

		bucketNo, err := fhm.GetBucketNo(buf1[:16])
		assert.NoError(t, err, "get bucket number")

		// Execute
		bucket, _, err := fhm.getBucket(bucketNo)

		// Check
		assert.NoError(t, err, "get bucket from file")

		trueRecordLength := fhm.keyLength + fhm.valueLength + conf.InUseFlagBytes
		bucketAddress := conf.MapFileHeaderLength + 7*(conf.BucketHeaderLength+fhm.recordsPerBucket*trueRecordLength)
		assert.False(t, bucket.HasOverflow, "has no overflow")
		assert.Zero(t, bucket.OverflowAddress, "no overflow address")
		assert.Equal(t, bucketAddress, bucket.BucketAddress, "correct bucket address")
		assert.Equal(t, 2, len(bucket.Records), "correct number of records")
		assert.True(t, utils.IsEqual(buf1[:16], bucket.Records[0].Key), "key is correct")
		assert.True(t, utils.IsEqual(buf1[16:], bucket.Records[0].Value), "value is correct")
		assert.True(t, utils.IsEqual(buf2[:16], bucket.Records[1].Key), "key is correct")
		assert.True(t, utils.IsEqual(buf2[16:], bucket.Records[1].Value), "value is correct")

		// Clean up
		err = fhm.RemoveFiles()
		assert.NoError(t, err, "files can be removed after close")
		_, err = os.Stat(fhm.ovflFileName)
		assert.Error(t, err, "overflow file is removed")
		_, err = os.Stat(fhm.mapFileName)
		assert.Error(t, err, "map file is removed")

	})
}

func TestOverflowIterator(t *testing.T) {
	t.Run("iterates over overflow records", func(t *testing.T) {
		// Prepare
		fhm, _, err := NewFileHashMap(testHashMap, 10, 16, 10, nil)
		assert.NoError(t, err, "create new file hash map struct")

		err = fhm.CreateNewFiles()
		assert.NoError(t, err, "create new files")

		buf0 := []byte{107, 68, 126, 11, 54, 204, 206, 242, 54, 35, 190, 163, 22, 221, 129, 95, 14, 31, 161, 110, 20, 250, 111, 111, 75, 195}
		buf1 := []byte{97, 130, 71, 240, 237, 168, 227, 220, 123, 204, 120, 55, 166, 202, 244, 133, 75, 246, 148, 41, 29, 111, 226, 56, 159, 48}
		buf2 := []byte{135, 25, 58, 216, 126, 252, 30, 22, 119, 3, 70, 135, 75, 111, 50, 236, 249, 241, 208, 233, 107, 199, 142, 168, 77, 12}
		buf3 := []byte{88, 153, 223, 103, 228, 229, 153, 24, 227, 106, 50, 192, 116, 195, 253, 157, 14, 123, 150, 14, 62, 159, 4, 145, 78, 68}
		buf4 := []byte{172, 117, 190, 175, 40, 161, 42, 110, 8, 91, 95, 184, 30, 18, 157, 240, 38, 158, 200, 22, 146, 167, 179, 94, 244, 178}
		buf := [5][]byte{buf0, buf1, buf2, buf3, buf4}

		for i, b := range buf {
			err = fhm.Set(b[:16], b[16:])
			assert.NoError(t, err, fmt.Sprintf("set record %d in file", i))
		}

		bucketNo, err := fhm.GetBucketNo(buf0[:16])
		assert.NoError(t, err, "get bucket number")

		// Execute
		_, iter, err := fhm.getBucket(bucketNo)

		// Check
		assert.NoError(t, err, "get iterator")

		var record model.Record
		for i := 2; i < 5; i++ {
			assert.True(t, iter.hasNext(), fmt.Sprintf("iteration %d has overflow record", i-2))

			record, err = iter.next()
			assert.NoError(t, err, fmt.Sprintf("get record %d in overflow file", i-2))

			assert.True(t, record.InUse, fmt.Sprintf("record %d in overflow file in use", i-2))
			assert.True(t, record.IsOverflow, fmt.Sprintf("record %d is overflow", i-2))
			assert.NotEqual(t, int64(0), record.RecordAddress, fmt.Sprintf("record %d in overflow file has address", i-2))
			if i < 4 {
				assert.NotEqual(t, int64(0), record.NextOverflow, fmt.Sprintf("record %d has overflow address", i-2))
			} else {
				assert.Equal(t, int64(0), record.NextOverflow, fmt.Sprintf("record %d has no overflow address", i-2))
			}
			assert.Truef(t, utils.IsEqual(buf[i][:16], record.Key), "key is correct in record %d", i-2)
			assert.Truef(t, utils.IsEqual(buf[i][16:], record.Value), "value is correct in record %d", i-2)
		}
		assert.False(t, iter.hasNext(), "iteration has no overflow record")

		// Clean up
		err = fhm.RemoveFiles()
		assert.NoError(t, err, "files can be removed after close")
		_, err = os.Stat(fhm.ovflFileName)
		assert.Error(t, err, "overflow file is removed")
		_, err = os.Stat(fhm.mapFileName)
		assert.Error(t, err, "map file is removed")
	})
}

func TestPop(t *testing.T) {
	t.Run("pops records", func(t *testing.T) {
		// Prepare
		fhm, _, err := NewFileHashMap(testHashMap, 10, 16, 10, nil)
		assert.NoError(t, err, "create new file hash map struct")

		err = fhm.CreateNewFiles()
		assert.NoError(t, err, "create new files")

		buf0 := []byte{107, 68, 126, 11, 54, 204, 206, 242, 54, 35, 190, 163, 22, 221, 129, 95, 14, 31, 161, 110, 20, 250, 111, 111, 75, 195}
		buf1 := []byte{97, 130, 71, 240, 237, 168, 227, 220, 123, 204, 120, 55, 166, 202, 244, 133, 75, 246, 148, 41, 29, 111, 226, 56, 159, 48}
		buf2 := []byte{135, 25, 58, 216, 126, 252, 30, 22, 119, 3, 70, 135, 75, 111, 50, 236, 249, 241, 208, 233, 107, 199, 142, 168, 77, 12}
		buf3 := []byte{88, 153, 223, 103, 228, 229, 153, 24, 227, 106, 50, 192, 116, 195, 253, 157, 14, 123, 150, 14, 62, 159, 4, 145, 78, 68}
		buf4 := []byte{172, 117, 190, 175, 40, 161, 42, 110, 8, 91, 95, 184, 30, 18, 157, 240, 38, 158, 200, 22, 146, 167, 179, 94, 244, 178}
		buf := [5][]byte{buf0, buf1, buf2, buf3, buf4}

		for i, b := range buf {
			err = fhm.Set(b[:16], b[16:])
			assert.NoError(t, err, fmt.Sprintf("set record %d in file", i))
		}

		// Execute
		value1, err1 := fhm.Pop(buf1[:16])
		value3, err3 := fhm.Pop(buf3[:16])

		// Check
		assert.NoError(t, err1, "pop record 1 (from bucket)")
		assert.NoError(t, err3, "pop record 3 (from overflow)")

		assert.True(t, utils.IsEqual(buf1[16:], value1), "value1 is correct in record 1")
		assert.True(t, utils.IsEqual(buf3[16:], value3), "value1 is correct in record 3")

		_, err = fhm.Get(buf1[:16])
		assert.ErrorIs(t, err, NoRecordFound{}, "key 1 gives correct error type")
		_, err = fhm.Get(buf3[:16])
		assert.ErrorIs(t, err, NoRecordFound{}, "key 2 gives correct error type")

		bucketNo, err := fhm.GetBucketNo(buf1[:16])
		assert.NoError(t, err, "get bucket number")

		bucket, iter, err := fhm.getBucket(bucketNo)
		assert.NoError(t, err, "get bucket and iterator")

		assert.True(t, bucket.Records[0].InUse, "record 0 in use")
		assert.False(t, bucket.Records[1].InUse, "record 1 not in use")
		record, err := iter.next()
		assert.NoError(t, err, "get first overflow record")
		assert.True(t, record.InUse, "record 2 in use")
		record, err = iter.next()
		assert.NoError(t, err, "get second overflow record")
		assert.False(t, record.InUse, "record 3 not in use")
		record, err = iter.next()
		assert.NoError(t, err, "get third overflow record")
		assert.True(t, record.InUse, "record 4 in use")

		// Clean up
		err = fhm.RemoveFiles()
		assert.NoError(t, err, "files can be removed after close")
		_, err = os.Stat(fhm.ovflFileName)
		assert.Error(t, err, "overflow file is removed")
		_, err = os.Stat(fhm.mapFileName)
		assert.Error(t, err, "map file is removed")
	})
}

func TestStat(t *testing.T) {
	t.Run("produces statistics from files", func(t *testing.T) {
		// Prepare
		rand.Seed(123)
		buf := make([]byte, 26)

		fhm, _, err := NewFileHashMap(testHashMap, 100, 16, 10, nil)
		assert.NoError(t, err, "create new file hash map struct")

		err = fhm.CreateNewFiles()
		assert.NoError(t, err, "create new files")

		for i := 0; i < 100; i++ {
			rand.Read(buf)
			err = fhm.Set(buf[:16], buf[16:])
			assert.NoError(t, err, "set record in file")
		}

		// Execute
		stat, err := fhm.Stat(true)

		// Check
		assert.NoError(t, err, "get statistics")
		assert.Equal(t, int64(100), stat.Records)
		assert.Equal(t, int64(86), stat.MapFileRecords)
		assert.Equal(t, int64(14), stat.OverflowRecords)

		var sum int64
		for _, v := range stat.BucketDistribution {
			sum += v
		}
		assert.Equal(t, int64(100), sum)

		// Clean up
		err = fhm.RemoveFiles()
		assert.NoError(t, err, "files can be removed after close")
		_, err = os.Stat(fhm.ovflFileName)
		assert.Error(t, err, "overflow file is removed")
		_, err = os.Stat(fhm.mapFileName)
		assert.Error(t, err, "map file is removed")
	})
}
