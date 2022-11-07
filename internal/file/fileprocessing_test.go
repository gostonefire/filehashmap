//go:build unit

package file

import (
	"encoding/binary"
	"github.com/gostonefire/filehashmap/internal/conf"
	"github.com/gostonefire/filehashmap/internal/utils"
	"github.com/stretchr/testify/assert"
	"io"
	"os"
	"testing"
)

const testFile1 string = "unittest1.bin"
const testFile2 string = "unittest2.bin"

func TestSetHeader(t *testing.T) {
	t.Run("sets a header in file", func(t *testing.T) {
		// Prepare
		f, err := os.OpenFile(testFile1, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0644)
		assert.NoError(t, err, "open/create file")

		err = f.Truncate(1000)
		assert.NoError(t, err, "truncate file to size")

		header := Header{
			InternalAlg:       true,
			InitialUniqueKeys: 1000,
			KeyLength:         16,
			ValueLength:       10,
			RecordsPerBucket:  2,
			NumberOfBuckets:   500,
			MinBucketNo:       0,
			MaxBucketNo:       499,
			FileSize:          1000,
		}

		// Execute
		err = SetHeader(f, header)

		// check
		assert.NoError(t, err, "set header to file")

		// Clean up
		_ = f.Close()
		err = os.Remove(testFile1)
		assert.NoError(t, err, "remove file")
	})
}

func TestGetHeader(t *testing.T) {
	t.Run("gets a header from file", func(t *testing.T) {
		// Prepare
		f, err := os.OpenFile(testFile1, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0644)
		assert.NoError(t, err, "open/create file")

		err = f.Truncate(1000)
		assert.NoError(t, err, "truncate file to size")

		header := Header{
			InternalAlg:       true,
			InitialUniqueKeys: 1000,
			KeyLength:         16,
			ValueLength:       10,
			RecordsPerBucket:  2,
			NumberOfBuckets:   500,
			MinBucketNo:       0,
			MaxBucketNo:       499,
			FileSize:          1000,
		}
		err = SetHeader(f, header)
		assert.NoError(t, err, "set header to file")

		// Execute
		header2, err := GetHeader(f)
		assert.NoError(t, err, "gets header from file")

		// Check
		assert.Equal(t, header.InternalAlg, header2.InternalAlg)
		assert.Equal(t, header.InitialUniqueKeys, header2.InitialUniqueKeys)
		assert.Equal(t, header.KeyLength, header2.KeyLength)
		assert.Equal(t, header.ValueLength, header2.ValueLength)
		assert.Equal(t, header.RecordsPerBucket, header2.RecordsPerBucket)
		assert.Equal(t, header.NumberOfBuckets, header2.NumberOfBuckets)
		assert.Equal(t, header.MinBucketNo, header2.MinBucketNo)
		assert.Equal(t, header.MaxBucketNo, header2.MaxBucketNo)
		assert.Equal(t, header.FileSize, header2.FileSize)

		// Clean up
		_ = f.Close()
		err = os.Remove(testFile1)
		assert.NoError(t, err, "remove file")
	})
}

func TestSetBucketRecord(t *testing.T) {
	t.Run("sets a bucket record in file", func(t *testing.T) {
		// Prepare
		f, err := os.OpenFile(testFile1, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0644)
		assert.NoError(t, err, "open/create file")

		err = f.Truncate(1000)
		assert.NoError(t, err, "truncate file to size")

		record := Record{
			InUse:         true,
			RecordAddress: 500,
			Key:           []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
			Value:         []byte{16, 17, 18, 19, 20, 21, 22, 23, 24, 25},
		}

		// Execute
		err = SetBucketRecord(f, record, 16, 10)

		// Check
		assert.NoError(t, err, "set record to file")

		// Clean up
		_ = f.Close()
		err = os.Remove(testFile1)
		assert.NoError(t, err, "remove file")
	})
}

func TestGetBucketRecord(t *testing.T) {
	t.Run("gets a bucket record from file", func(t *testing.T) {
		// Prepare
		f, err := os.OpenFile(testFile1, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0644)
		assert.NoError(t, err, "open/create file")

		err = f.Truncate(2000)
		assert.NoError(t, err, "truncate file to size")

		bucketAddress := conf.MapFileHeaderLength + 1*(27*1+conf.BucketHeaderLength)

		record := Record{
			InUse:         true,
			RecordAddress: bucketAddress + conf.BucketHeaderLength,
			Key:           []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
			Value:         []byte{16, 17, 18, 19, 20, 21, 22, 23, 24, 25},
		}

		err = SetBucketRecord(f, record, 16, 10)
		assert.NoError(t, err, "set record to file")

		// Execute
		record2, err := GetBucketRecords(f, 1, 16, 10, 1)

		// Check
		assert.NoError(t, err, "get bucket records")
		assert.Equal(t, record.InUse, record2.Records[0].InUse)
		assert.Equal(t, record.RecordAddress, record2.Records[0].RecordAddress)
		assert.True(t, utils.IsEqual(record.Key, record2.Records[0].Key), "key is correct in record")
		assert.True(t, utils.IsEqual(record.Value, record2.Records[0].Value), "value is correct in record")

		// Clean up
		_ = f.Close()
		err = os.Remove(testFile1)
		assert.NoError(t, err, "remove file")
	})
}

func TestCreateNewHashMapFile(t *testing.T) {
	t.Run("creates a new hash map file", func(t *testing.T) {
		// Execute
		f, err := CreateNewHashMapFile(testFile1, 1024)
		_ = f.Close()

		// Check
		assert.NoError(t, err, "create new file")

		stat, err := os.Stat(testFile1)
		assert.NoError(t, err, "get stat info")
		assert.Equal(t, int64(1024), stat.Size(), "has right size")

		// Clean up
		err = os.Remove(testFile1)
		assert.NoError(t, err, "remove file")
	})
}

func TestCreateNewOverflowFile(t *testing.T) {
	t.Run("creates a new overflow file", func(t *testing.T) {
		// Execute
		f, err := CreateNewOverflowFile(testFile1)
		_ = f.Close()

		// Check
		assert.NoError(t, err, "create new file")

		stat, err := os.Stat(testFile1)
		assert.NoError(t, err, "get stat info")
		assert.Equal(t, conf.OvflFileHeaderLength, stat.Size(), "has right size")

		// Clean up
		err = os.Remove(testFile1)
		assert.NoError(t, err, "remove file")
	})
}

func TestRemoveFiles(t *testing.T) {
	t.Run("removes files", func(t *testing.T) {
		// Prepare
		mf, err := CreateNewHashMapFile(testFile1, 1024)
		assert.NoError(t, err, "create new hash map file")
		of, err := CreateNewOverflowFile(testFile2)
		assert.NoError(t, err, "create new overflow file")

		CloseFiles(mf, of)

		// Execute
		err = RemoveFiles(testFile1, testFile2)

		// Check
		assert.NoError(t, err, "remove files")

		_, err = os.Stat(testFile1)
		assert.Errorf(t, err, "hash map file removed")
		_, err = os.Stat(testFile2)
		assert.Errorf(t, err, "overflow file removed")
	})
}

func TestOpenHashMapFile(t *testing.T) {
	t.Run("opens an existing hash map file", func(t *testing.T) {
		// Prepare
		f, err := CreateNewHashMapFile(testFile1, 1024)
		assert.NoError(t, err, "create new file")

		header := Header{
			InternalAlg:       true,
			InitialUniqueKeys: 1000,
			KeyLength:         16,
			ValueLength:       10,
			RecordsPerBucket:  2,
			NumberOfBuckets:   500,
			MinBucketNo:       0,
			MaxBucketNo:       499,
			FileSize:          1024,
		}
		err = SetHeader(f, header)
		assert.NoError(t, err, "set header to file")

		_ = f.Close()

		// Execute
		f, header2, err := OpenHashMapFile(testFile1, false)

		// Check
		assert.NoError(t, err, "open hash map file")
		assert.NotNil(t, f, "got file pointer")
		assert.True(t, header2.InternalAlg)
		assert.Equal(t, header.InitialUniqueKeys, header2.InitialUniqueKeys)
		assert.Equal(t, header.KeyLength, header2.KeyLength)
		assert.Equal(t, header.ValueLength, header2.ValueLength)
		assert.Equal(t, header.RecordsPerBucket, header2.RecordsPerBucket)
		assert.Equal(t, header.NumberOfBuckets, header2.NumberOfBuckets)
		assert.Equal(t, header.MinBucketNo, header2.MinBucketNo)
		assert.Equal(t, header.MaxBucketNo, header2.MaxBucketNo)
		assert.Equal(t, header.FileSize, header2.FileSize)

		// Clean up
		_ = f.Close()
		err = os.Remove(testFile1)
		assert.NoError(t, err, "remove file")
	})
}

func TestOpenOverflowFile(t *testing.T) {
	t.Run("opens an existing hash map file", func(t *testing.T) {
		// Prepare
		f, err := CreateNewOverflowFile(testFile1)
		assert.NoError(t, err, "create new file")

		_ = f.Close()

		// Execute
		f, err = OpenOverflowFile(testFile1)

		// Check
		assert.NoError(t, err, "open overflow file")
		assert.NotNil(t, f, "got file pointer")

		// Clean up
		_ = f.Close()
		err = os.Remove(testFile1)
		assert.NoError(t, err, "remove file")
	})
}

func TestNewBucketOverflow(t *testing.T) {
	t.Run("adds new overflow record to file", func(t *testing.T) {
		// Prepare
		f, err := CreateNewOverflowFile(testFile1)
		assert.NoError(t, err, "create new file")

		key := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
		value := []byte{16, 17, 18, 19, 20, 21, 22, 23, 24, 25}

		// Execute
		overflowAddress, err := NewBucketOverflow(f, key, value, 16, 10)

		// Check
		assert.NoError(t, err, "add record to overflow file")
		assert.Equal(t, conf.OvflFileHeaderLength, overflowAddress)

		// Clean up
		_ = f.Close()
		err = os.Remove(testFile1)
		assert.NoError(t, err, "remove file")
	})
}

func TestSetBucketOverflowAddress(t *testing.T) {
	t.Run("sets overflow address to file", func(t *testing.T) {
		// Prepare
		f, err := CreateNewHashMapFile(testFile1, 2048)
		assert.NoError(t, err, "create new file")

		// execute
		err = SetBucketOverflowAddress(f, 1024, 3000)

		// Check
		assert.NoError(t, err, "set overflow address to file")

		_, err = f.Seek(1024, io.SeekStart)
		assert.NoError(t, err, "seek to bucket address")

		buf := make([]byte, 8)
		_, err = f.Read(buf)
		assert.NoError(t, err, "read at bucket address")
		assert.Equal(t, uint64(3000), binary.LittleEndian.Uint64(buf), "correct overflow address")

		// Clean up
		_ = f.Close()
		err = os.Remove(testFile1)
		assert.NoError(t, err, "remove file")
	})
}

func TestGetOverflowRecord(t *testing.T) {
	t.Run("gets overflow record from file", func(t *testing.T) {
		// Prepare
		f, err := CreateNewOverflowFile(testFile1)
		assert.NoError(t, err, "create new file")

		key := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
		value := []byte{16, 17, 18, 19, 20, 21, 22, 23, 24, 25}

		_, err = NewBucketOverflow(f, key, value, 16, 10)
		assert.NoError(t, err, "add record to overflow file")

		// Execute
		record2, err := GetOverflowRecord(f, conf.OvflFileHeaderLength, 16, 10)

		// Check
		assert.NoError(t, err, "get record from overflow file")
		assert.True(t, record2.InUse)
		assert.True(t, record2.IsOverflow)
		assert.Equal(t, conf.OvflFileHeaderLength, record2.RecordAddress)
		assert.Equal(t, int64(0), record2.NextOverflow)
		assert.True(t, utils.IsEqual(key, record2.Key), "key is correct in record")
		assert.True(t, utils.IsEqual(value, record2.Value), "value is correct in record")

		// Clean up
		_ = f.Close()
		err = os.Remove(testFile1)
		assert.NoError(t, err, "remove file")
	})
}

func TestSetOverflowRecord(t *testing.T) {
	t.Run("sets overflow record in file", func(t *testing.T) {
		// Prepare
		f, err := CreateNewOverflowFile(testFile1)
		assert.NoError(t, err, "create new file")

		err = f.Truncate(2048)
		assert.NoError(t, err, "extend file")

		record := Record{
			InUse:         true,
			IsOverflow:    true,
			RecordAddress: conf.OvflFileHeaderLength,
			NextOverflow:  0,
			Key:           []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
			Value:         []byte{16, 17, 18, 19, 20, 21, 22, 23, 24, 25},
		}

		// Execute
		err = SetOverflowRecord(f, record, 16, 10)

		// Check
		assert.NoError(t, err, "set record in file")

		record2, err := GetOverflowRecord(f, conf.OvflFileHeaderLength, 16, 10)
		assert.NoError(t, err, "get record from overflow file")
		assert.True(t, record2.InUse)
		assert.True(t, record2.IsOverflow)
		assert.Equal(t, conf.OvflFileHeaderLength, record2.RecordAddress)
		assert.Equal(t, int64(0), record2.NextOverflow)
		assert.True(t, utils.IsEqual(record.Key, record2.Key), "key is correct in record")
		assert.True(t, utils.IsEqual(record.Value, record2.Value), "value is correct in record")

		// Clean up
		_ = f.Close()
		err = os.Remove(testFile1)
		assert.NoError(t, err, "remove file")
	})
}

func TestAppendOverflowRecord(t *testing.T) {
	t.Run("appends overflow record in file", func(t *testing.T) {
		// Prepare
		f, err := CreateNewOverflowFile(testFile1)
		assert.NoError(t, err, "create new file")

		err = f.Truncate(2048)
		assert.NoError(t, err, "extend file")

		record := Record{
			InUse:         true,
			IsOverflow:    true,
			RecordAddress: conf.OvflFileHeaderLength,
			NextOverflow:  0,
			Key:           []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
			Value:         []byte{16, 17, 18, 19, 20, 21, 22, 23, 24, 25},
		}

		err = SetOverflowRecord(f, record, 16, 10)
		assert.NoError(t, err, "set record in file")

		key2 := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
		value2 := []byte{16, 17, 18, 19, 20, 21, 22, 23, 24, 25}

		// Execute
		err = AppendOverflowRecord(f, record, key2, value2, 16, 10)

		// check
		assert.NoError(t, err, "append record to file")

		record3, err := GetOverflowRecord(f, conf.OvflFileHeaderLength, 16, 10)
		assert.NoError(t, err, "get linking record from file")
		assert.Equal(t, int64(2048), record3.NextOverflow, "correct next overflow address")

		record4, err := GetOverflowRecord(f, record3.NextOverflow, 16, 10)
		assert.NoError(t, err, "get appended record from file")
		assert.True(t, record4.InUse)
		assert.True(t, record4.IsOverflow)
		assert.Equal(t, int64(2048), record4.RecordAddress)
		assert.Equal(t, int64(0), record4.NextOverflow)
		assert.True(t, utils.IsEqual(record.Key, record4.Key), "key is correct in record")
		assert.True(t, utils.IsEqual(record.Value, record4.Value), "value is correct in record")

		// Clean up
		_ = f.Close()
		err = os.Remove(testFile1)
		assert.NoError(t, err, "remove file")
	})
}

func TestGetBucketOverflowAddress(t *testing.T) {
	t.Run("sets overflow address to file", func(t *testing.T) {
		// Prepare
		f, err := CreateNewHashMapFile(testFile1, 2048)
		assert.NoError(t, err, "create new file")

		err = SetBucketOverflowAddress(f, 1144, 3000)
		assert.NoError(t, err, "set overflow address to file")

		// execute
		overflowAddress, err := GetBucketOverflowAddress(f, 2, 26, 2)

		// Check
		assert.NoError(t, err, "get overflow address from file")
		assert.Equal(t, int64(3000), overflowAddress, "correct overflow address")

		// Clean up
		_ = f.Close()
		err = os.Remove(testFile1)
		assert.NoError(t, err, "remove file")
	})
}
