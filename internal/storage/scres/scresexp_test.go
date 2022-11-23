//go:build unit

package scres

import (
	"github.com/gostonefire/filehashmap/internal/model"
	"github.com/gostonefire/filehashmap/internal/utils"
	"github.com/gostonefire/filehashmap/storage"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"os"
	"testing"
)

func TestNewSCFiles(t *testing.T) {
	t.Run("creates a new SCFiles instance", func(t *testing.T) {
		// Prepare
		scConf := SCFilesConf{
			Name:              "test",
			InitialUniqueKeys: 10,
			KeyLength:         16,
			ValueLength:       10,
			HashAlgorithm:     nil,
		}

		// Execute
		scFiles, err := NewSCFiles(scConf)

		// Check
		assert.NoError(t, err, "create new SCFiles instance")
		assert.Equal(t, "test-map.bin", scFiles.mapFileName, "map filename correct")
		assert.Equal(t, "test-ovfl.bin", scFiles.ovflFileName, "overflow filename correct")
		assert.NotNil(t, scFiles.mapFile, "has map file")
		assert.NotNil(t, scFiles.ovflFile, "has overflow file")
		assert.Equal(t, scConf.InitialUniqueKeys, scFiles.initialUniqueKeys, "initial unique keys preserved")
		assert.Equal(t, scConf.KeyLength, scFiles.keyLength, "key length preserved")
		assert.Equal(t, scConf.ValueLength, scFiles.valueLength, "value length preserved")
		assert.NotZero(t, scFiles.recordsPerBucket, "records per bucket non zero")
		assert.NotZero(t, scFiles.fillFactor, "fill factor non zero")
		assert.Zero(t, scFiles.minBucketNo, "min bucket number is zero")
		assert.NotZero(t, scFiles.maxBucketNo, "max bucket number is not zero")
		assert.Greater(t, scFiles.mapFileSize, int64(1024), "map file size greater than header length")
		assert.NotNil(t, scFiles.hashAlgorithm, "hash algorithm is assigned")

		stat, err := os.Stat(scFiles.mapFileName)
		assert.NoError(t, err, "map file exists")
		assert.Equal(t, scFiles.mapFileSize, stat.Size(), "map file in correct size")
		_, err = os.Stat(scFiles.ovflFileName)
		assert.NoError(t, err, "overflow file exists")

		scFiles.CloseFiles()
		err = scFiles.RemoveFiles()
		assert.NoError(t, err, "removes files")

		_, err = os.Stat(scFiles.mapFileName)
		assert.True(t, os.IsNotExist(err), "map file removed")
		_, err = os.Stat(scFiles.ovflFileName)
		assert.True(t, os.IsNotExist(err), "overflow file removed")

		// Clean up
	})
}

func TestNewSCFilesFromExistingFiles(t *testing.T) {
	t.Run("opens SCFiles on existing files", func(t *testing.T) {
		// Prepare
		scConf := SCFilesConf{
			Name:              "test",
			InitialUniqueKeys: 10,
			KeyLength:         16,
			ValueLength:       10,
			HashAlgorithm:     nil,
		}

		scFilesInit, err := NewSCFiles(scConf)
		assert.NoError(t, err, "create new SCFiles instance")
		scFilesInit.CloseFiles()

		// Execute
		scFiles, err := NewSCFilesFromExistingFiles("test", nil)

		// Check
		assert.NoError(t, err, "opens existing files")
		assert.Equal(t, "test-map.bin", scFiles.mapFileName, "map filename correct")
		assert.Equal(t, "test-ovfl.bin", scFiles.ovflFileName, "overflow filename correct")
		assert.NotNil(t, scFiles.mapFile, "has map file")
		assert.NotNil(t, scFiles.ovflFile, "has overflow file")
		assert.Equal(t, scConf.InitialUniqueKeys, scFiles.initialUniqueKeys, "initial unique keys preserved")
		assert.Equal(t, scConf.KeyLength, scFiles.keyLength, "key length preserved")
		assert.Equal(t, scConf.ValueLength, scFiles.valueLength, "value length preserved")
		assert.NotZero(t, scFiles.recordsPerBucket, "records per bucket non zero")
		assert.NotZero(t, scFiles.fillFactor, "fill factor non zero")
		assert.Zero(t, scFiles.minBucketNo, "min bucket number is zero")
		assert.NotZero(t, scFiles.maxBucketNo, "max bucket number is not zero")
		assert.Greater(t, scFiles.mapFileSize, int64(1024), "map file size greater than header length")
		assert.NotNil(t, scFiles.hashAlgorithm, "hash algorithm is assigned")

		// Clean up
		scFiles.CloseFiles()
		err = scFiles.RemoveFiles()
		assert.NoError(t, err, "removes files")

		_, err = os.Stat(scFiles.mapFileName)
		assert.True(t, os.IsNotExist(err), "map file removed")
		_, err = os.Stat(scFiles.ovflFileName)
		assert.True(t, os.IsNotExist(err), "overflow file removed")
	})
}

func TestSCFiles_GetStorageParameters(t *testing.T) {
	t.Run("gets storage parameters", func(t *testing.T) {
		// Prepare
		scConf := SCFilesConf{
			Name:              "test",
			InitialUniqueKeys: 10,
			KeyLength:         16,
			ValueLength:       10,
			HashAlgorithm:     nil,
		}

		scFiles, err := NewSCFiles(scConf)
		assert.NoError(t, err, "create new SCFiles instance")

		// Execute
		sp := scFiles.GetStorageParameters()

		// Check
		assert.Equal(t, scConf.InitialUniqueKeys, sp.InitialUniqueKeys, "initial unique keys preserved")
		assert.Equal(t, scConf.KeyLength, sp.KeyLength, "key length preserved")
		assert.Equal(t, scConf.ValueLength, sp.ValueLength, "value length preserved")
		assert.Equal(t, scFiles.recordsPerBucket, sp.RecordsPerBucket, "records per bucket preserved")
		assert.Equal(t, scFiles.numberOfBuckets, sp.NumberOfBuckets, "number of buckets preserved")
		assert.Equal(t, scFiles.fillFactor, sp.FillFactor, "fill factor preserved")
		assert.Equal(t, scFiles.mapFileSize, sp.MapFileSize, "map file size preserved")
		assert.True(t, sp.InternalAlgorithm, "indicates using internal hash algorithm")

		// Clean up
		scFiles.CloseFiles()
		err = scFiles.RemoveFiles()
		assert.NoError(t, err, "removes files")

		_, err = os.Stat(scFiles.mapFileName)
		assert.True(t, os.IsNotExist(err), "map file removed")
		_, err = os.Stat(scFiles.ovflFileName)
		assert.True(t, os.IsNotExist(err), "overflow file removed")
	})
}

func TestSCFiles_Set(t *testing.T) {
	t.Run("sets a record in file", func(t *testing.T) {
		// Prepare
		scConf := SCFilesConf{
			Name:              "test",
			InitialUniqueKeys: 10,
			KeyLength:         16,
			ValueLength:       10,
			HashAlgorithm:     nil,
		}

		scFiles, err := NewSCFiles(scConf)
		assert.NoError(t, err, "create new SCFiles instance")

		record := model.Record{
			Key:   []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
			Value: []byte{16, 17, 18, 19, 20, 21, 22, 23, 24, 25},
		}

		// Execute
		err = scFiles.Set(record)

		// Check
		assert.NoError(t, err, "sets record to file")

		// Clean up
		scFiles.CloseFiles()
		err = scFiles.RemoveFiles()
		assert.NoError(t, err, "removes files")

		_, err = os.Stat(scFiles.mapFileName)
		assert.True(t, os.IsNotExist(err), "map file removed")
		_, err = os.Stat(scFiles.ovflFileName)
		assert.True(t, os.IsNotExist(err), "overflow file removed")
	})
}

func TestSCFiles_Get(t *testing.T) {
	t.Run("gets a record from file", func(t *testing.T) {
		// Prepare
		scConf := SCFilesConf{
			Name:              "test",
			InitialUniqueKeys: 10,
			KeyLength:         16,
			ValueLength:       10,
			HashAlgorithm:     nil,
		}

		scFiles, err := NewSCFiles(scConf)
		assert.NoError(t, err, "create new SCFiles instance")

		recordInit := model.Record{
			Key:   []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
			Value: []byte{16, 17, 18, 19, 20, 21, 22, 23, 24, 25},
		}

		err = scFiles.Set(recordInit)
		assert.NoError(t, err, "sets record to file")

		// Execute
		record, err := scFiles.Get(model.Record{Key: recordInit.Key})

		// Check
		assert.NoError(t, err, "gets a record from file")
		assert.True(t, record.InUse, "record marked in use")
		assert.NotZero(t, record.RecordAddress, "has valid record address")
		assert.False(t, record.IsOverflow, "record not marked as overflow")
		assert.Zero(t, record.NextOverflow, "has no valid overflow address")
		assert.True(t, utils.IsEqual(recordInit.Key, record.Key), "key is preserved")
		assert.True(t, utils.IsEqual(recordInit.Value, record.Value), "value is preserved")

		// Clean up
		scFiles.CloseFiles()
		err = scFiles.RemoveFiles()
		assert.NoError(t, err, "removes files")

		_, err = os.Stat(scFiles.mapFileName)
		assert.True(t, os.IsNotExist(err), "map file removed")
		_, err = os.Stat(scFiles.ovflFileName)
		assert.True(t, os.IsNotExist(err), "overflow file removed")
	})
}

func TestSCFiles_Delete(t *testing.T) {
	t.Run("deletes a bucket record from file", func(t *testing.T) {
		// Prepare
		scConf := SCFilesConf{
			Name:              "test",
			InitialUniqueKeys: 10,
			KeyLength:         16,
			ValueLength:       10,
			HashAlgorithm:     nil,
		}

		scFiles, err := NewSCFiles(scConf)
		assert.NoError(t, err, "create new SCFiles instance")

		recordInit := model.Record{
			Key:   []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
			Value: []byte{16, 17, 18, 19, 20, 21, 22, 23, 24, 25},
		}

		err = scFiles.Set(recordInit)
		assert.NoError(t, err, "sets record to file")

		record, err := scFiles.Get(model.Record{Key: recordInit.Key})
		assert.NoError(t, err, "gets a record from file")

		// Execute
		err = scFiles.Delete(record)

		// Check
		assert.NoError(t, err, "deletes a record from file")

		record, err = scFiles.Get(model.Record{Key: recordInit.Key})
		assert.ErrorIs(t, err, storage.NoRecordFound{}, "returns correct error")

		emptyRecord := model.Record{}
		assert.Equal(t, emptyRecord.InUse, record.InUse, "in use is according empty record")
		assert.Equal(t, emptyRecord.IsOverflow, record.IsOverflow, "is overflow is according empty record")
		assert.Equal(t, emptyRecord.RecordAddress, record.RecordAddress, "record address is according empty record")
		assert.Equal(t, emptyRecord.NextOverflow, record.NextOverflow, "next overflow is according empty record")
		assert.True(t, utils.IsEqual(emptyRecord.Key, record.Key), "key is according empty record")
		assert.True(t, utils.IsEqual(emptyRecord.Value, record.Value), "value is according empty record")

		// Clean up
		scFiles.CloseFiles()
		err = scFiles.RemoveFiles()
		assert.NoError(t, err, "removes files")

		_, err = os.Stat(scFiles.mapFileName)
		assert.True(t, os.IsNotExist(err), "map file removed")
		_, err = os.Stat(scFiles.ovflFileName)
		assert.True(t, os.IsNotExist(err), "overflow file removed")
	})
}

func TestSCFiles_Overflow(t *testing.T) {
	t.Run("uses overflow", func(t *testing.T) {
		// Prepare
		scConf := SCFilesConf{
			Name:              "test",
			InitialUniqueKeys: 10,
			KeyLength:         16,
			ValueLength:       10,
			HashAlgorithm:     nil,
		}

		scFiles, err := NewSCFiles(scConf)
		assert.NoError(t, err, "create new SCFiles instance")

		records := make([]model.Record, 1000)
		for i := 0; i < 1000; i++ {
			records[i].Key = make([]byte, 16)
			rand.Read(records[i].Key)
			records[i].Value = make([]byte, 10)
			rand.Read(records[i].Value)

			err = scFiles.Set(records[i])
			assert.NoErrorf(t, err, "sets record #%d to file", i)
		}

		// Check
		var record model.Record
		var hadOverflow bool
		for i := 0; i < 1000; i++ {
			record, err = scFiles.Get(model.Record{Key: records[i].Key})
			assert.NoErrorf(t, err, "gets record #%d from file", i)
			assert.Truef(t, utils.IsEqual(records[i].Value, record.Value), "value of record #%d is correct", i)
			if record.IsOverflow {
				hadOverflow = true
			}
		}
		assert.True(t, hadOverflow, "some record(s) is in overflow")

		// Clean up
		scFiles.CloseFiles()
		err = scFiles.RemoveFiles()
		assert.NoError(t, err, "removes files")

		_, err = os.Stat(scFiles.mapFileName)
		assert.True(t, os.IsNotExist(err), "map file removed")
		_, err = os.Stat(scFiles.ovflFileName)
		assert.True(t, os.IsNotExist(err), "overflow file removed")
	})
}

func TestSCFiles_GetBucket(t *testing.T) {
	t.Run("returns a bucket", func(t *testing.T) {
		// Prepare
		scConf := SCFilesConf{
			Name:              "test",
			InitialUniqueKeys: 10,
			KeyLength:         16,
			ValueLength:       10,
			HashAlgorithm:     nil,
		}

		scFiles, err := NewSCFiles(scConf)
		assert.NoError(t, err, "create new SCFiles instance")

		records := make([]model.Record, 1000)
		for i := 0; i < 1000; i++ {
			records[i].Key = make([]byte, 16)
			rand.Read(records[i].Key)
			records[i].Value = make([]byte, 10)
			rand.Read(records[i].Value)

			err = scFiles.Set(records[i])
			assert.NoErrorf(t, err, "sets record #%d to file", i)
		}

		// Execute
		bucket, iterator, err := scFiles.GetBucket(2)

		// Check
		assert.NoError(t, err, "gets a bucket")
		assert.Equal(t, int(scFiles.recordsPerBucket), len(bucket.Records), "correct number of records in bucket")
		assert.True(t, bucket.HasOverflow, "bucket has overflow")
		assert.NotZero(t, bucket.OverflowAddress, "bucket has overflow address")
		for i := int64(0); i < scFiles.recordsPerBucket; i++ {
			assert.Truef(t, bucket.Records[i].InUse, "record #%d in bucket is in use", i)
		}

		var hadOverflowRecord bool
		var ovflRecord model.Record
		for iterator.HasNext() {
			ovflRecord, err = iterator.Next()
			assert.NoError(t, err, "next returns record")
			assert.True(t, ovflRecord.InUse, "record marked in use")
			assert.NotZero(t, ovflRecord.RecordAddress, "has valid record address")
			assert.True(t, ovflRecord.IsOverflow, "record marked as overflow")
			if iterator.HasNext() {
				assert.NotZero(t, ovflRecord.NextOverflow, "has valid overflow address")
			} else {
				assert.Zero(t, ovflRecord.NextOverflow, "last record has no overflow address")
			}
			hadOverflowRecord = true
		}
		assert.True(t, hadOverflowRecord, "some record(s) is in overflow")

		// Clean up
		scFiles.CloseFiles()
		err = scFiles.RemoveFiles()
		assert.NoError(t, err, "removes files")

		_, err = os.Stat(scFiles.mapFileName)
		assert.True(t, os.IsNotExist(err), "map file removed")
		_, err = os.Stat(scFiles.ovflFileName)
		assert.True(t, os.IsNotExist(err), "overflow file removed")
	})
}
