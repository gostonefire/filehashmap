//go:build unit

package lpres

import (
	"github.com/gostonefire/filehashmap/crt"
	"github.com/gostonefire/filehashmap/internal/model"
	"github.com/gostonefire/filehashmap/internal/storage"
	"github.com/gostonefire/filehashmap/internal/utils"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"os"
	"testing"
)

func TestNewLPFiles(t *testing.T) {
	t.Run("creates a new LPFiles instance", func(t *testing.T) {
		// Prepare
		crtConf := model.CRTConf{
			Name:                  "test",
			NumberOfBucketsNeeded: 10,
			KeyLength:             16,
			ValueLength:           10,
			HashAlgorithm:         nil,
		}

		// Execute
		lpFiles, err := NewLPFiles(crtConf)

		// Check
		mapFileSize := storage.MapFileHeaderLength + lpFiles.numberOfBucketsAvailable*(crtConf.KeyLength+crtConf.ValueLength+stateBytes)
		assert.NoError(t, err, "create new LPFiles instance")
		assert.Equal(t, "test-map.bin", lpFiles.mapFileName, "map filename correct")
		assert.NotNil(t, lpFiles.mapFile, "has map file")
		assert.GreaterOrEqual(t, lpFiles.numberOfBucketsAvailable, crtConf.NumberOfBucketsNeeded, "needed buckets preserved in number of buckets")
		assert.Equal(t, crtConf.KeyLength, lpFiles.keyLength, "key length preserved")
		assert.Equal(t, crtConf.ValueLength, lpFiles.valueLength, "value length preserved")
		assert.Zero(t, lpFiles.minBucketNo, "min bucket number is zero")
		assert.NotZero(t, lpFiles.maxBucketNo, "max bucket number is not zero")
		assert.Equal(t, lpFiles.mapFileSize, mapFileSize, "map file size in correct length")
		assert.NotNil(t, lpFiles.hashAlgorithm, "hash algorithm is assigned")

		stat, err := os.Stat(lpFiles.mapFileName)
		assert.NoError(t, err, "map file exists")
		assert.Equal(t, lpFiles.mapFileSize, stat.Size(), "map file in correct size")

		lpFiles.CloseFiles()
		err = lpFiles.RemoveFiles()
		assert.NoError(t, err, "removes files")

		_, err = os.Stat(lpFiles.mapFileName)
		assert.True(t, os.IsNotExist(err), "map file removed")

		// Clean up
	})
}

func TestNewLPFilesFromExistingFiles(t *testing.T) {
	t.Run("opens LPFiles on existing files", func(t *testing.T) {
		// Prepare
		crtConf := model.CRTConf{
			Name:                  "test",
			NumberOfBucketsNeeded: 10,
			KeyLength:             16,
			ValueLength:           10,
			HashAlgorithm:         nil,
		}

		lpFilesInit, err := NewLPFiles(crtConf)
		assert.NoError(t, err, "create new LPFiles instance")
		lpFilesInit.CloseFiles()

		// Execute
		lpFiles, err := NewLPFilesFromExistingFiles("test", nil)

		// Check
		mapFileSize := storage.MapFileHeaderLength + lpFiles.numberOfBucketsAvailable*(crtConf.KeyLength+crtConf.ValueLength+stateBytes)
		assert.NoError(t, err, "opens existing files")
		assert.Equal(t, "test-map.bin", lpFiles.mapFileName, "map filename correct")
		assert.NotNil(t, lpFiles.mapFile, "has map file")
		assert.GreaterOrEqual(t, lpFiles.numberOfBucketsAvailable, crtConf.NumberOfBucketsNeeded, "needed buckets preserved in number of buckets")
		assert.Equal(t, crtConf.KeyLength, lpFiles.keyLength, "key length preserved")
		assert.Equal(t, crtConf.ValueLength, lpFiles.valueLength, "value length preserved")
		assert.Zero(t, lpFiles.minBucketNo, "min bucket number is zero")
		assert.NotZero(t, lpFiles.maxBucketNo, "max bucket number is not zero")
		assert.Equal(t, lpFiles.mapFileSize, mapFileSize, "map file size in correct length")
		assert.NotNil(t, lpFiles.hashAlgorithm, "hash algorithm is assigned")

		// Clean up
		lpFiles.CloseFiles()
		err = lpFiles.RemoveFiles()
		assert.NoError(t, err, "removes files")

		_, err = os.Stat(lpFiles.mapFileName)
		assert.True(t, os.IsNotExist(err), "map file removed")
	})
}

func TestLPFiles_GetStorageParameters(t *testing.T) {
	t.Run("gets storage parameters", func(t *testing.T) {
		// Prepare
		crtConf := model.CRTConf{
			Name:                  "test",
			NumberOfBucketsNeeded: 10,
			KeyLength:             16,
			ValueLength:           10,
			HashAlgorithm:         nil,
		}

		lpFiles, err := NewLPFiles(crtConf)
		assert.NoError(t, err, "create new LPFiles instance")

		// Execute
		sp := lpFiles.GetStorageParameters()

		// Check
		assert.Equal(t, crt.LinearProbing, sp.CollisionResolutionTechnique, "correct crt")
		assert.Equal(t, crtConf.NumberOfBucketsNeeded, sp.NumberOfBucketsNeeded, "buckets needed preserved")
		assert.Equal(t, crtConf.KeyLength, sp.KeyLength, "key length preserved")
		assert.Equal(t, crtConf.ValueLength, sp.ValueLength, "value length preserved")
		assert.Equal(t, lpFiles.numberOfBucketsAvailable, sp.NumberOfBucketsAvailable, "number of buckets preserved")
		assert.Equal(t, lpFiles.mapFileSize, sp.MapFileSize, "map file size preserved")
		assert.True(t, sp.InternalAlgorithm, "indicates using internal hash algorithm")

		// Clean up
		lpFiles.CloseFiles()
		err = lpFiles.RemoveFiles()
		assert.NoError(t, err, "removes files")

		_, err = os.Stat(lpFiles.mapFileName)
		assert.True(t, os.IsNotExist(err), "map file removed")
	})
}

func TestLPFiles_Set(t *testing.T) {
	t.Run("sets a record in file", func(t *testing.T) {
		// Prepare
		crtConf := model.CRTConf{
			Name:                  "test",
			NumberOfBucketsNeeded: 1000,
			KeyLength:             16,
			ValueLength:           10,
			HashAlgorithm:         nil,
		}

		lpFiles, err := NewLPFiles(crtConf)
		assert.NoError(t, err, "create new LPFiles instance")

		record := model.Record{
			Key:   []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
			Value: []byte{16, 17, 18, 19, 20, 21, 22, 23, 24, 25},
		}

		// Execute
		err = lpFiles.Set(record)

		// Check
		assert.NoError(t, err, "sets record to file")

		// Clean up
		lpFiles.CloseFiles()
		err = lpFiles.RemoveFiles()
		assert.NoError(t, err, "removes files")

		_, err = os.Stat(lpFiles.mapFileName)
		assert.True(t, os.IsNotExist(err), "map file removed")
	})

	t.Run("sets all available records in file", func(t *testing.T) {
		// Prepare
		crtConf := model.CRTConf{
			Name:                  "test",
			NumberOfBucketsNeeded: 1000,
			KeyLength:             16,
			ValueLength:           10,
			HashAlgorithm:         nil,
		}

		lpFiles, err := NewLPFiles(crtConf)
		assert.NoError(t, err, "create new LPFiles instance")

		records := make([]model.Record, lpFiles.numberOfBucketsAvailable)
		for i := int64(0); i < lpFiles.numberOfBucketsAvailable; i++ {
			records[i].Key = make([]byte, 16)
			rand.Read(records[i].Key)
			records[i].Value = make([]byte, 10)
			rand.Read(records[i].Value)
		}

		// Execute and Check
		for i := int64(0); i < lpFiles.numberOfBucketsAvailable; i++ {
			err = lpFiles.Set(records[i])
			assert.NoErrorf(t, err, "sets record #%d to file", i)
		}

		// Clean up
		lpFiles.CloseFiles()
		err = lpFiles.RemoveFiles()
		assert.NoError(t, err, "removes files")

		_, err = os.Stat(lpFiles.mapFileName)
		assert.True(t, os.IsNotExist(err), "map file removed")
	})

	t.Run("sets to many records in file", func(t *testing.T) {
		// Prepare
		crtConf := model.CRTConf{
			Name:                  "test",
			NumberOfBucketsNeeded: 1000,
			KeyLength:             16,
			ValueLength:           10,
			HashAlgorithm:         nil,
		}

		lpFiles, err := NewLPFiles(crtConf)
		assert.NoError(t, err, "create new LPFiles instance")

		records := make([]model.Record, lpFiles.numberOfBucketsAvailable+1)
		for i := int64(0); i < lpFiles.numberOfBucketsAvailable+1; i++ {
			records[i].Key = make([]byte, 16)
			rand.Read(records[i].Key)
			records[i].Value = make([]byte, 10)
			rand.Read(records[i].Value)
		}

		for i := int64(0); i < lpFiles.numberOfBucketsAvailable; i++ {
			err = lpFiles.Set(records[i])
			assert.NoErrorf(t, err, "sets record #%d to file", i)
		}

		// Execute
		err = lpFiles.Set(records[lpFiles.numberOfBucketsAvailable])

		// Check
		assert.ErrorIs(t, err, crt.MapFileFull{}, "correct error when map file is full")

		// Clean up
		lpFiles.CloseFiles()
		err = lpFiles.RemoveFiles()
		assert.NoError(t, err, "removes files")

		_, err = os.Stat(lpFiles.mapFileName)
		assert.True(t, os.IsNotExist(err), "map file removed")
	})
}

func TestLPFiles_Get(t *testing.T) {
	t.Run("gets a record from file", func(t *testing.T) {
		// Prepare
		crtConf := model.CRTConf{
			Name:                  "test",
			NumberOfBucketsNeeded: 10,
			KeyLength:             16,
			ValueLength:           10,
			HashAlgorithm:         nil,
		}

		lpFiles, err := NewLPFiles(crtConf)
		assert.NoError(t, err, "create new LPFiles instance")

		recordInit := model.Record{
			Key:   []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
			Value: []byte{16, 17, 18, 19, 20, 21, 22, 23, 24, 25},
		}

		err = lpFiles.Set(recordInit)
		assert.NoError(t, err, "sets record to file")

		// Execute
		record, err := lpFiles.Get(model.Record{Key: recordInit.Key})

		// Check
		assert.NoError(t, err, "gets a record from file")
		assert.Equal(t, model.RecordOccupied, record.State, "record marked in use")
		assert.NotZero(t, record.RecordAddress, "has valid record address")
		assert.False(t, record.IsOverflow, "record not marked as overflow")
		assert.Zero(t, record.NextOverflow, "has no valid overflow address")
		assert.True(t, utils.IsEqual(recordInit.Key, record.Key), "key is preserved")
		assert.True(t, utils.IsEqual(recordInit.Value, record.Value), "value is preserved")

		// Clean up
		lpFiles.CloseFiles()
		err = lpFiles.RemoveFiles()
		assert.NoError(t, err, "removes files")

		_, err = os.Stat(lpFiles.mapFileName)
		assert.True(t, os.IsNotExist(err), "map file removed")
	})
}

func TestLPFiles_Delete(t *testing.T) {
	t.Run("deletes a bucket record from file", func(t *testing.T) {
		// Prepare
		crtConf := model.CRTConf{
			Name:                  "test",
			NumberOfBucketsNeeded: 10,
			KeyLength:             16,
			ValueLength:           10,
			HashAlgorithm:         nil,
		}

		lpFiles, err := NewLPFiles(crtConf)
		assert.NoError(t, err, "create new LPFiles instance")

		recordInit := model.Record{
			Key:   []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
			Value: []byte{16, 17, 18, 19, 20, 21, 22, 23, 24, 25},
		}

		err = lpFiles.Set(recordInit)
		assert.NoError(t, err, "sets record to file")

		record, err := lpFiles.Get(model.Record{Key: recordInit.Key})
		assert.NoError(t, err, "gets a record from file")

		// Execute
		err = lpFiles.Delete(record)

		// Check
		assert.NoError(t, err, "deletes a record from file")

		record, err = lpFiles.Get(model.Record{Key: recordInit.Key})
		assert.ErrorIs(t, err, crt.NoRecordFound{}, "returns correct error")

		emptyRecord := model.Record{}
		assert.Equal(t, emptyRecord.State, record.State, "in use is according empty record")
		assert.Equal(t, emptyRecord.IsOverflow, record.IsOverflow, "is overflow is according empty record")
		assert.Equal(t, emptyRecord.RecordAddress, record.RecordAddress, "record address is according empty record")
		assert.Equal(t, emptyRecord.NextOverflow, record.NextOverflow, "next overflow is according empty record")
		assert.True(t, utils.IsEqual(emptyRecord.Key, record.Key), "key is according empty record")
		assert.True(t, utils.IsEqual(emptyRecord.Value, record.Value), "value is according empty record")

		// Clean up
		lpFiles.CloseFiles()
		err = lpFiles.RemoveFiles()
		assert.NoError(t, err, "removes files")

		_, err = os.Stat(lpFiles.mapFileName)
		assert.True(t, os.IsNotExist(err), "map file removed")
	})
}

func TestLPFiles_GetBucket(t *testing.T) {
	t.Run("returns a bucket", func(t *testing.T) {
		// Prepare
		crtConf := model.CRTConf{
			Name:                  "test",
			NumberOfBucketsNeeded: 10,
			KeyLength:             16,
			ValueLength:           10,
			HashAlgorithm:         nil,
		}

		lpFiles, err := NewLPFiles(crtConf)
		assert.NoError(t, err, "create new LPFiles instance")

		records := make([]model.Record, lpFiles.numberOfBucketsAvailable)
		for i := int64(0); i < lpFiles.numberOfBucketsAvailable; i++ {
			records[i].Key = make([]byte, 16)
			rand.Read(records[i].Key)
			records[i].Value = make([]byte, 10)
			rand.Read(records[i].Value)

			err = lpFiles.Set(records[i])
			assert.NoErrorf(t, err, "sets record #%d to file", i)
		}

		// Execute
		bucket, iterator, err := lpFiles.GetBucket(2)

		// Check
		assert.NoError(t, err, "gets a bucket")
		assert.False(t, bucket.HasOverflow, "bucket has no overflow")
		assert.Zero(t, bucket.OverflowAddress, "bucket has no overflow address")
		assert.Nil(t, iterator, "no overflow iterator")
		assert.Equal(t, model.RecordOccupied, bucket.Record.State, "record in bucket is in use")

		// Clean up
		lpFiles.CloseFiles()
		err = lpFiles.RemoveFiles()
		assert.NoError(t, err, "removes files")

		_, err = os.Stat(lpFiles.mapFileName)
		assert.True(t, os.IsNotExist(err), "map file removed")
	})
}
