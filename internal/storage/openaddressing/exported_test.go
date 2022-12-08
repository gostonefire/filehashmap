//go:build unit

package openaddressing

import (
	"fmt"
	"github.com/gostonefire/filehashmap/crt"
	"github.com/gostonefire/filehashmap/internal/model"
	"github.com/gostonefire/filehashmap/internal/storage"
	"github.com/gostonefire/filehashmap/internal/utils"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"os"
	"testing"
)

type TestCaseOAFiles struct {
	crtName     string
	buckets     int64
	keyLength   int64
	valueLength int64
	crt         int
}

func TestNewOAFiles(t *testing.T) {
	t.Run("creates QAFiles instances for all CRTs", func(t *testing.T) {
		// Prepare
		tests := []TestCaseOAFiles{
			{crtName: "LinearProbing", buckets: 10, keyLength: 16, valueLength: 10, crt: crt.LinearProbing},
			{crtName: "QuadraticProbing", buckets: 10, keyLength: 16, valueLength: 10, crt: crt.QuadraticProbing},
			{crtName: "DoubleHashing", buckets: 10, keyLength: 16, valueLength: 10, crt: crt.DoubleHashing},
		}

		for _, test := range tests {
			t.Run(fmt.Sprintf("creates a new OAFiles instance for %s", test.crtName), func(t *testing.T) {
				// Prepare
				crtConf := model.CRTConf{
					Name:                         "test",
					NumberOfBucketsNeeded:        test.buckets,
					KeyLength:                    test.keyLength,
					ValueLength:                  test.valueLength,
					CollisionResolutionTechnique: test.crt,
					HashAlgorithm:                nil,
				}

				// Execute
				oaFiles, err := NewOAFiles(crtConf)

				// Check
				mapFileSize := storage.MapFileHeaderLength + oaFiles.numberOfBucketsAvailable*(crtConf.KeyLength+crtConf.ValueLength+1)
				assert.NoError(t, err, "create new OAFiles instance")
				assert.Equal(t, "test-map.bin", oaFiles.mapFileName, "map filename correct")
				assert.NotNil(t, oaFiles.mapFile, "has map file")
				assert.GreaterOrEqual(t, oaFiles.numberOfBucketsAvailable, crtConf.NumberOfBucketsNeeded, "needed buckets preserved in number of buckets")
				assert.Equal(t, crtConf.KeyLength, oaFiles.keyLength, "key length preserved")
				assert.Equal(t, crtConf.ValueLength, oaFiles.valueLength, "value length preserved")
				assert.NotZero(t, oaFiles.maxBucketNo, "max bucket number is not zero")
				assert.Equal(t, oaFiles.mapFileSize, mapFileSize, "map file size in correct length")
				assert.NotNil(t, oaFiles.hashAlgorithm, "hash algorithm is assigned")

				stat, err := os.Stat(oaFiles.mapFileName)
				assert.NoError(t, err, "map file exists")
				assert.Equal(t, oaFiles.mapFileSize, stat.Size(), "map file in correct size")

				oaFiles.CloseFiles()
				err = oaFiles.RemoveFiles()
				assert.NoError(t, err, "removes files")

				_, err = os.Stat(oaFiles.mapFileName)
				assert.True(t, os.IsNotExist(err), "map file removed")

				// Clean up
			})
		}
	})
}

func TestNewOAFilesFromExistingFiles(t *testing.T) {
	t.Run("opens existing QAFiles for all CRTs", func(t *testing.T) {
		// Prepare
		tests := []TestCaseOAFiles{
			{crtName: "LinearProbing", buckets: 10, keyLength: 16, valueLength: 10, crt: crt.LinearProbing},
			{crtName: "QuadraticProbing", buckets: 10, keyLength: 16, valueLength: 10, crt: crt.QuadraticProbing},
			{crtName: "DoubleHashing", buckets: 10, keyLength: 16, valueLength: 10, crt: crt.DoubleHashing},
		}

		for _, test := range tests {
			t.Run(fmt.Sprintf("opens OAFiles on existing files for %s", test.crtName), func(t *testing.T) {
				// Prepare
				crtConf := model.CRTConf{
					Name:                         "test",
					NumberOfBucketsNeeded:        test.buckets,
					KeyLength:                    test.keyLength,
					ValueLength:                  test.valueLength,
					CollisionResolutionTechnique: test.crt,
					HashAlgorithm:                nil,
				}

				oaFilesInit, err := NewOAFiles(crtConf)
				assert.NoError(t, err, "create new OAFiles instance")
				oaFilesInit.CloseFiles()

				// Execute
				oaFiles, err := NewOAFilesFromExistingFiles("test", nil)

				// Check
				mapFileSize := storage.MapFileHeaderLength + oaFiles.numberOfBucketsAvailable*(crtConf.KeyLength+crtConf.ValueLength+1)
				assert.NoError(t, err, "opens existing files")
				assert.Equal(t, "test-map.bin", oaFiles.mapFileName, "map filename correct")
				assert.NotNil(t, oaFiles.mapFile, "has map file")
				assert.GreaterOrEqual(t, oaFiles.numberOfBucketsAvailable, crtConf.NumberOfBucketsNeeded, "needed buckets preserved in number of buckets")
				assert.Equal(t, crtConf.KeyLength, oaFiles.keyLength, "key length preserved")
				assert.Equal(t, crtConf.ValueLength, oaFiles.valueLength, "value length preserved")
				assert.NotZero(t, oaFiles.maxBucketNo, "max bucket number is not zero")
				assert.Equal(t, oaFiles.mapFileSize, mapFileSize, "map file size in correct length")
				assert.NotNil(t, oaFiles.hashAlgorithm, "hash algorithm is assigned")

				// Clean up
				oaFiles.CloseFiles()
				err = oaFiles.RemoveFiles()
				assert.NoError(t, err, "removes files")

				_, err = os.Stat(oaFiles.mapFileName)
				assert.True(t, os.IsNotExist(err), "map file removed")
			})
		}
	})

}

func TestOAFiles_GetStorageParameters(t *testing.T) {
	t.Run("gets storage parameters for all CRTs", func(t *testing.T) {
		// Prepare
		tests := []TestCaseOAFiles{
			{crtName: "LinearProbing", buckets: 10, keyLength: 16, valueLength: 10, crt: crt.LinearProbing},
			{crtName: "QuadraticProbing", buckets: 10, keyLength: 16, valueLength: 10, crt: crt.QuadraticProbing},
			{crtName: "DoubleHashing", buckets: 10, keyLength: 16, valueLength: 10, crt: crt.DoubleHashing},
		}

		for _, test := range tests {
			t.Run(fmt.Sprintf("gets storage parameters for %s", test.crtName), func(t *testing.T) {
				// Prepare
				crtConf := model.CRTConf{
					Name:                         "test",
					NumberOfBucketsNeeded:        test.buckets,
					KeyLength:                    test.keyLength,
					ValueLength:                  test.valueLength,
					CollisionResolutionTechnique: test.crt,
					HashAlgorithm:                nil,
				}

				oaFiles, err := NewOAFiles(crtConf)
				assert.NoError(t, err, "create new OAFiles instance")

				// Execute
				sp := oaFiles.GetStorageParameters()

				// Check
				assert.Equal(t, test.crt, sp.CollisionResolutionTechnique, "correct crt")
				assert.Equal(t, crtConf.NumberOfBucketsNeeded, sp.NumberOfBucketsNeeded, "buckets needed preserved")
				assert.Equal(t, crtConf.KeyLength, sp.KeyLength, "key length preserved")
				assert.Equal(t, crtConf.ValueLength, sp.ValueLength, "value length preserved")
				assert.Equal(t, oaFiles.numberOfBucketsAvailable, sp.NumberOfBucketsAvailable, "number of buckets preserved")
				assert.Equal(t, oaFiles.mapFileSize, sp.MapFileSize, "map file size preserved")
				assert.True(t, sp.InternalAlgorithm, "indicates using internal hash algorithm")

				// Clean up
				oaFiles.CloseFiles()
				err = oaFiles.RemoveFiles()
				assert.NoError(t, err, "removes files")

				_, err = os.Stat(oaFiles.mapFileName)
				assert.True(t, os.IsNotExist(err), "map file removed")

			})
		}
	})
}

func TestOAFiles_Set(t *testing.T) {
	t.Run("sets a record in file for all CRTs", func(t *testing.T) {
		// Prepare
		tests := []TestCaseOAFiles{
			{crtName: "LinearProbing", buckets: 1000, keyLength: 16, valueLength: 10, crt: crt.LinearProbing},
			{crtName: "QuadraticProbing", buckets: 1000, keyLength: 16, valueLength: 10, crt: crt.QuadraticProbing},
			{crtName: "DoubleHashing", buckets: 1000, keyLength: 16, valueLength: 10, crt: crt.DoubleHashing},
		}

		for _, test := range tests {
			t.Run(fmt.Sprintf("sets a record in file for %s", test.crtName), func(t *testing.T) {
				// Prepare
				crtConf := model.CRTConf{
					Name:                         "test",
					NumberOfBucketsNeeded:        test.buckets,
					KeyLength:                    test.keyLength,
					ValueLength:                  test.valueLength,
					CollisionResolutionTechnique: test.crt,
					HashAlgorithm:                nil,
				}

				oaFiles, err := NewOAFiles(crtConf)
				assert.NoError(t, err, "create new OAFiles instance")

				record := model.Record{
					Key:   []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
					Value: []byte{16, 17, 18, 19, 20, 21, 22, 23, 24, 25},
				}

				// Execute
				err = oaFiles.Set(record)

				// Check
				assert.NoError(t, err, "sets record to file")

				// Clean up
				oaFiles.CloseFiles()
				err = oaFiles.RemoveFiles()
				assert.NoError(t, err, "removes files")

				_, err = os.Stat(oaFiles.mapFileName)
				assert.True(t, os.IsNotExist(err), "map file removed")
			})

			t.Run(fmt.Sprintf("sets all available records in file for %s", test.crtName), func(t *testing.T) {
				// Prepare
				crtConf := model.CRTConf{
					Name:                         "test",
					NumberOfBucketsNeeded:        test.buckets,
					KeyLength:                    test.keyLength,
					ValueLength:                  test.valueLength,
					CollisionResolutionTechnique: test.crt,
					HashAlgorithm:                nil,
				}

				oaFiles, err := NewOAFiles(crtConf)
				assert.NoError(t, err, "create new OAFiles instance")

				records := make([]model.Record, oaFiles.numberOfBucketsAvailable)
				for i := int64(0); i < oaFiles.numberOfBucketsAvailable; i++ {
					records[i].Key = make([]byte, 16)
					rand.Read(records[i].Key)
					records[i].Value = make([]byte, 10)
					rand.Read(records[i].Value)
				}

				// Execute and Check
				for i := int64(0); i < oaFiles.numberOfBucketsAvailable; i++ {
					err = oaFiles.Set(records[i])
					assert.NoErrorf(t, err, "sets record #%d to file", i)
				}

				// Clean up
				oaFiles.CloseFiles()
				err = oaFiles.RemoveFiles()
				assert.NoError(t, err, "removes files")

				_, err = os.Stat(oaFiles.mapFileName)
				assert.True(t, os.IsNotExist(err), "map file removed")
			})

			t.Run(fmt.Sprintf("sets to many records in file for %s", test.crtName), func(t *testing.T) {
				// Prepare
				crtConf := model.CRTConf{
					Name:                         "test",
					NumberOfBucketsNeeded:        test.buckets,
					KeyLength:                    test.keyLength,
					ValueLength:                  test.valueLength,
					CollisionResolutionTechnique: test.crt,
					HashAlgorithm:                nil,
				}

				oaFiles, err := NewOAFiles(crtConf)
				assert.NoError(t, err, "create new OAFiles instance")

				records := make([]model.Record, oaFiles.numberOfBucketsAvailable+1)
				for i := int64(0); i < oaFiles.numberOfBucketsAvailable+1; i++ {
					records[i].Key = make([]byte, 16)
					rand.Read(records[i].Key)
					records[i].Value = make([]byte, 10)
					rand.Read(records[i].Value)
				}

				for i := int64(0); i < oaFiles.numberOfBucketsAvailable; i++ {
					err = oaFiles.Set(records[i])
					assert.NoErrorf(t, err, "sets record #%d to file", i)
				}

				// Execute
				err = oaFiles.Set(records[oaFiles.numberOfBucketsAvailable])

				// Check
				assert.ErrorIs(t, err, crt.MapFileFull{}, "correct error when map file is full")

				// Clean up
				oaFiles.CloseFiles()
				err = oaFiles.RemoveFiles()
				assert.NoError(t, err, "removes files")

				_, err = os.Stat(oaFiles.mapFileName)
				assert.True(t, os.IsNotExist(err), "map file removed")
			})
		}
	})
}

func TestOAFiles_Get(t *testing.T) {
	t.Run("gets a record from file for all CRTs", func(t *testing.T) {
		// Prepare
		tests := []TestCaseOAFiles{
			{crtName: "LinearProbing", buckets: 10, keyLength: 16, valueLength: 10, crt: crt.LinearProbing},
			{crtName: "QuadraticProbing", buckets: 10, keyLength: 16, valueLength: 10, crt: crt.QuadraticProbing},
			{crtName: "DoubleHashing", buckets: 10, keyLength: 16, valueLength: 10, crt: crt.DoubleHashing},
		}

		for _, test := range tests {
			t.Run(fmt.Sprintf("gets a record from file for %s", test.crtName), func(t *testing.T) {
				// Prepare
				crtConf := model.CRTConf{
					Name:                         "test",
					NumberOfBucketsNeeded:        test.buckets,
					KeyLength:                    test.keyLength,
					ValueLength:                  test.valueLength,
					CollisionResolutionTechnique: test.crt,
					HashAlgorithm:                nil,
				}

				oaFiles, err := NewOAFiles(crtConf)
				assert.NoError(t, err, "create new OAFiles instance")

				recordInit := model.Record{
					Key:   []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
					Value: []byte{16, 17, 18, 19, 20, 21, 22, 23, 24, 25},
				}

				err = oaFiles.Set(recordInit)
				assert.NoError(t, err, "sets record to file")

				// Execute
				record, err := oaFiles.Get(model.Record{Key: recordInit.Key})

				// Check
				assert.NoError(t, err, "gets a record from file")
				assert.Equal(t, model.RecordOccupied, record.State, "record marked in use")
				assert.NotZero(t, record.RecordAddress, "has valid record address")
				assert.False(t, record.IsOverflow, "record not marked as overflow")
				assert.Zero(t, record.NextOverflow, "has no valid overflow address")
				assert.True(t, utils.IsEqual(recordInit.Key, record.Key), "key is preserved")
				assert.True(t, utils.IsEqual(recordInit.Value, record.Value), "value is preserved")

				// Clean up
				oaFiles.CloseFiles()
				err = oaFiles.RemoveFiles()
				assert.NoError(t, err, "removes files")

				_, err = os.Stat(oaFiles.mapFileName)
				assert.True(t, os.IsNotExist(err), "map file removed")

			})
		}
	})
}

func TestOAFiles_Delete(t *testing.T) {
	t.Run("deletes a bucket record from file for all CRTs", func(t *testing.T) {
		// Prepare
		tests := []TestCaseOAFiles{
			{crtName: "LinearProbing", buckets: 10, keyLength: 16, valueLength: 10, crt: crt.LinearProbing},
			{crtName: "QuadraticProbing", buckets: 10, keyLength: 16, valueLength: 10, crt: crt.QuadraticProbing},
			{crtName: "DoubleHashing", buckets: 10, keyLength: 16, valueLength: 10, crt: crt.DoubleHashing},
		}

		for _, test := range tests {
			t.Run(fmt.Sprintf("deletes a bucket record from file for %s", test.crtName), func(t *testing.T) {
				// Prepare
				crtConf := model.CRTConf{
					Name:                         "test",
					NumberOfBucketsNeeded:        test.buckets,
					KeyLength:                    test.keyLength,
					ValueLength:                  test.valueLength,
					CollisionResolutionTechnique: test.crt,
					HashAlgorithm:                nil,
				}

				oaFiles, err := NewOAFiles(crtConf)
				assert.NoError(t, err, "create new OAFiles instance")

				recordInit := model.Record{
					Key:   []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
					Value: []byte{16, 17, 18, 19, 20, 21, 22, 23, 24, 25},
				}

				err = oaFiles.Set(recordInit)
				assert.NoError(t, err, "sets record to file")

				record, err := oaFiles.Get(model.Record{Key: recordInit.Key})
				assert.NoError(t, err, "gets a record from file")

				// Execute
				err = oaFiles.Delete(record)

				// Check
				assert.NoError(t, err, "deletes a record from file")

				record, err = oaFiles.Get(model.Record{Key: recordInit.Key})
				assert.ErrorIs(t, err, crt.NoRecordFound{}, "returns correct error")

				emptyRecord := model.Record{}
				assert.Equal(t, emptyRecord.State, record.State, "in use is according empty record")
				assert.Equal(t, emptyRecord.IsOverflow, record.IsOverflow, "is overflow is according empty record")
				assert.Equal(t, emptyRecord.RecordAddress, record.RecordAddress, "record address is according empty record")
				assert.Equal(t, emptyRecord.NextOverflow, record.NextOverflow, "next overflow is according empty record")
				assert.True(t, utils.IsEqual(emptyRecord.Key, record.Key), "key is according empty record")
				assert.True(t, utils.IsEqual(emptyRecord.Value, record.Value), "value is according empty record")

				// Clean up
				oaFiles.CloseFiles()
				err = oaFiles.RemoveFiles()
				assert.NoError(t, err, "removes files")

				_, err = os.Stat(oaFiles.mapFileName)
				assert.True(t, os.IsNotExist(err), "map file removed")

			})
		}
	})
}

func TestOAFiles_GetBucket(t *testing.T) {
	t.Run("returns a bucket for all CRTs", func(t *testing.T) {
		// Prepare
		tests := []TestCaseOAFiles{
			{crtName: "LinearProbing", buckets: 10, keyLength: 16, valueLength: 10, crt: crt.LinearProbing},
			{crtName: "QuadraticProbing", buckets: 10, keyLength: 16, valueLength: 10, crt: crt.QuadraticProbing},
			{crtName: "DoubleHashing", buckets: 10, keyLength: 16, valueLength: 10, crt: crt.DoubleHashing},
		}

		for _, test := range tests {
			t.Run(fmt.Sprintf("returns a bucket for %s", test.crtName), func(t *testing.T) {
				// Prepare
				crtConf := model.CRTConf{
					Name:                         "test",
					NumberOfBucketsNeeded:        test.buckets,
					KeyLength:                    test.keyLength,
					ValueLength:                  test.valueLength,
					CollisionResolutionTechnique: test.crt,
					HashAlgorithm:                nil,
				}

				oaFiles, err := NewOAFiles(crtConf)
				assert.NoError(t, err, "create new OAFiles instance")

				records := make([]model.Record, oaFiles.numberOfBucketsAvailable-1)
				for i := int64(0); i < oaFiles.numberOfBucketsAvailable-1; i++ {
					records[i].Key = make([]byte, 16)
					rand.Read(records[i].Key)
					records[i].Value = make([]byte, 10)
					rand.Read(records[i].Value)

					err = oaFiles.Set(records[i])
					assert.NoErrorf(t, err, "sets record #%d to file", i)
				}

				// Execute
				bucket, iterator, err := oaFiles.GetBucket(2)

				// Check
				assert.NoError(t, err, "gets a bucket")
				assert.False(t, bucket.HasOverflow, "bucket has no overflow")
				assert.Zero(t, bucket.OverflowAddress, "bucket has no overflow address")
				assert.Nil(t, iterator, "no overflow iterator")
				assert.Equal(t, model.RecordOccupied, bucket.Record.State, "record in bucket is in use")

				// Clean up
				oaFiles.CloseFiles()
				err = oaFiles.RemoveFiles()
				assert.NoError(t, err, "removes files")

				_, err = os.Stat(oaFiles.mapFileName)
				assert.True(t, os.IsNotExist(err), "map file removed")

			})
		}
	})
}
