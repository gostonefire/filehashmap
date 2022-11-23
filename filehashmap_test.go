//go:build integration

package filehashmap

import (
	"fmt"
	"github.com/gostonefire/filehashmap/internal/utils"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"os"
	"testing"
)

const testHashMap string = "test"

func TestNewFileHashMap(t *testing.T) {
	t.Run("creates file hash map", func(t *testing.T) {
		// Prepare

		// Execute
		fhm, info, err := NewFileHashMap(testHashMap, 100000, 16, 10, nil)

		// Check
		assert.NoError(t, err, "creates file hash map")
		assert.NotNil(t, fhm.fileManagement, "file management is assigned")
		assert.Equal(t, testHashMap, fhm.name, "correct name")

		sp := fhm.fileManagement.GetStorageParameters()
		assert.Equal(t, sp.RecordsPerBucket, info.RecordsPerBucket, "correct records per bucket in info")
		assert.Equal(t, sp.FillFactor, info.AverageBucketFillFactor, "correct fill factor in info")
		assert.Equal(t, sp.NumberOfBuckets, info.NumberOfBuckets, "correct number of buckets in info")
		assert.Equal(t, sp.MapFileSize, info.FileSize, "correct filesize in info")
		assert.Equal(t, int64(100000), sp.InitialUniqueKeys, "correct initial unique keys")
		assert.Equal(t, int64(16), sp.KeyLength, "correct key length")
		assert.Equal(t, int64(10), sp.ValueLength, "correct value length")
		assert.True(t, sp.InternalAlgorithm, "has internal hash algorithm")

		// Clean up
		err = fhm.RemoveFiles()
		assert.NoError(t, err, "removes files")

		_, err = os.Stat(fmt.Sprintf("%s-map.bin", testHashMap))
		assert.True(t, os.IsNotExist(err), "map file removed")
		_, err = os.Stat(fmt.Sprintf("%s-ovfl.bin", testHashMap))
		assert.True(t, os.IsNotExist(err), "overflow file removed")
	})
}

func TestNewFromExistingFiles(t *testing.T) {
	t.Run("opens an existing file", func(t *testing.T) {
		// Prepare
		fhmInit, infoInit, err := NewFileHashMap(testHashMap, 100000, 16, 10, nil)
		assert.NoError(t, err, "creates file hash map")

		fhmInit.CloseFiles()

		// Execute
		fhm, info, err := NewFromExistingFiles(testHashMap, nil)

		// Check
		assert.NoError(t, err, "opens file hash map")
		assert.Equal(t, testHashMap, fhm.name, "correct name")
		assert.Equal(t, infoInit.RecordsPerBucket, info.RecordsPerBucket, "records per bucket preserved")
		assert.Equal(t, infoInit.AverageBucketFillFactor, info.AverageBucketFillFactor, "fill factor preserved")
		assert.Equal(t, infoInit.NumberOfBuckets, info.NumberOfBuckets, "number of buckets preserved")
		assert.Equal(t, infoInit.FileSize, info.FileSize, "filesize preserved")

		// Clean up
		err = fhm.RemoveFiles()
		assert.NoError(t, err, "removes files")

		_, err = os.Stat(fmt.Sprintf("%s-map.bin", testHashMap))
		assert.True(t, os.IsNotExist(err), "map file removed")
		_, err = os.Stat(fmt.Sprintf("%s-ovfl.bin", testHashMap))
		assert.True(t, os.IsNotExist(err), "overflow file removed")
	})

	t.Run("error when reopen an non-existing file", func(t *testing.T) {
		// Execute
		_, _, err := NewFromExistingFiles(testHashMap, nil)

		// Check
		assert.Error(t, err)
	})

	t.Run("error when supplying an invalid key length", func(t *testing.T) {
		// Execute
		_, _, err := NewFileHashMap(testHashMap, 10, -2, 10, nil)

		// Check
		assert.Error(t, err)
	})

	t.Run("error when supplying an invalid value length", func(t *testing.T) {
		// Execute
		_, _, err := NewFileHashMap(testHashMap, 10, 16, 0, nil)

		// Check
		assert.Error(t, err)
	})

	t.Run("error when supplying an invalid name", func(t *testing.T) {
		// Execute
		_, _, err := NewFileHashMap("", 10, 16, 10, nil)

		// Check
		assert.Error(t, err)
	})

}

func TestReorgFiles(t *testing.T) {
	t.Run("reorganizes file", func(t *testing.T) {
		// Prepare
		newName := fmt.Sprintf("%s-reorg", testHashMap)
		mapFileName := fmt.Sprintf("%s-map.bin", testHashMap)
		ovflFileName := fmt.Sprintf("%s-ovfl.bin", testHashMap)

		rand.Seed(123)
		keys := make([][]byte, 100)
		values := make([][]byte, 100)
		for i := 0; i < 100; i++ {
			key := make([]byte, 5)
			rand.Read(key)
			keys[i] = key
			value := make([]byte, 10)
			rand.Read(value)
			values[i] = value
		}

		fhm, _, err := NewFileHashMap(testHashMap, 10, 5, 10, nil)
		assert.NoError(t, err, "create fil hash map")

		for i := 0; i < 100; i++ {
			err = fhm.Set(keys[i], values[i])
			assert.NoError(t, err, "set key/value in file hash map")
		}

		fhm.CloseFiles()

		reorgConf := ReorgConf{
			InitialUniqueKeys:     1000,
			KeyExtension:          5,
			PrependKeyExtension:   false,
			ValueExtension:        10,
			PrependValueExtension: true,
			NewBucketAlgorithm:    nil,
			OldBucketAlgorithm:    nil,
		}

		// Execute
		_, _, err = ReorgFiles(testHashMap, reorgConf, false)

		// Check
		assert.NoError(t, err, "run reorg files")

		fhm, _, err = NewFromExistingFiles(newName, nil)
		assert.NoError(t, err, "open reorged files")

		for i := 0; i < 100; i++ {
			key := make([]byte, len(keys[i]))
			valueToBe := make([]byte, len(values[i]))
			_ = copy(key, keys[i])
			_ = copy(valueToBe, values[i])
			key = append(key, make([]byte, 5)...)
			valueToBe = append(make([]byte, 10), valueToBe...)
			value, err := fhm.Get(key)
			assert.NoError(t, err, "get value for extended key")

			if !utils.IsEqual(valueToBe, value) {
				assert.Fail(t, "correct value")
			}
		}

		// Clean up
		err = fhm.RemoveFiles()
		assert.NoError(t, err, "backup file can be removed after close")

		err = os.Remove(mapFileName)
		assert.NoError(t, err, "backup map file can be removed after close")

		err = os.Remove(ovflFileName)
		assert.NoError(t, err, "backup overflow file can be removed after close")
	})
}
