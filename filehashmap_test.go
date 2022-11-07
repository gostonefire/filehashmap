//go:build integration

package filehashmap

import (
	"fmt"
	"github.com/gostonefire/filehashmap/internal/conf"
	"github.com/gostonefire/filehashmap/internal/utils"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"os"
	"testing"
)

const testHashMap string = "unittest"

func TestNewFileHashMap(t *testing.T) {
	t.Run("creates valid file hash map parameters", func(t *testing.T) {
		// Prepare
		expectedFileSize := (1<<16)*(2*27+conf.BucketHeaderLength) + conf.MapFileHeaderLength

		// Execute
		fhm, info, err := NewFileHashMap(testHashMap, 100000, 16, 10, nil)

		// Check
		assert.NoError(t, err)

		assert.True(t, fhm.internalAlg)
		assert.Equal(t, int64(100000), fhm.initialUniqueKeys)
		assert.Equal(t, int64(16), fhm.keyLength)
		assert.Equal(t, int64(10), fhm.valueLength)
		assert.Equal(t, int64(2), fhm.recordsPerBucket)
		assert.Equal(t, int64(65536), fhm.numberOfBuckets)
		assert.Equal(t, int64(0), fhm.minBucketNo)
		assert.Equal(t, int64(65535), fhm.maxBucketNo)
		assert.Equal(t, expectedFileSize, fhm.fileSize)
		assert.Equal(t, 0.762939453125, info.AverageBucketFillFactor)
	})

	t.Run("creates valid files", func(t *testing.T) {
		// Prepare
		fhm, _, err := NewFileHashMap(testHashMap, 100000, 16, 10, nil)
		assert.NoError(t, err, "create new file hash map struct")

		// Execute
		err = fhm.CreateNewFiles()

		// Check
		assert.NoError(t, err, "create new files")

		stat, err := os.Stat(fhm.ovflFileName)
		assert.NoError(t, err, "overflow file exists")
		assert.Equal(t, conf.OvflFileHeaderLength, stat.Size(), "overflow file has correct size")

		stat, err = os.Stat(fhm.mapFileName)
		assert.NoError(t, err, "map file exists")
		assert.Equal(t, fhm.fileSize, stat.Size(), "map file has correct size")

		// Clean up
		err = fhm.RemoveFiles()
		assert.NoError(t, err, "files can be removed after close")
		_, err = os.Stat(fhm.ovflFileName)
		assert.Error(t, err, "overflow file is removed")
		_, err = os.Stat(fhm.mapFileName)
		assert.Error(t, err, "map file is removed")

	})
}

func TestNewFromExistingFiles(t *testing.T) {
	t.Run("opens an existing file", func(t *testing.T) {
		// Prepare
		fhm, _, err := NewFileHashMap(testHashMap, 100000, 16, 10, nil)
		assert.NoError(t, err, "create new file hash map struct")

		err = fhm.CreateNewFiles()
		assert.NoError(t, err, "create new files")

		stat, err := os.Stat(fhm.ovflFileName)
		assert.NoError(t, err, "overflow file exists")
		assert.Equal(t, conf.OvflFileHeaderLength, stat.Size(), "overflow file has correct size")

		stat, err = os.Stat(fhm.mapFileName)
		assert.NoError(t, err, "map file exists")
		assert.Equal(t, fhm.fileSize, stat.Size(), "map file has correct size")

		fhm.CloseFiles()

		expectedFileSize := (1<<16)*(2*27+conf.BucketHeaderLength) + conf.MapFileHeaderLength

		// Execute
		fhm2, _, err := NewFromExistingFiles(testHashMap, nil)

		// Check
		assert.True(t, fhm2.internalAlg, "same internal alg flag")
		assert.NoError(t, err, "open existing files")
		assert.Equal(t, int64(100000), fhm2.initialUniqueKeys, "same initial unique values")
		assert.Equal(t, int64(16), fhm2.keyLength, "same key length")
		assert.Equal(t, int64(10), fhm2.valueLength, "same value length")
		assert.Equal(t, int64(2), fhm2.recordsPerBucket, "same records per bucket")
		assert.Equal(t, int64(65536), fhm2.numberOfBuckets, "same number of bucket")
		assert.Equal(t, int64(0), fhm2.minBucketNo, "same min bucket no")
		assert.Equal(t, int64(65535), fhm2.maxBucketNo, "same max bucket no")
		assert.Equal(t, expectedFileSize, fhm2.fileSize, "same file size")

		// Clean up
		err = fhm2.RemoveFiles()
		assert.NoError(t, err, "files can be removed after close")
		_, err = os.Stat(fhm2.ovflFileName)
		assert.Error(t, err, "overflow file is removed")
		_, err = os.Stat(fhm2.mapFileName)
		assert.Error(t, err, "map file is removed")
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
		bakName := fmt.Sprintf("%s-original", testHashMap)
		bakMapFileName := fmt.Sprintf("%s-map.bin", bakName)
		bakOvflFileName := fmt.Sprintf("%s-ovfl.bin", bakName)

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
		err = fhm.CreateNewFiles()
		assert.NoError(t, err, "create fil hash map files")

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
			BucketAlgorithm:       nil,
		}

		// Execute
		_, _, err = ReorgFiles(testHashMap, reorgConf, false)

		// Check
		assert.NoError(t, err, "run reorg files")

		fhm, _, err = NewFromExistingFiles(testHashMap, nil)
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

		if _, err = os.Stat(bakMapFileName); err != nil {
			assert.Fail(t, "backup map file exist")
		}
		if _, err = os.Stat(bakOvflFileName); err != nil {
			assert.Fail(t, "backup overflow file exist")
		}

		// Clean up
		err = fhm.RemoveFiles()
		assert.NoError(t, err, "backup file can be removed after close")

		err = os.Remove(bakMapFileName)
		assert.NoError(t, err, "backup map file can be removed after close")

		err = os.Remove(bakOvflFileName)
		assert.NoError(t, err, "backup overflow file can be removed after close")
	})
}
