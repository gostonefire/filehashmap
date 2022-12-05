//go:build integration

package filehashmap

import (
	"fmt"
	"github.com/gostonefire/filehashmap/crt"
	"github.com/gostonefire/filehashmap/internal/utils"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"os"
	"testing"
)

const testHashMap string = "test"

type TestCaseFileHashMap struct {
	crtToName   string
	crtFromName string
	toBuckets   int
	fromBuckets int
	keyLength   int
	valueLength int
	toCrt       int
	fromCrt     int
}

func TestNewFileHashMap(t *testing.T) {
	t.Run("NewFileHashMap tests for all CRTs", func(t *testing.T) {
		// Prepare
		tests := []TestCaseFileHashMap{
			{crtToName: "SeparateChaining", toBuckets: 100000, keyLength: 16, valueLength: 10, toCrt: crt.SeparateChaining},
			{crtToName: "LinearProbing", toBuckets: 100000, keyLength: 16, valueLength: 10, toCrt: crt.LinearProbing},
			{crtToName: "QuadraticProbing", toBuckets: 100000, keyLength: 16, valueLength: 10, toCrt: crt.QuadraticProbing},
		}

		for _, test := range tests {
			t.Run(fmt.Sprintf("creates file hash map for %s", test.crtToName), func(t *testing.T) {
				// Prepare

				// Execute
				fhm, info, err := NewFileHashMap(testHashMap, test.toCrt, test.toBuckets, test.keyLength, test.valueLength, nil)

				// Check
				assert.NoError(t, err, "creates file hash map")
				assert.NotNil(t, fhm.fileManagement, "file management is assigned")
				assert.Equal(t, testHashMap, fhm.name, "correct name")

				sp := fhm.fileManagement.GetStorageParameters()
				assert.Equal(t, int(sp.NumberOfBucketsNeeded), info.NumberOfBucketsNeeded, "correct number of buckets needed in info")
				assert.Equal(t, int(sp.NumberOfBucketsAvailable), info.NumberOfBucketsAvailable, "correct number of buckets available in info")
				assert.Equal(t, int(sp.MapFileSize), info.FileSize, "correct filesize in info")
				assert.Equal(t, int64(test.toBuckets), sp.NumberOfBucketsNeeded, "correct buckets needed")
				assert.Equal(t, int64(test.keyLength), sp.KeyLength, "correct key length")
				assert.Equal(t, int64(test.valueLength), sp.ValueLength, "correct value length")
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
	})

	t.Run("error when supplying an invalid crt", func(t *testing.T) {
		// Execute
		_, _, err := NewFileHashMap(testHashMap, 0, 10, 16, 10, nil)

		// Check
		assert.Error(t, err)

		// Execute
		_, _, err = NewFileHashMap(testHashMap, 5, 10, 16, 10, nil)

		// Check
		assert.Error(t, err)
	})

	t.Run("error when supplying an invalid key length", func(t *testing.T) {
		// Execute
		_, _, err := NewFileHashMap(testHashMap, crt.SeparateChaining, 10, -2, 10, nil)

		// Check
		assert.Error(t, err)
	})

	t.Run("error when supplying an invalid value length", func(t *testing.T) {
		// Execute
		_, _, err := NewFileHashMap(testHashMap, crt.SeparateChaining, 10, 16, 0, nil)

		// Check
		assert.Error(t, err)
	})

	t.Run("error when supplying an invalid name", func(t *testing.T) {
		// Execute
		_, _, err := NewFileHashMap("", crt.SeparateChaining, 10, 16, 10, nil)

		// Check
		assert.Error(t, err)
	})
}

func TestNewFromExistingFiles(t *testing.T) {
	t.Run("NewFromExistingFiles tests for all CRTs", func(t *testing.T) {
		// Prepare
		tests := []TestCaseFileHashMap{
			{crtToName: "SeparateChaining", toBuckets: 100000, keyLength: 16, valueLength: 10, toCrt: crt.SeparateChaining},
			{crtToName: "LinearProbing", toBuckets: 100000, keyLength: 16, valueLength: 10, toCrt: crt.LinearProbing},
			{crtToName: "QuadraticProbing", toBuckets: 100000, keyLength: 16, valueLength: 10, toCrt: crt.QuadraticProbing},
		}

		for _, test := range tests {
			t.Run(fmt.Sprintf("opens an existing file for %s", test.crtToName), func(t *testing.T) {
				// Prepare
				fhmInit, infoInit, err := NewFileHashMap(testHashMap, test.toCrt, test.toBuckets, test.keyLength, test.valueLength, nil)
				assert.NoError(t, err, "creates file hash map")

				fhmInit.CloseFiles()

				// Execute
				fhm, info, err := NewFromExistingFiles(testHashMap, nil)

				// Check
				assert.NoError(t, err, "opens file hash map")
				assert.Equal(t, testHashMap, fhm.name, "correct name")
				assert.Equal(t, infoInit.NumberOfBucketsNeeded, info.NumberOfBucketsNeeded, "number of buckets needed preserved")
				assert.Equal(t, infoInit.NumberOfBucketsAvailable, info.NumberOfBucketsAvailable, "number of buckets available preserved")
				assert.Equal(t, infoInit.FileSize, info.FileSize, "filesize preserved")

				// Clean up
				err = fhm.RemoveFiles()
				assert.NoError(t, err, "removes files")

				_, err = os.Stat(fmt.Sprintf("%s-map.bin", testHashMap))
				assert.True(t, os.IsNotExist(err), "map file removed")
				_, err = os.Stat(fmt.Sprintf("%s-ovfl.bin", testHashMap))
				assert.True(t, os.IsNotExist(err), "overflow file removed")
			})
		}
	})

	t.Run("error when reopen an non-existing file", func(t *testing.T) {
		// Execute
		_, _, err := NewFromExistingFiles(testHashMap, nil)

		// Check
		assert.Error(t, err)
	})
}

func TestReorgFiles(t *testing.T) {
	t.Run("ReorgFiles tests for all CRTs", func(t *testing.T) {
		// Prepare
		tests := []TestCaseFileHashMap{
			{crtFromName: "SeparateChaining", crtToName: "SeparateChaining", fromBuckets: 10, toBuckets: 100, keyLength: 5, valueLength: 10, fromCrt: crt.SeparateChaining, toCrt: crt.SeparateChaining},
			{crtFromName: "LinearProbing", crtToName: "LinearProbing", fromBuckets: 100, toBuckets: 100, keyLength: 5, valueLength: 10, fromCrt: crt.LinearProbing, toCrt: crt.LinearProbing},
			{crtFromName: "QuadraticProbing", crtToName: "QuadraticProbing", fromBuckets: 100, toBuckets: 100, keyLength: 5, valueLength: 10, fromCrt: crt.QuadraticProbing, toCrt: crt.QuadraticProbing},
			{crtFromName: "DoubleHashing", crtToName: "DoubleHashing", fromBuckets: 100, toBuckets: 100, keyLength: 5, valueLength: 10, fromCrt: crt.DoubleHashing, toCrt: crt.DoubleHashing},

			{crtFromName: "SeparateChaining", crtToName: "LinearProbing", fromBuckets: 10, toBuckets: 100, keyLength: 5, valueLength: 10, fromCrt: crt.SeparateChaining, toCrt: crt.LinearProbing},
			{crtFromName: "SeparateChaining", crtToName: "QuadraticProbing", fromBuckets: 10, toBuckets: 100, keyLength: 5, valueLength: 10, fromCrt: crt.SeparateChaining, toCrt: crt.QuadraticProbing},
			{crtFromName: "SeparateChaining", crtToName: "DoubleHashing", fromBuckets: 10, toBuckets: 100, keyLength: 5, valueLength: 10, fromCrt: crt.SeparateChaining, toCrt: crt.DoubleHashing},
			{crtFromName: "LinearProbing", crtToName: "QuadraticProbing", fromBuckets: 100, toBuckets: 100, keyLength: 5, valueLength: 10, fromCrt: crt.LinearProbing, toCrt: crt.QuadraticProbing},
			{crtFromName: "LinearProbing", crtToName: "DoubleHashing", fromBuckets: 100, toBuckets: 100, keyLength: 5, valueLength: 10, fromCrt: crt.LinearProbing, toCrt: crt.DoubleHashing},
			{crtFromName: "QuadraticProbing", crtToName: "DoubleHashing", fromBuckets: 100, toBuckets: 100, keyLength: 5, valueLength: 10, fromCrt: crt.QuadraticProbing, toCrt: crt.DoubleHashing},

			{crtFromName: "LinearProbing", crtToName: "SeparateChaining", fromBuckets: 100, toBuckets: 10, keyLength: 5, valueLength: 10, fromCrt: crt.LinearProbing, toCrt: crt.SeparateChaining},
			{crtFromName: "QuadraticProbing", crtToName: "SeparateChaining", fromBuckets: 100, toBuckets: 10, keyLength: 5, valueLength: 10, fromCrt: crt.QuadraticProbing, toCrt: crt.SeparateChaining},
			{crtFromName: "DoubleHashing", crtToName: "SeparateChaining", fromBuckets: 100, toBuckets: 10, keyLength: 5, valueLength: 10, fromCrt: crt.DoubleHashing, toCrt: crt.SeparateChaining},
			{crtFromName: "QuadraticProbing", crtToName: "LinearProbing", fromBuckets: 100, toBuckets: 100, keyLength: 5, valueLength: 10, fromCrt: crt.QuadraticProbing, toCrt: crt.LinearProbing},
			{crtFromName: "DoubleHashing", crtToName: "LinearProbing", fromBuckets: 100, toBuckets: 100, keyLength: 5, valueLength: 10, fromCrt: crt.DoubleHashing, toCrt: crt.LinearProbing},
			{crtFromName: "DoubleHashing", crtToName: "QuadraticProbing", fromBuckets: 100, toBuckets: 100, keyLength: 5, valueLength: 10, fromCrt: crt.DoubleHashing, toCrt: crt.QuadraticProbing},
		}

		for _, test := range tests {
			t.Run(fmt.Sprintf("reorganizes file between %s and %s", test.crtFromName, test.crtToName), func(t *testing.T) {
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

				fhm, _, err := NewFileHashMap(testHashMap, test.fromCrt, test.fromBuckets, test.keyLength, test.valueLength, nil)
				assert.NoError(t, err, "create fil hash map")

				for i := 0; i < 100; i++ {
					err = fhm.Set(keys[i], values[i])
					assert.NoError(t, err, "set key/value in file hash map")
				}

				fhm.CloseFiles()

				reorgConf := ReorgConf{
					CollisionResolutionTechnique: test.toCrt,
					NumberOfBucketsNeeded:        test.toBuckets,
					KeyExtension:                 5,
					PrependKeyExtension:          false,
					ValueExtension:               10,
					PrependValueExtension:        true,
					NewHashAlgorithm:             nil,
					OldHashAlgorithm:             nil,
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
					assert.NoErrorf(t, err, "get value for extended key #%d", i)

					if !utils.IsEqual(valueToBe, value) {
						assert.Failf(t, "fail equal test", "correct value for key #%d", i)
					}
				}

				// Clean up
				err = fhm.RemoveFiles()
				assert.NoError(t, err, "new file can be removed after close")

				err = os.Remove(mapFileName)
				assert.NoError(t, err, "backup map file can be removed after close")

				if _, err = os.Stat(ovflFileName); err == nil {
					err = os.Remove(ovflFileName)
					assert.NoError(t, err, "backup overflow file can be removed after close")
				}
			})

		}
	})
}
