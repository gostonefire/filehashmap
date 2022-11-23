//go:build integration

package filehashmap

import (
	"fmt"
	"github.com/gostonefire/filehashmap/internal/utils"
	"github.com/gostonefire/filehashmap/storage"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"os"
	"testing"
)

func TestFileHashMap_Set(t *testing.T) {
	t.Run("sets a new record to file", func(t *testing.T) {
		// Prepare
		fhm, _, err := NewFileHashMap(testHashMap, 10000, 16, 10, nil)
		assert.NoError(t, err, "create new file hash map")

		key := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
		value := []byte{16, 17, 18, 19, 20, 21, 22, 23, 24, 25}

		// Execute
		err = fhm.Set(key, value)

		// Check
		assert.NoError(t, err, "set a record to file")

		// Clean up
		err = fhm.RemoveFiles()
		assert.NoError(t, err, "removes files")

		_, err = os.Stat(fmt.Sprintf("%s-map.bin", testHashMap))
		assert.True(t, os.IsNotExist(err), "map file removed")
		_, err = os.Stat(fmt.Sprintf("%s-ovfl.bin", testHashMap))
		assert.True(t, os.IsNotExist(err), "overflow file removed")
	})

	t.Run("updates an existing record in file", func(t *testing.T) {
		// Prepare
		fhm, _, err := NewFileHashMap(testHashMap, 10000, 16, 10, nil)
		assert.NoError(t, err, "create new file hash map")

		key := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
		value1 := []byte{16, 17, 18, 19, 20, 21, 22, 23, 24, 25}
		value2 := []byte{25, 24, 23, 22, 21, 20, 19, 18, 17, 16}

		err = fhm.Set(key, value1)
		assert.NoError(t, err, "set a record to file")

		// Execute
		err = fhm.Set(key, value2)

		// Check
		assert.NoError(t, err, "update an existing record in file")

		value, err := fhm.Get(key)
		assert.NoError(t, err, "get records from file")
		assert.True(t, utils.IsEqual(value2, value), "value is correct")

		// Clean up
		err = fhm.RemoveFiles()
		assert.NoError(t, err, "removes files")

		_, err = os.Stat(fmt.Sprintf("%s-map.bin", testHashMap))
		assert.True(t, os.IsNotExist(err), "map file removed")
		_, err = os.Stat(fmt.Sprintf("%s-ovfl.bin", testHashMap))
		assert.True(t, os.IsNotExist(err), "overflow file removed")
	})

	t.Run("sets records to overflow file", func(t *testing.T) {
		// Prepare
		fhm, _, err := NewFileHashMap(testHashMap, 10, 16, 10, nil)
		assert.NoError(t, err, "create new file hash map struct")

		keys := make([][]byte, 1000)
		values := make([][]byte, 1000)

		// Execute
		for i := 0; i < 1000; i++ {
			keys[i] = make([]byte, 16)
			rand.Read(keys[i])
			values[i] = make([]byte, 10)
			rand.Read(values[i])

			err = fhm.Set(keys[i], values[i])
			assert.NoErrorf(t, err, "sets record #%d to file", i)
		}

		// Check
		var value []byte
		for i := 0; i < 1000; i++ {
			value, err = fhm.Get(keys[i])
			assert.NoErrorf(t, err, "gets record #%d", i)
			assert.Truef(t, utils.IsEqual(values[i], value), "record #%d has correct value", i)
		}

		// Clean up
		err = fhm.RemoveFiles()
		assert.NoError(t, err, "removes files")

		_, err = os.Stat(fmt.Sprintf("%s-map.bin", testHashMap))
		assert.True(t, os.IsNotExist(err), "map file removed")
		_, err = os.Stat(fmt.Sprintf("%s-ovfl.bin", testHashMap))
		assert.True(t, os.IsNotExist(err), "overflow file removed")
	})

	t.Run("throws correct error when key is not found", func(t *testing.T) {
		// Prepare
		fhm, _, err := NewFileHashMap(testHashMap, 1000, 16, 10, nil)
		assert.NoError(t, err, "create new file hash map struct")

		key := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}

		// Execute
		_, err = fhm.Get(key)

		// Check
		assert.ErrorIs(t, err, storage.NoRecordFound{}, "get correct error")

		// Clean up
		err = fhm.RemoveFiles()
		assert.NoError(t, err, "removes files")

		_, err = os.Stat(fmt.Sprintf("%s-map.bin", testHashMap))
		assert.True(t, os.IsNotExist(err), "map file removed")
		_, err = os.Stat(fmt.Sprintf("%s-ovfl.bin", testHashMap))
		assert.True(t, os.IsNotExist(err), "overflow file removed")
	})
}

/* Get is thoroughly tested in the TestFileHashMap_Set -> "sets records to overflow file" test

func TestFileHashMap_Get(t *testing.T) {
	t.Run("gets records", func(t *testing.T) {

	})
}
*/

func TestPop(t *testing.T) {
	t.Run("pops records", func(t *testing.T) {
		// Prepare
		fhm, _, err := NewFileHashMap(testHashMap, 10, 16, 10, nil)
		assert.NoError(t, err, "create new file hash map struct")

		keys := make([][]byte, 1000)
		values := make([][]byte, 1000)

		for i := 0; i < 1000; i++ {
			keys[i] = make([]byte, 16)
			rand.Read(keys[i])
			values[i] = make([]byte, 10)
			rand.Read(values[i])

			err = fhm.Set(keys[i], values[i])
			assert.NoErrorf(t, err, "sets record #%d to file", i)
		}

		// Execute
		var value []byte
		for i := 0; i < 1000; i++ {
			value, err = fhm.Pop(keys[i])
			assert.NoErrorf(t, err, "pops record #%d", i)
			assert.Truef(t, utils.IsEqual(values[i], value), "record #%d has correct value", i)
		}

		// Check
		for i := 0; i < 1000; i++ {
			value, err = fhm.Get(keys[i])
			assert.ErrorIsf(t, err, storage.NoRecordFound{}, "gets correct error when geting poped record #%d", i)
			assert.Nilf(t, value, "value is nil in record #%d", i)
		}

		// Clean up
		err = fhm.RemoveFiles()
		assert.NoError(t, err, "removes files")

		_, err = os.Stat(fmt.Sprintf("%s-map.bin", testHashMap))
		assert.True(t, os.IsNotExist(err), "map file removed")
		_, err = os.Stat(fmt.Sprintf("%s-ovfl.bin", testHashMap))
		assert.True(t, os.IsNotExist(err), "overflow file removed")
	})
}

func TestStat(t *testing.T) {
	t.Run("produces statistics without distribution", func(t *testing.T) {
		// Prepare
		fhm, _, err := NewFileHashMap(testHashMap, 1000, 16, 10, nil)
		assert.NoError(t, err, "create new file hash map struct")

		keys := make([][]byte, 1001)
		values := make([][]byte, 1001)

		for i := 0; i < 1001; i++ {
			keys[i] = make([]byte, 16)
			rand.Read(keys[i])
			values[i] = make([]byte, 10)
			rand.Read(values[i])

			err = fhm.Set(keys[i], values[i])
			assert.NoErrorf(t, err, "sets record #%d to file", i)
		}

		// Execute
		stat, err := fhm.Stat(false)

		// Check
		assert.NoError(t, err, "gets statistics")
		assert.Equal(t, int64(1001), stat.Records, "correct number of record reported")
		assert.NotZero(t, stat.MapFileRecords, "map file is used")
		assert.NotZero(t, stat.OverflowRecords, "overflow file is used")
		assert.Nil(t, stat.BucketDistribution, "no distribution is provided")

		// Clean up
		err = fhm.RemoveFiles()
		assert.NoError(t, err, "removes files")

		_, err = os.Stat(fmt.Sprintf("%s-map.bin", testHashMap))
		assert.True(t, os.IsNotExist(err), "map file removed")
		_, err = os.Stat(fmt.Sprintf("%s-ovfl.bin", testHashMap))
		assert.True(t, os.IsNotExist(err), "overflow file removed")
	})

	t.Run("produces statistics with distribution", func(t *testing.T) {
		// Prepare
		fhm, _, err := NewFileHashMap(testHashMap, 1000, 16, 10, nil)
		assert.NoError(t, err, "create new file hash map struct")

		keys := make([][]byte, 1001)
		values := make([][]byte, 1001)

		for i := 0; i < 1001; i++ {
			keys[i] = make([]byte, 16)
			rand.Read(keys[i])
			values[i] = make([]byte, 10)
			rand.Read(values[i])

			err = fhm.Set(keys[i], values[i])
			assert.NoErrorf(t, err, "sets record #%d to file", i)
		}

		sp := fhm.fileManagement.GetStorageParameters()

		// Execute
		stat, err := fhm.Stat(true)

		// Check
		assert.NoError(t, err, "gets statistics")
		assert.Equal(t, int64(1001), stat.Records, "correct number of record reported")
		assert.NotZero(t, stat.MapFileRecords, "map file is used")
		assert.NotZero(t, stat.OverflowRecords, "overflow file is used")
		assert.Equal(t, int(sp.NumberOfBuckets), len(stat.BucketDistribution), "bucket distribution has correct length")
		var dRecords int64
		for _, v := range stat.BucketDistribution {
			dRecords += v
		}
		assert.Equal(t, int64(1001), dRecords, "correct number of records reported in distribution")

		// Clean up
		err = fhm.RemoveFiles()
		assert.NoError(t, err, "removes files")

		_, err = os.Stat(fmt.Sprintf("%s-map.bin", testHashMap))
		assert.True(t, os.IsNotExist(err), "map file removed")
		_, err = os.Stat(fmt.Sprintf("%s-ovfl.bin", testHashMap))
		assert.True(t, os.IsNotExist(err), "overflow file removed")
	})
}
