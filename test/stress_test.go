//go:build stress

package test

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/gostonefire/filehashmap"
	"github.com/gostonefire/filehashmap/internal/utils"
	"github.com/stretchr/testify/assert"
	"io"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"testing"
)

func bytesToStrings(d []byte) []string {
	r := make([]string, len(d))
	for i, v := range d {
		r[i] = strconv.Itoa(int(v))
	}
	return r
}

func stringsToBytes(d []string) ([]byte, error) {
	r := make([]byte, len(d))
	for i, v := range d {
		b, err := strconv.Atoi(v)
		if err != nil {
			return nil, err
		}
		r[i] = uint8(b)
	}
	return r, nil
}

func createAndStoreTestdata(amount int, fileName string) error {
	data := make([]byte, 30)

	f, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer func(f *os.File) { _ = f.Close() }(f)

	for i := 0; i < amount; i++ {
		rand.Read(data)
		line := strings.Join(bytesToStrings(data), ",")
		_, err = fmt.Fprintln(f, line)
		if err != nil {
			return err
		}
	}

	return nil
}

func setTestdata(fileName string, fhm *filehashmap.FileHashMap) error {
	f, err := os.OpenFile(fileName, os.O_RDONLY, 0644)
	if err != nil {
		return err
	}
	defer func(f *os.File) { _ = f.Close() }(f)

	var line string
	fr := bufio.NewReader(f)

	for {
		line, err = fr.ReadString('\n')
		if errors.Is(err, io.EOF) {
			break
		}
		line = strings.TrimRight(line, "\n\r")
		data, err := stringsToBytes(strings.Split(line, ","))
		if err != nil {
			return err
		}
		err = fhm.Set(data[:20], data[20:])
		if err != nil {
			return err
		}
	}

	return nil
}

func popTestdata(fileName string, fhm *filehashmap.FileHashMap) error {
	f, err := os.OpenFile(fileName, os.O_RDONLY, 0644)
	if err != nil {
		return err
	}
	defer func(f *os.File) { _ = f.Close() }(f)

	var line string
	var value []byte
	fr := bufio.NewReader(f)

	for {
		line, err = fr.ReadString('\n')
		if errors.Is(err, io.EOF) {
			break
		}
		line = strings.TrimRight(line, "\n\r")
		data, err := stringsToBytes(strings.Split(line, ","))
		if err != nil {
			return err
		}
		value, err = fhm.Pop(data[:20])
		if err != nil {
			return err
		}
		if !utils.IsEqual(value, data[20:]) {
			return fmt.Errorf("popped wrong value")
		}
	}

	return nil
}

func getTestdata(fileName string, fhm *filehashmap.FileHashMap, shouldNotExist bool) error {
	f, err := os.OpenFile(fileName, os.O_RDONLY, 0644)
	if err != nil {
		return err
	}
	defer func(f *os.File) { _ = f.Close() }(f)

	var line string
	var value []byte
	fr := bufio.NewReader(f)

	for {
		line, err = fr.ReadString('\n')
		if errors.Is(err, io.EOF) {
			break
		}
		line = strings.TrimRight(line, "\n\r")
		data, err := stringsToBytes(strings.Split(line, ","))
		if err != nil {
			return err
		}
		value, err = fhm.Get(data[:20])
		if shouldNotExist {
			if err == nil {
				return fmt.Errorf("get should not get data")
			} else if !errors.Is(err, filehashmap.NoRecordFound{}) {
				return err
			}
		} else {
			if err != nil {
				return err
			}
			if !utils.IsEqual(value, data[20:]) {
				return fmt.Errorf("popped wrong value")
			}
		}
	}

	return nil
}

func TestStress(t *testing.T) {
	t.Run("handles lots of stress and reorgs to better", func(t *testing.T) {
		// Prepare test data
		rand.Seed(123)
		err := createAndStoreTestdata(1000000, "testdata_1.txt")
		assert.NoError(t, err, "create testdata 1")
		err = createAndStoreTestdata(1000000, "testdata_2.txt")
		assert.NoError(t, err, "create testdata 2")
		err = createAndStoreTestdata(1000000, "testdata_3.txt")
		assert.NoError(t, err, "create testdata 3")

		// Prepare file hash map
		fhm, _, err := filehashmap.NewFileHashMap("test", 1000000, 20, 10, nil)
		assert.NoError(t, err, "create file hash map")
		err = fhm.CreateNewFiles()
		assert.NoError(t, err, "create hash map files")

		// Set first two sets of test data
		err = setTestdata("testdata_1.txt", fhm)
		assert.NoError(t, err, "set test set 1")
		err = setTestdata("testdata_2.txt", fhm)
		assert.NoError(t, err, "set test set 2")

		// Remove first set from hash map files
		err = popTestdata("testdata_1.txt", fhm)
		assert.NoError(t, err, "pop test set 1")

		// Set third set of test data
		err = setTestdata("testdata_3.txt", fhm)
		assert.NoError(t, err, "set test set 3")

		// Check all three test sets
		err = getTestdata("testdata_1.txt", fhm, true)
		assert.NoError(t, err, "get test set 1, should not exist")
		err = getTestdata("testdata_2.txt", fhm, false)
		assert.NoError(t, err, "get test set 2")
		err = getTestdata("testdata_3.txt", fhm, false)
		assert.NoError(t, err, "get test set 3")

		// Remove second set from hash map files
		err = popTestdata("testdata_2.txt", fhm)
		assert.NoError(t, err, "pop test set 2")

		// Check all three test sets
		err = getTestdata("testdata_1.txt", fhm, true)
		assert.NoError(t, err, "get test set 1, should not exist")
		err = getTestdata("testdata_2.txt", fhm, true)
		assert.NoError(t, err, "get test set 2, should not exist")
		err = getTestdata("testdata_3.txt", fhm, false)
		assert.NoError(t, err, "get test set 3")

		// Get stats
		stat1, err := fhm.Stat(false)
		assert.NoError(t, err, "get stat 1")

		assert.Equal(t, int64(1000000), stat1.Records, "correct number of records, pre-reorg")
		assert.Equal(t, int64(596691), stat1.MapFileRecords, "correct number of map file records, pre-reorg")
		assert.Equal(t, int64(403309), stat1.OverflowRecords, "correct number of overflow file records, pre-reorg")

		fhm.CloseFiles()

		// Reorganize files
		reorgConf := filehashmap.ReorgConf{}
		_, _, err = filehashmap.ReorgFiles("test", reorgConf, true)
		assert.NoError(t, err, "reorg files")

		// Open reorganized files
		fhm, _, err = filehashmap.NewFromExistingFiles("test", nil)
		assert.NoError(t, err, "open reorganized files")

		// Get stats
		stat2, err := fhm.Stat(false)
		assert.NoError(t, err, "get stat 2")

		assert.Equal(t, int64(1000000), stat2.Records, "correct number of records, post-reorg")
		assert.Equal(t, int64(743931), stat2.MapFileRecords, "correct number of map file records, post-reorg")
		assert.Equal(t, int64(256069), stat2.OverflowRecords, "correct number of overflow file records, post-reorg")

		// Remove files
		err = fhm.RemoveFiles()
		assert.NoError(t, err, "remove files")

		// Remove original files
		fhm, _, err = filehashmap.NewFromExistingFiles("test-original", nil)
		assert.NoError(t, err, "open original files")

		err = fhm.RemoveFiles()
		assert.NoError(t, err, "remove original files")

		// Remove test sets
		err = os.Remove("testdata_1.txt")
		assert.NoError(t, err, "remove testdata 1")
		err = os.Remove("testdata_2.txt")
		assert.NoError(t, err, "remove testdata 2")
		err = os.Remove("testdata_3.txt")
		assert.NoError(t, err, "remove testdata 3")
	})
}
