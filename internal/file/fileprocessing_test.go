//go:build unit

package file

import (
	"github.com/gostonefire/filehashmap/internal/conf"
	"github.com/gostonefire/filehashmap/internal/model"
	"github.com/gostonefire/filehashmap/internal/utils"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestNewSCFiles(t *testing.T) {
	t.Run("creates a new SCFiles instance", func(t *testing.T) {
		// Prepare
		scConf := SCFilesConf{
			MapFileName:      "test-map.bin",
			OvflFileName:     "test-ovfl.bin",
			KeyLength:        16,
			ValueLength:      10,
			RecordsPerBucket: 2,
			FileSize:         2048,
		}

		header := model.Header{
			InternalAlg:       false,
			InitialUniqueKeys: 10,
			KeyLength:         16,
			ValueLength:       10,
			RecordsPerBucket:  2,
			NumberOfBuckets:   5,
			MinBucketNo:       0,
			MaxBucketNo:       4,
			FileSize:          2048,
		}

		// Execute
		scFiles, err := NewSCFiles(scConf, header)

		// Check
		assert.NoError(t, err, "create new SCFiles instance")
		assert.Equal(t, scConf.MapFileName, scFiles.mapFileName, "map filename preserved")
		assert.Equal(t, scConf.OvflFileName, scFiles.ovflFileName, "overflow filename preserved")
		assert.Equal(t, scConf.KeyLength, scFiles.keyLength, "key length preserved")
		assert.Equal(t, scConf.ValueLength, scFiles.valueLength, "value length preserved")
		assert.Equal(t, scConf.RecordsPerBucket, scFiles.recordsPerBucket, "records per bucket preserved")

		stat, err := os.Stat(scConf.MapFileName)
		assert.NoError(t, err, "map file exists")
		assert.Equal(t, scConf.FileSize, stat.Size(), "map file in correct size")
		_, err = os.Stat(scConf.OvflFileName)
		assert.NoError(t, err, "overflow file exists")

		scFiles.CloseFiles()
		err = scFiles.RemoveFiles()
		assert.NoError(t, err, "removes files")

		_, err = os.Stat(scConf.MapFileName)
		assert.True(t, os.IsNotExist(err), "map file removed")
		_, err = os.Stat(scConf.OvflFileName)
		assert.True(t, os.IsNotExist(err), "overflow file removed")

		// Clean up
	})
}

func TestNewSCFilesFromExistingFiles(t *testing.T) {
	t.Run("creates a new SCFiles instance", func(t *testing.T) {
		// Prepare
		scConf := SCFilesConf{
			MapFileName:      "test-map.bin",
			OvflFileName:     "test-ovfl.bin",
			KeyLength:        16,
			ValueLength:      10,
			RecordsPerBucket: 2,
			FileSize:         2048,
		}

		header := model.Header{
			InternalAlg:       false,
			InitialUniqueKeys: 10,
			KeyLength:         16,
			ValueLength:       10,
			RecordsPerBucket:  2,
			NumberOfBuckets:   5,
			MinBucketNo:       0,
			MaxBucketNo:       4,
			FileSize:          2048,
		}

		scFiles, err := NewSCFiles(scConf, header)
		assert.NoError(t, err, "create new SCFiles instance")
		scFiles.CloseFiles()

		// Execute
		scFiles2, header2, err := NewSCFilesFromExistingFiles(scConf.MapFileName, scConf.OvflFileName)

		// Check
		assert.NoError(t, err, "opens existing files")
		assert.Equal(t, scFiles.mapFileName, scFiles2.mapFileName, "map file name preserved")
		assert.Equal(t, scFiles.ovflFileName, scFiles2.ovflFileName, "overflow file name preserved")
		assert.Equal(t, scFiles.keyLength, scFiles2.keyLength, "key length preserved")
		assert.Equal(t, scFiles.valueLength, scFiles2.valueLength, "value length preserved")
		assert.Equal(t, scFiles.recordsPerBucket, scFiles2.recordsPerBucket, "records per bucket preserved")

		assert.Equal(t, header.FileSize, header2.FileSize, "header FileSize preserved")
		assert.Equal(t, header.KeyLength, header2.KeyLength, "header KeyLength preserved")
		assert.Equal(t, header.ValueLength, header2.ValueLength, "header ValueLength preserved")
		assert.Equal(t, header.RecordsPerBucket, header2.RecordsPerBucket, "header RecordsPerBucket preserved")
		assert.Equal(t, header.InitialUniqueKeys, header2.InitialUniqueKeys, "header InitialUniqueKeys preserved")
		assert.Equal(t, header.NumberOfBuckets, header2.NumberOfBuckets, "header NumberOfBuckets preserved")
		assert.Equal(t, header.MinBucketNo, header2.MinBucketNo, "header MinBucketNo preserved")
		assert.Equal(t, header.MaxBucketNo, header2.MaxBucketNo, "header MaxBucketNo preserved")
		assert.Equal(t, header.InternalAlg, header2.InternalAlg, "header InternalAlg preserved")

		// Clean up
		scFiles2.CloseFiles()
		err = scFiles2.RemoveFiles()
		assert.NoError(t, err, "removes files")

		_, err = os.Stat(scConf.MapFileName)
		assert.True(t, os.IsNotExist(err), "map file removed")
		_, err = os.Stat(scConf.OvflFileName)
		assert.True(t, os.IsNotExist(err), "overflow file removed")
	})
}

func TestSCFiles_SetBucketRecord(t *testing.T) {
	t.Run("sets a bucket record in file", func(t *testing.T) {
		// Prepare
		scConf := SCFilesConf{
			MapFileName:      "test-map.bin",
			OvflFileName:     "test-ovfl.bin",
			KeyLength:        16,
			ValueLength:      10,
			RecordsPerBucket: 2,
			FileSize:         2048,
		}

		header := model.Header{
			InternalAlg:       false,
			InitialUniqueKeys: 10,
			KeyLength:         16,
			ValueLength:       10,
			RecordsPerBucket:  2,
			NumberOfBuckets:   5,
			MinBucketNo:       0,
			MaxBucketNo:       4,
			FileSize:          2048,
		}

		scFiles, err := NewSCFiles(scConf, header)
		assert.NoError(t, err, "create new SCFiles instance")

		record := model.Record{
			InUse:         true,
			RecordAddress: 1024,
			Key:           []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
			Value:         []byte{16, 17, 18, 19, 20, 21, 22, 23, 24, 25},
		}

		// Execute
		err = scFiles.SetBucketRecord(record)

		// Check
		assert.NoError(t, err, "sets bucket record")

		// Clean up
		scFiles.CloseFiles()
		err = scFiles.RemoveFiles()
		assert.NoError(t, err, "removes files")

		_, err = os.Stat(scConf.MapFileName)
		assert.True(t, os.IsNotExist(err), "map file removed")
		_, err = os.Stat(scConf.OvflFileName)
		assert.True(t, os.IsNotExist(err), "overflow file removed")
	})
}

func TestSCFiles_GetBucketRecords(t *testing.T) {
	t.Run("gets a bucket record from file", func(t *testing.T) {
		// Prepare
		scConf := SCFilesConf{
			MapFileName:      "test-map.bin",
			OvflFileName:     "test-ovfl.bin",
			KeyLength:        16,
			ValueLength:      10,
			RecordsPerBucket: 2,
			FileSize:         2048,
		}

		header := model.Header{
			InternalAlg:       false,
			InitialUniqueKeys: 10,
			KeyLength:         16,
			ValueLength:       10,
			RecordsPerBucket:  2,
			NumberOfBuckets:   5,
			MinBucketNo:       0,
			MaxBucketNo:       4,
			FileSize:          2048,
		}

		scFiles, err := NewSCFiles(scConf, header)
		assert.NoError(t, err, "create new SCFiles instance")

		record := model.Record{
			InUse:         true,
			RecordAddress: 1024 + conf.BucketHeaderLength,
			Key:           []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
			Value:         []byte{16, 17, 18, 19, 20, 21, 22, 23, 24, 25},
		}

		err = scFiles.SetBucketRecord(record)
		assert.NoError(t, err, "sets bucket record")

		// Execute
		bucket, err := scFiles.GetBucketRecords(0)

		// Check
		assert.NoError(t, err, "gets bucket record")
		assert.True(t, bucket.Records[0].InUse, "record 0 in use")
		assert.True(t, utils.IsEqual(record.Key, bucket.Records[0].Key), "record key preserved")
		assert.True(t, utils.IsEqual(record.Value, bucket.Records[0].Value), "record value preserved")
		assert.Equal(t, record.RecordAddress, bucket.Records[0].RecordAddress, "correct records address for record 0")
		assert.False(t, bucket.Records[1].InUse, "record 1 not in use")
		assert.False(t, bucket.HasOverflow, "bucket has no overflow")
		assert.Equal(t, int64(0), bucket.OverflowAddress, "bucket overflow address correct")

		// Clean up
		scFiles.CloseFiles()
		err = scFiles.RemoveFiles()
		assert.NoError(t, err, "removes files")

		_, err = os.Stat(scConf.MapFileName)
		assert.True(t, os.IsNotExist(err), "map file removed")
		_, err = os.Stat(scConf.OvflFileName)
		assert.True(t, os.IsNotExist(err), "overflow file removed")
	})
}

func TestSCFiles_NewBucketOverflow(t *testing.T) {
	t.Run("adds new overflow record to file", func(t *testing.T) {
		// Prepare
		scConf := SCFilesConf{
			MapFileName:      "test-map.bin",
			OvflFileName:     "test-ovfl.bin",
			KeyLength:        16,
			ValueLength:      10,
			RecordsPerBucket: 2,
			FileSize:         2048,
		}

		header := model.Header{
			InternalAlg:       false,
			InitialUniqueKeys: 10,
			KeyLength:         16,
			ValueLength:       10,
			RecordsPerBucket:  2,
			NumberOfBuckets:   5,
			MinBucketNo:       0,
			MaxBucketNo:       4,
			FileSize:          2048,
		}

		scFiles, err := NewSCFiles(scConf, header)
		assert.NoError(t, err, "create new SCFiles instance")

		key := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
		value := []byte{16, 17, 18, 19, 20, 21, 22, 23, 24, 25}

		// Execute
		ovflAddress, err := scFiles.NewBucketOverflow(key, value)

		// Check
		assert.NoError(t, err, "adds new bucket overflow")
		assert.Equal(t, conf.OvflFileHeaderLength, ovflAddress, "correct overflow address")

		// Clean up
		scFiles.CloseFiles()
		err = scFiles.RemoveFiles()
		assert.NoError(t, err, "removes files")

		_, err = os.Stat(scConf.MapFileName)
		assert.True(t, os.IsNotExist(err), "map file removed")
		_, err = os.Stat(scConf.OvflFileName)
		assert.True(t, os.IsNotExist(err), "overflow file removed")
	})
}

func TestSCFiles_SetBucketOverflowAddress(t *testing.T) {
	t.Run("sets overflow address to file", func(t *testing.T) {
		// Prepare
		scConf := SCFilesConf{
			MapFileName:      "test-map.bin",
			OvflFileName:     "test-ovfl.bin",
			KeyLength:        16,
			ValueLength:      10,
			RecordsPerBucket: 2,
			FileSize:         2048,
		}

		header := model.Header{
			InternalAlg:       false,
			InitialUniqueKeys: 10,
			KeyLength:         16,
			ValueLength:       10,
			RecordsPerBucket:  2,
			NumberOfBuckets:   5,
			MinBucketNo:       0,
			MaxBucketNo:       4,
			FileSize:          2048,
		}

		scFiles, err := NewSCFiles(scConf, header)
		assert.NoError(t, err, "create new SCFiles instance")

		// Execute
		err = scFiles.SetBucketOverflowAddress(conf.MapFileHeaderLength, conf.OvflFileHeaderLength)
		assert.NoError(t, err, "sets overflow address in bucket")

		// Clean up
		scFiles.CloseFiles()
		err = scFiles.RemoveFiles()
		assert.NoError(t, err, "removes files")

		_, err = os.Stat(scConf.MapFileName)
		assert.True(t, os.IsNotExist(err), "map file removed")
		_, err = os.Stat(scConf.OvflFileName)
		assert.True(t, os.IsNotExist(err), "overflow file removed")
	})
}

func TestSCFiles_GetOverflowRecord(t *testing.T) {
	t.Run("gets overflow record from file", func(t *testing.T) {
		// Prepare
		scConf := SCFilesConf{
			MapFileName:      "test-map.bin",
			OvflFileName:     "test-ovfl.bin",
			KeyLength:        16,
			ValueLength:      10,
			RecordsPerBucket: 2,
			FileSize:         2048,
		}

		header := model.Header{
			InternalAlg:       false,
			InitialUniqueKeys: 10,
			KeyLength:         16,
			ValueLength:       10,
			RecordsPerBucket:  2,
			NumberOfBuckets:   5,
			MinBucketNo:       0,
			MaxBucketNo:       4,
			FileSize:          2048,
		}

		scFiles, err := NewSCFiles(scConf, header)
		assert.NoError(t, err, "create new SCFiles instance")

		key := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
		value := []byte{16, 17, 18, 19, 20, 21, 22, 23, 24, 25}

		ovflAddress, err := scFiles.NewBucketOverflow(key, value)
		assert.NoError(t, err, "adds new bucket overflow")

		// Execute
		record, err := scFiles.GetOverflowRecord(ovflAddress)

		// Check
		assert.NoError(t, err, "gets overflow record")
		assert.True(t, record.InUse, "record is in use")
		assert.True(t, record.IsOverflow, "record is overflow")
		assert.Equal(t, conf.OvflFileHeaderLength, record.RecordAddress, "record has correct address")
		assert.True(t, utils.IsEqual(key, record.Key), "key is preserved")
		assert.True(t, utils.IsEqual(value, record.Value), "value is preserved")

		// Clean up
		scFiles.CloseFiles()
		err = scFiles.RemoveFiles()
		assert.NoError(t, err, "removes files")

		_, err = os.Stat(scConf.MapFileName)
		assert.True(t, os.IsNotExist(err), "map file removed")
		_, err = os.Stat(scConf.OvflFileName)
		assert.True(t, os.IsNotExist(err), "overflow file removed")
	})
}

func TestSCFiles_SetOverflowRecord(t *testing.T) {
	t.Run("sets overflow record in file", func(t *testing.T) {
		// Prepare
		scConf := SCFilesConf{
			MapFileName:      "test-map.bin",
			OvflFileName:     "test-ovfl.bin",
			KeyLength:        16,
			ValueLength:      10,
			RecordsPerBucket: 2,
			FileSize:         2048,
		}

		header := model.Header{
			InternalAlg:       false,
			InitialUniqueKeys: 10,
			KeyLength:         16,
			ValueLength:       10,
			RecordsPerBucket:  2,
			NumberOfBuckets:   5,
			MinBucketNo:       0,
			MaxBucketNo:       4,
			FileSize:          2048,
		}

		scFiles, err := NewSCFiles(scConf, header)
		assert.NoError(t, err, "create new SCFiles instance")

		key := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
		value := []byte{16, 17, 18, 19, 20, 21, 22, 23, 24, 25}

		ovflAddress, err := scFiles.NewBucketOverflow(key, value)
		assert.NoError(t, err, "adds new bucket overflow")

		record := model.Record{
			InUse:         true,
			IsOverflow:    true,
			RecordAddress: ovflAddress,
			NextOverflow:  0,
			Key:           []byte{15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0},
			Value:         []byte{25, 24, 23, 22, 21, 20, 19, 18, 17, 16},
		}

		// Execute
		err = scFiles.SetOverflowRecord(record)

		// Check
		assert.NoError(t, err, "sets overflow record")

		record2, err := scFiles.GetOverflowRecord(ovflAddress)
		assert.NoError(t, err, "gets overflow record")
		assert.True(t, record2.InUse, "record is in use")
		assert.True(t, record2.IsOverflow, "record is overflow")
		assert.Equal(t, conf.OvflFileHeaderLength, record2.RecordAddress, "record has correct address")
		assert.True(t, utils.IsEqual(record.Key, record2.Key), "key is preserved")
		assert.True(t, utils.IsEqual(record.Value, record2.Value), "value is preserved")

		// Clean up
		scFiles.CloseFiles()
		err = scFiles.RemoveFiles()
		assert.NoError(t, err, "removes files")

		_, err = os.Stat(scConf.MapFileName)
		assert.True(t, os.IsNotExist(err), "map file removed")
		_, err = os.Stat(scConf.OvflFileName)
		assert.True(t, os.IsNotExist(err), "overflow file removed")
	})
}

func TestSCFiles_AppendOverflowRecord(t *testing.T) {
	t.Run("appends overflow record in file", func(t *testing.T) {
		// Prepare
		scConf := SCFilesConf{
			MapFileName:      "test-map.bin",
			OvflFileName:     "test-ovfl.bin",
			KeyLength:        16,
			ValueLength:      10,
			RecordsPerBucket: 2,
			FileSize:         2048,
		}

		header := model.Header{
			InternalAlg:       false,
			InitialUniqueKeys: 10,
			KeyLength:         16,
			ValueLength:       10,
			RecordsPerBucket:  2,
			NumberOfBuckets:   5,
			MinBucketNo:       0,
			MaxBucketNo:       4,
			FileSize:          2048,
		}

		scFiles, err := NewSCFiles(scConf, header)
		assert.NoError(t, err, "create new SCFiles instance")

		key := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
		value := []byte{16, 17, 18, 19, 20, 21, 22, 23, 24, 25}

		ovflAddress, err := scFiles.NewBucketOverflow(key, value)
		assert.NoError(t, err, "adds new bucket overflow")

		record, err := scFiles.GetOverflowRecord(ovflAddress)
		assert.NoError(t, err, "gets overflow record")

		key2 := []byte{15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0}
		value2 := []byte{25, 24, 23, 22, 21, 20, 19, 18, 17, 16}

		// Execute
		err = scFiles.AppendOverflowRecord(record, key2, value2)

		// Check
		assert.NoError(t, err, "appends overflow record")

		record, err = scFiles.GetOverflowRecord(ovflAddress)
		assert.NoError(t, err, "gets overflow record")
		assert.NotEqualf(t, int64(0), record.NextOverflow, "has next overflow address")

		record2, err := scFiles.GetOverflowRecord(record.NextOverflow)
		assert.NoError(t, err, "gets next overflow record")
		assert.Equalf(t, int64(0), record2.NextOverflow, "has no next overflow address")
		assert.True(t, utils.IsEqual(key2, record2.Key), "key is preserved")
		assert.True(t, utils.IsEqual(value2, record2.Value), "value is preserved")

		// Clean up
		scFiles.CloseFiles()
		err = scFiles.RemoveFiles()
		assert.NoError(t, err, "removes files")

		_, err = os.Stat(scConf.MapFileName)
		assert.True(t, os.IsNotExist(err), "map file removed")
		_, err = os.Stat(scConf.OvflFileName)
		assert.True(t, os.IsNotExist(err), "overflow file removed")
	})
}
