//go:build integration

package test

import (
	"fmt"
	"github.com/gostonefire/filehashmap"
	"github.com/gostonefire/filehashmap/crt"
	"github.com/gostonefire/filehashmap/internal/model"
	"github.com/gostonefire/filehashmap/internal/storage"
	"github.com/gostonefire/filehashmap/internal/storage/lpres"
	"github.com/gostonefire/filehashmap/internal/storage/qpres"
	"github.com/gostonefire/filehashmap/internal/storage/scres"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"os"
	"testing"
)

type TestCaseCommon struct {
	crtName            string
	buckets            int
	bucketHeaderLength int
	keyLength          int
	valueLength        int
	crt                int
}

func TestGetFileUtilization(t *testing.T) {
	t.Run("utilization tests for all CRTs", func(t *testing.T) {
		// Prepare
		tests := []TestCaseCommon{
			{crtName: "OpenChaining", buckets: 1000, bucketHeaderLength: 8, keyLength: 16, valueLength: 10, crt: crt.OpenChaining},
			{crtName: "LinearProbing", buckets: 1000, bucketHeaderLength: 0, keyLength: 16, valueLength: 10, crt: crt.LinearProbing},
			{crtName: "QuadraticProbing", buckets: 1000, bucketHeaderLength: 0, keyLength: 16, valueLength: 10, crt: crt.QuadraticProbing},
		}

		for _, test := range tests {
			t.Run(fmt.Sprintf("gets utilization information for %s", test.crtName), func(t *testing.T) {
				// Prepare
				crtConf := model.CRTConf{
					Name:                  "test",
					NumberOfBucketsNeeded: int64(test.buckets),
					KeyLength:             int64(test.keyLength),
					ValueLength:           int64(test.valueLength),
					HashAlgorithm:         nil,
				}

				var fhm filehashmap.FileManagement
				var err error
				switch test.crt {
				case crt.OpenChaining:
					fhm, err = scres.NewSCFiles(crtConf)
				case crt.LinearProbing:
					fhm, err = lpres.NewLPFiles(crtConf)
				case crt.QuadraticProbing:
					fhm, err = qpres.NewQPFiles(crtConf)
				default:
					err = fmt.Errorf("crt not yet implemented")
				}
				assert.NoError(t, err, "creates crt file(s)")

				keys := make([][]byte, 1000)
				values := make([][]byte, 1000)

				for i := 0; i < 1000; i++ {
					keys[i] = make([]byte, 16)
					rand.Read(keys[i])
					values[i] = make([]byte, 10)
					rand.Read(values[i])

					err = fhm.Set(model.Record{Key: keys[i], Value: values[i]})
					assert.NoErrorf(t, err, "sets record #%d to file", i)
				}

				for i := 0; i < 400; i++ {
					record, err := fhm.Get(model.Record{Key: keys[i]})
					assert.NoErrorf(t, err, "gets record #%d to file", i)

					err = fhm.Delete(record)
					assert.NoErrorf(t, err, "deletes record #%d to file", i)
				}

				fhm.CloseFiles()

				file, err := os.OpenFile("test-map.bin", os.O_RDONLY, 0644)
				assert.NoError(t, err, "opens file")

				headerInit, err := storage.GetHeader(file)
				assert.NoError(t, err, "gets a header")

				// Execute
				header, err := storage.GetFileUtilization(file, int64(test.bucketHeaderLength), headerInit)

				// Check
				assert.NoError(t, err, "gets file utilization information")
				if test.crt == crt.OpenChaining {
					assert.NotZero(t, header.NumberOfEmptyRecords, "has empty records")
					assert.Equal(t, headerInit.NumberOfEmptyRecords, header.NumberOfEmptyRecords, "same empty records between header and counted")
					assert.NotZero(t, header.NumberOfOccupiedRecords, "has occupied records")
					assert.Equal(t, headerInit.NumberOfOccupiedRecords, header.NumberOfOccupiedRecords, "same occupied records between header and counted")
					assert.NotZero(t, header.NumberOfDeletedRecords, "has deleted records")
					assert.Equal(t, headerInit.NumberOfDeletedRecords, header.NumberOfDeletedRecords, "same deleted records between header and counted")
				} else {
					assert.Equal(t, headerInit.NumberOfBucketsAvailable-1000, header.NumberOfEmptyRecords, "correct number of empty records")
					assert.Equal(t, headerInit.NumberOfEmptyRecords, header.NumberOfEmptyRecords, "same empty records between header and counted")
					assert.Equal(t, int64(600), header.NumberOfOccupiedRecords, "correct number of occupied records")
					assert.Equal(t, headerInit.NumberOfOccupiedRecords, header.NumberOfOccupiedRecords, "same occupied records between header and counted")
					assert.Equal(t, int64(400), header.NumberOfDeletedRecords, "correct number of deleted records")
					assert.Equal(t, headerInit.NumberOfDeletedRecords, header.NumberOfDeletedRecords, "same deleted records between header and counted")
				}

				// Clean up
				err = file.Close()
				assert.NoError(t, err, "closes file")

				err = fhm.RemoveFiles()
				assert.NoError(t, err, "removes file(s)")
			})
		}
	})
}
