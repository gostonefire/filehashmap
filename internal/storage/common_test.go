//go:build unit

package storage

import (
	"encoding/binary"
	"github.com/gostonefire/filehashmap/crt"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestSetHeader(t *testing.T) {
	t.Run("sets a header in file", func(t *testing.T) {
		// Prepare
		file, err := os.OpenFile("testfile", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
		assert.NoError(t, err, "creates a file")

		err = file.Truncate(MapFileHeaderLength)
		assert.NoError(t, err, "sets file to header size")

		header := Header{
			InternalHash:                 true,
			KeyLength:                    16,
			ValueLength:                  10,
			NumberOfBucketsNeeded:        400,
			NumberOfBucketsAvailable:     500,
			MaxBucketNo:                  499,
			FileSize:                     100000,
			CollisionResolutionTechnique: int64(crt.QuadraticProbing),
			NumberOfEmptyRecords:         300,
			NumberOfOccupiedRecords:      150,
			NumberOfDeletedRecords:       50,
		}

		// Execute
		err = SetHeader(file, header)

		// Check
		assert.NoError(t, err, "sets header")

		// Clean up
		err = file.Close()
		assert.NoError(t, err, "closes file")

		err = os.Remove("testfile")
		assert.NoError(t, err, "removes file")
	})
}

func TestGetHeader(t *testing.T) {
	t.Run("gets a header from file", func(t *testing.T) {
		// Prepare
		file, err := os.OpenFile("testfile", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
		assert.NoError(t, err, "creates a file")

		err = file.Truncate(MapFileHeaderLength)
		assert.NoError(t, err, "sets file to header size")

		headerInit := Header{
			InternalHash:                 true,
			KeyLength:                    16,
			ValueLength:                  10,
			NumberOfBucketsNeeded:        400,
			NumberOfBucketsAvailable:     500,
			MaxBucketNo:                  499,
			FileSize:                     100000,
			CollisionResolutionTechnique: int64(crt.QuadraticProbing),
			NumberOfEmptyRecords:         300,
			NumberOfOccupiedRecords:      150,
			NumberOfDeletedRecords:       50,
		}

		err = SetHeader(file, headerInit)
		assert.NoError(t, err, "sets header")

		// Execute
		header, err := GetHeader(file)

		// Check
		assert.NoError(t, err, "gets header")

		assert.Equal(t, headerInit.InternalHash, header.InternalHash)
		assert.Equal(t, headerInit.KeyLength, header.KeyLength)
		assert.Equal(t, headerInit.ValueLength, header.ValueLength)
		assert.Equal(t, headerInit.NumberOfBucketsNeeded, header.NumberOfBucketsNeeded)
		assert.Equal(t, headerInit.NumberOfBucketsAvailable, header.NumberOfBucketsAvailable)
		assert.Equal(t, headerInit.MaxBucketNo, header.MaxBucketNo)
		assert.Equal(t, headerInit.FileSize, header.FileSize)
		assert.Equal(t, headerInit.CollisionResolutionTechnique, header.CollisionResolutionTechnique)
		assert.Equal(t, headerInit.NumberOfEmptyRecords, header.NumberOfEmptyRecords)
		assert.Equal(t, headerInit.NumberOfOccupiedRecords, header.NumberOfOccupiedRecords)
		assert.Equal(t, headerInit.NumberOfDeletedRecords, header.NumberOfDeletedRecords)
		assert.Zero(t, header.FileCloseDate)

		// Clean up
		err = file.Close()
		assert.NoError(t, err, "closes file")

		err = os.Remove("testfile")
		assert.NoError(t, err, "removes file")
	})
}

func TestGetFileHeader(t *testing.T) {
	t.Run("gets a header from file by filename", func(t *testing.T) {
		// Prepare
		file, err := os.OpenFile("testfile", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
		assert.NoError(t, err, "creates a file")

		err = file.Truncate(MapFileHeaderLength)
		assert.NoError(t, err, "sets file to header size")

		headerInit := Header{
			InternalHash:                 true,
			KeyLength:                    16,
			ValueLength:                  10,
			NumberOfBucketsNeeded:        400,
			NumberOfBucketsAvailable:     500,
			MaxBucketNo:                  499,
			FileSize:                     100000,
			CollisionResolutionTechnique: int64(crt.QuadraticProbing),
			NumberOfEmptyRecords:         300,
			NumberOfOccupiedRecords:      150,
			NumberOfDeletedRecords:       50,
		}

		err = SetHeader(file, headerInit)
		assert.NoError(t, err, "sets header")

		err = file.Close()
		assert.NoError(t, err, "closes file")

		// Execute
		header, err := GetFileHeader("testfile")

		// Check
		assert.NoError(t, err, "gets header")

		assert.Equal(t, headerInit.InternalHash, header.InternalHash)
		assert.Equal(t, headerInit.KeyLength, header.KeyLength)
		assert.Equal(t, headerInit.ValueLength, header.ValueLength)
		assert.Equal(t, headerInit.NumberOfBucketsNeeded, header.NumberOfBucketsNeeded)
		assert.Equal(t, headerInit.NumberOfBucketsAvailable, header.NumberOfBucketsAvailable)
		assert.Equal(t, headerInit.MaxBucketNo, header.MaxBucketNo)
		assert.Equal(t, headerInit.FileSize, header.FileSize)
		assert.Equal(t, headerInit.CollisionResolutionTechnique, header.CollisionResolutionTechnique)
		assert.Equal(t, headerInit.NumberOfEmptyRecords, header.NumberOfEmptyRecords)
		assert.Equal(t, headerInit.NumberOfOccupiedRecords, header.NumberOfOccupiedRecords)
		assert.Equal(t, headerInit.NumberOfDeletedRecords, header.NumberOfDeletedRecords)
		assert.Zero(t, header.FileCloseDate)

		// Clean up
		err = os.Remove("testfile")
		assert.NoError(t, err, "removes file")
	})
}

func TestSetFileCloseDate(t *testing.T) {
	t.Run("sets a close date to file header", func(t *testing.T) {
		// Prepare
		file, err := os.OpenFile("testfile", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
		assert.NoError(t, err, "creates a file")

		err = file.Truncate(MapFileHeaderLength)
		assert.NoError(t, err, "sets file to header size")

		headerInit := Header{
			InternalHash:                 true,
			KeyLength:                    16,
			ValueLength:                  10,
			NumberOfBucketsNeeded:        400,
			NumberOfBucketsAvailable:     500,
			MaxBucketNo:                  499,
			FileSize:                     100000,
			CollisionResolutionTechnique: int64(crt.QuadraticProbing),
			NumberOfEmptyRecords:         300,
			NumberOfOccupiedRecords:      150,
			NumberOfDeletedRecords:       50,
		}

		err = SetHeader(file, headerInit)
		assert.NoError(t, err, "sets header")

		// Execute
		err = SetFileCloseDate(file, false)

		// Check
		assert.NoError(t, err, "sets file close date")

		header, err := GetHeader(file)
		assert.NoError(t, err, "gets header")

		assert.NotZero(t, header.FileCloseDate)

		// Clean up
		err = file.Close()
		assert.NoError(t, err, "closes file")

		err = os.Remove("testfile")
		assert.NoError(t, err, "removes file")
	})

	t.Run("nullifies a close date in file header", func(t *testing.T) {
		// Prepare
		file, err := os.OpenFile("testfile", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
		assert.NoError(t, err, "creates a file")

		err = file.Truncate(MapFileHeaderLength)
		assert.NoError(t, err, "sets file to header size")

		headerInit := Header{
			InternalHash:                 true,
			KeyLength:                    16,
			ValueLength:                  10,
			NumberOfBucketsNeeded:        400,
			NumberOfBucketsAvailable:     500,
			MaxBucketNo:                  499,
			FileSize:                     100000,
			CollisionResolutionTechnique: int64(crt.QuadraticProbing),
			NumberOfEmptyRecords:         300,
			NumberOfOccupiedRecords:      150,
			NumberOfDeletedRecords:       50,
		}

		err = SetHeader(file, headerInit)
		assert.NoError(t, err, "sets header")

		err = SetFileCloseDate(file, false)
		assert.NoError(t, err, "sets file close date")

		// Execute
		err = SetFileCloseDate(file, true)

		// Check
		assert.NoError(t, err, "nullifies file close date")

		header, err := GetHeader(file)
		assert.NoError(t, err, "gets header")

		assert.Zero(t, header.FileCloseDate)

		// Clean up
		err = file.Close()
		assert.NoError(t, err, "closes file")

		err = os.Remove("testfile")
		assert.NoError(t, err, "removes file")
	})
}

func TestBytesToHeader(t *testing.T) {
	t.Run("converts between bytes and Header struct", func(t *testing.T) {
		// Prepare
		buf := make([]byte, MapFileHeaderLength)
		buf[hashAlgorithmOffset] = 1
		binary.LittleEndian.PutUint32(buf[keyLengthOffset:], 16)
		binary.LittleEndian.PutUint32(buf[valueLengthOffset:], 10)
		binary.LittleEndian.PutUint64(buf[numberOfBucketsNeededOffset:], 400)
		binary.LittleEndian.PutUint64(buf[numberOfBucketsAvailableOffset:], 500)
		binary.LittleEndian.PutUint64(buf[maxBucketNoOffset:], 499)
		binary.LittleEndian.PutUint64(buf[fileSizeOffset:], 100000)
		buf[collisionResolutionTechniqueOffset] = uint8(crt.LinearProbing)
		binary.LittleEndian.PutUint64(buf[numberOfEmptyRecordsOffset:], 300)
		binary.LittleEndian.PutUint64(buf[numberOfOccupiedRecordsOffset:], 150)
		binary.LittleEndian.PutUint64(buf[numberOfDeletedRecordsOffset:], 50)

		// execute
		header := bytesToHeader(buf)

		// Check
		assert.True(t, header.InternalHash)
		assert.Equal(t, int64(16), header.KeyLength)
		assert.Equal(t, int64(10), header.ValueLength)
		assert.Equal(t, int64(400), header.NumberOfBucketsNeeded)
		assert.Equal(t, int64(500), header.NumberOfBucketsAvailable)
		assert.Equal(t, int64(499), header.MaxBucketNo)
		assert.Equal(t, int64(100000), header.FileSize)
		assert.Equal(t, int64(crt.LinearProbing), header.CollisionResolutionTechnique)
		assert.Equal(t, int64(300), header.NumberOfEmptyRecords)
		assert.Equal(t, int64(150), header.NumberOfOccupiedRecords)
		assert.Equal(t, int64(50), header.NumberOfDeletedRecords)
	})
}

func TestHeaderToBytes(t *testing.T) {
	t.Run("converts between bytes and Header struct", func(t *testing.T) {
		// Prepare
		header := Header{
			InternalHash:                 true,
			KeyLength:                    16,
			ValueLength:                  10,
			NumberOfBucketsNeeded:        400,
			NumberOfBucketsAvailable:     500,
			MaxBucketNo:                  499,
			FileSize:                     100000,
			CollisionResolutionTechnique: int64(crt.QuadraticProbing),
			NumberOfEmptyRecords:         300,
			NumberOfOccupiedRecords:      150,
			NumberOfDeletedRecords:       50,
		}

		// Execute
		buf := headerToBytes(header)

		// Check
		internalHash := buf[hashAlgorithmOffset] == 1
		keyLength := int64(binary.LittleEndian.Uint32(buf[keyLengthOffset:]))
		valueLength := int64(binary.LittleEndian.Uint32(buf[valueLengthOffset:]))
		numberOfBucketsNeeded := int64(binary.LittleEndian.Uint64(buf[numberOfBucketsNeededOffset:]))
		numberOfBucketsAvailable := int64(binary.LittleEndian.Uint64(buf[numberOfBucketsAvailableOffset:]))
		maxBucketNo := int64(binary.LittleEndian.Uint64(buf[maxBucketNoOffset:]))
		fileSize := int64(binary.LittleEndian.Uint64(buf[fileSizeOffset:]))
		collisionResolutionTechnique := int64(buf[collisionResolutionTechniqueOffset])
		nEmpty := int64(binary.LittleEndian.Uint64(buf[numberOfEmptyRecordsOffset:]))
		nOccupied := int64(binary.LittleEndian.Uint64(buf[numberOfOccupiedRecordsOffset:]))
		nDeleted := int64(binary.LittleEndian.Uint64(buf[numberOfDeletedRecordsOffset:]))

		assert.True(t, internalHash)
		assert.Equal(t, header.KeyLength, keyLength)
		assert.Equal(t, header.ValueLength, valueLength)
		assert.Equal(t, header.NumberOfBucketsNeeded, numberOfBucketsNeeded)
		assert.Equal(t, header.NumberOfBucketsAvailable, numberOfBucketsAvailable)
		assert.Equal(t, header.MaxBucketNo, maxBucketNo)
		assert.Equal(t, header.FileSize, fileSize)
		assert.Equal(t, header.CollisionResolutionTechnique, collisionResolutionTechnique)
		assert.Equal(t, header.NumberOfEmptyRecords, nEmpty)
		assert.Equal(t, header.NumberOfOccupiedRecords, nOccupied)
		assert.Equal(t, header.NumberOfDeletedRecords, nDeleted)
	})
}
