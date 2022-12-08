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

		// Clean up
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

		assert.True(t, internalHash)
		assert.Equal(t, header.KeyLength, keyLength)
		assert.Equal(t, header.ValueLength, valueLength)
		assert.Equal(t, header.NumberOfBucketsNeeded, numberOfBucketsNeeded)
		assert.Equal(t, header.NumberOfBucketsAvailable, numberOfBucketsAvailable)
		assert.Equal(t, header.MaxBucketNo, maxBucketNo)
		assert.Equal(t, header.FileSize, fileSize)
		assert.Equal(t, header.CollisionResolutionTechnique, collisionResolutionTechnique)
	})
}
