//go:build unit

package storage

import (
	"encoding/binary"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBytesToHeader(t *testing.T) {
	t.Run("converts between bytes and Header struct", func(t *testing.T) {
		// Prepare
		buf := make([]byte, MapFileHeaderLength)
		buf[hashAlgorithmOffset] = 1
		binary.LittleEndian.PutUint32(buf[keyLengthOffset:], 16)
		binary.LittleEndian.PutUint32(buf[valueLengthOffset:], 10)
		binary.LittleEndian.PutUint64(buf[numberOfBucketsNeededOffset:], 400)
		binary.LittleEndian.PutUint64(buf[numberOfBucketsAvailableOffset:], 500)
		binary.LittleEndian.PutUint64(buf[minBucketNoOffset:], 0)
		binary.LittleEndian.PutUint64(buf[maxBucketNoOffset:], 499)
		binary.LittleEndian.PutUint64(buf[fileSizeOffset:], 100000)

		// execute
		header := bytesToHeader(buf)

		// Check
		assert.True(t, header.InternalHash)
		assert.Equal(t, int64(16), header.KeyLength)
		assert.Equal(t, int64(10), header.ValueLength)
		assert.Equal(t, int64(400), header.NumberOfBucketsNeeded)
		assert.Equal(t, int64(500), header.NumberOfBucketsAvailable)
		assert.Equal(t, int64(0), header.MinBucketNo)
		assert.Equal(t, int64(499), header.MaxBucketNo)
		assert.Equal(t, int64(100000), header.FileSize)
	})
}

func TestHeaderToBytes(t *testing.T) {
	t.Run("converts between bytes and Header struct", func(t *testing.T) {
		// Prepare
		header := Header{
			InternalHash:             true,
			KeyLength:                16,
			NumberOfBucketsNeeded:    400,
			NumberOfBucketsAvailable: 500,
			MinBucketNo:              0,
			MaxBucketNo:              499,
			FileSize:                 100000,
		}

		// Execute
		buf := headerToBytes(header)

		// Check
		internalHash := buf[hashAlgorithmOffset] == 1
		keyLength := int64(binary.LittleEndian.Uint32(buf[keyLengthOffset:]))
		valueLength := int64(binary.LittleEndian.Uint32(buf[valueLengthOffset:]))
		numberOfBucketsNeeded := int64(binary.LittleEndian.Uint64(buf[numberOfBucketsNeededOffset:]))
		numberOfBucketsAvailable := int64(binary.LittleEndian.Uint64(buf[numberOfBucketsAvailableOffset:]))
		minBucketNo := int64(binary.LittleEndian.Uint64(buf[minBucketNoOffset:]))
		maxBucketNo := int64(binary.LittleEndian.Uint64(buf[maxBucketNoOffset:]))
		fileSize := int64(binary.LittleEndian.Uint64(buf[fileSizeOffset:]))

		assert.True(t, internalHash)
		assert.Equal(t, header.KeyLength, keyLength)
		assert.Equal(t, header.ValueLength, valueLength)
		assert.Equal(t, header.NumberOfBucketsNeeded, numberOfBucketsNeeded)
		assert.Equal(t, header.NumberOfBucketsAvailable, numberOfBucketsAvailable)
		assert.Equal(t, header.MinBucketNo, minBucketNo)
		assert.Equal(t, header.MaxBucketNo, maxBucketNo)
		assert.Equal(t, header.FileSize, fileSize)
	})
}
