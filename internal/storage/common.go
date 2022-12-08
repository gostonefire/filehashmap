package storage

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

// MapFileHeaderLength - Length of hash map file header
const MapFileHeaderLength int64 = 1024

// hashAlgorithmOffset - Header offset to whether using internal (1) or external (0) bucket algorithm - 1 byte
const hashAlgorithmOffset int64 = 0

// keyLengthOffset - Header offset to the key length in records used in buckets - 4 bytes
const keyLengthOffset int64 = 1

// valueLengthOffset - Header offset to the value length in records used in buckets - 4 bytes
const valueLengthOffset int64 = 5

// numberOfBucketsNeededOffset - Header offset to number of buckets - 8 bytes
const numberOfBucketsNeededOffset int64 = 9

// numberOfBucketsAvailableOffset - Header offset to number of buckets - 8 bytes
const numberOfBucketsAvailableOffset int64 = 17

// maxBucketNoOffset - Header offset to max (inclusive) bucket number - 8 bytes
const maxBucketNoOffset int64 = 25

// fileSizeOffset - Header offset to the file size (should of course reflect true file size) - 8 bytes
const fileSizeOffset int64 = 33

// collisionResolutionTechniqueOffset - Header offset to which collision resolution technique is used - 1 byte
const collisionResolutionTechniqueOffset int64 = 41

// Header - Represents the hash map file header data
type Header struct {
	InternalHash                 bool
	KeyLength                    int64
	ValueLength                  int64
	NumberOfBucketsNeeded        int64
	NumberOfBucketsAvailable     int64
	MaxBucketNo                  int64
	FileSize                     int64
	CollisionResolutionTechnique int64
}

// GetMapFileName - Return the map file name given the file hash map name
func GetMapFileName(name string) (fileName string) {
	return fmt.Sprintf("%s-map.bin", name)
}

// GetOvflFileName - Return the overflow file name given the file hash map name
func GetOvflFileName(name string) (fileName string) {
	return fmt.Sprintf("%s-ovfl.bin", name)
}

// GetFileHeader - Reads header data from file and returns it as a Header struct
// This function opens the file for reading, thus expecting it to not already be open.
func GetFileHeader(fileName string) (header Header, err error) {
	file, err := os.OpenFile(fileName, os.O_RDONLY, 0644)
	if err != nil {
		return
	}
	defer func(file *os.File) { _ = file.Close() }(file)

	_, err = file.Seek(0, io.SeekStart)
	if err != nil {
		return
	}

	buf := make([]byte, MapFileHeaderLength)
	_, err = file.Read(buf)
	if err != nil {
		return
	}

	header = bytesToHeader(buf)

	return
}

// GetHeader - Reads header data from file and returns it as a Header struct
func GetHeader(file *os.File) (header Header, err error) {
	_, err = file.Seek(0, io.SeekStart)
	if err != nil {
		return
	}

	buf := make([]byte, MapFileHeaderLength)
	_, err = file.Read(buf)
	if err != nil {
		return
	}

	header = bytesToHeader(buf)

	return
}

// SetHeader - Takes a Header struct and writes header data to file
func SetHeader(file *os.File, header Header) (err error) {
	_, err = file.Seek(0, io.SeekStart)
	if err != nil {
		return
	}

	buf := headerToBytes(header)

	_, err = file.Write(buf)

	return
}

// bytesToHeader - Converts a slice of bytes to a Header struct
func bytesToHeader(buf []byte) (header Header) {
	header = Header{
		InternalHash:                 buf[hashAlgorithmOffset] == 1,
		KeyLength:                    int64(binary.LittleEndian.Uint32(buf[keyLengthOffset:])),
		ValueLength:                  int64(binary.LittleEndian.Uint32(buf[valueLengthOffset:])),
		NumberOfBucketsNeeded:        int64(binary.LittleEndian.Uint64(buf[numberOfBucketsNeededOffset:])),
		NumberOfBucketsAvailable:     int64(binary.LittleEndian.Uint64(buf[numberOfBucketsAvailableOffset:])),
		MaxBucketNo:                  int64(binary.LittleEndian.Uint64(buf[maxBucketNoOffset:])),
		FileSize:                     int64(binary.LittleEndian.Uint64(buf[fileSizeOffset:])),
		CollisionResolutionTechnique: int64(buf[collisionResolutionTechniqueOffset]),
	}

	return
}

// headerToBytes - Converts a Header struct to a slice of bytes
func headerToBytes(header Header) (buf []byte) {
	// Create byte buffer
	buf = make([]byte, MapFileHeaderLength)

	if header.InternalHash {
		buf[hashAlgorithmOffset] = 1
	}

	binary.LittleEndian.PutUint32(buf[keyLengthOffset:], uint32(header.KeyLength))
	binary.LittleEndian.PutUint32(buf[valueLengthOffset:], uint32(header.ValueLength))
	binary.LittleEndian.PutUint64(buf[numberOfBucketsNeededOffset:], uint64(header.NumberOfBucketsNeeded))
	binary.LittleEndian.PutUint64(buf[numberOfBucketsAvailableOffset:], uint64(header.NumberOfBucketsAvailable))
	binary.LittleEndian.PutUint64(buf[maxBucketNoOffset:], uint64(header.MaxBucketNo))
	binary.LittleEndian.PutUint64(buf[fileSizeOffset:], uint64(header.FileSize))
	buf[collisionResolutionTechniqueOffset] = uint8(header.CollisionResolutionTechnique)

	return
}
