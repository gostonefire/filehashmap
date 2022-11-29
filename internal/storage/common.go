package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/gostonefire/filehashmap/internal/model"
	"io"
	"os"
	"time"
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

// numberOfEmptyRecordsOffset - Header offset to number of empty records - 8 bytes
const numberOfEmptyRecordsOffset int64 = 42

// numberOfOccupiedRecordsOffset - Header offset to number of occupied records - 8 bytes
const numberOfOccupiedRecordsOffset int64 = 50

// numberOfDeletedRecordsOffset - Header offset to number of deleted records - 8 bytes
const numberOfDeletedRecordsOffset int64 = 58

// fileCloseDateOffset - Header offset to unix datetime when file was closed - 8 bytes
const fileCloseDateOffset int64 = 68

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
	NumberOfEmptyRecords         int64
	NumberOfOccupiedRecords      int64
	NumberOfDeletedRecords       int64
	FileCloseDate                int64
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

// SetFileCloseDate - Sets the date time in unix format when the file is closed.
// It is supposed to be called just before closing a file or at time of opening with nullify = true
func SetFileCloseDate(file *os.File, nullify bool) (err error) {
	_, err = file.Seek(fileCloseDateOffset, io.SeekStart)
	if err != nil {
		return
	}

	buf := make([]byte, 8)

	if !nullify {
		unixDate := time.Now().Unix()
		binary.LittleEndian.PutUint64(buf, uint64(unixDate))
	}

	_, err = file.Write(buf)

	return
}

// GetFileUtilization - Walks through the map file and updates header with number of empty, occupied and deleted records
func GetFileUtilization(file *os.File, bucketHeaderLength int64, header Header) (updatedHeader Header, err error) {
	_, err = file.Seek(MapFileHeaderLength, io.SeekStart)
	if err != nil {
		return
	}

	headerLength := int(bucketHeaderLength)
	recordLength := int(1 + header.KeyLength + header.ValueLength) // One byte for state and rest is data
	bucketLength := headerLength + recordLength
	nBuckets := int(header.FileSize-MapFileHeaderLength) / bucketLength
	chunk := (1048576 / bucketLength) * bucketLength

	data := make([]byte, chunk)
	var n int
	var nEmpty, nOccupied, nDeleted int64
	for {
		// Read until we get an EOF with no data returned
		n, err = file.Read(data)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				return
			} else if n == 0 {
				break
			}
		}

		// Check so we have complete buckets in read data
		if n%bucketLength != 0 {
			err = fmt.Errorf("read incomplete bucket from file")
			return
		}

		// Loop through all records in retrieved chunk and update utilization entities
		for i := 0; i < n/bucketLength; i++ {
			switch data[i*bucketLength+headerLength] {
			case model.RecordEmpty:
				nEmpty++
			case model.RecordOccupied:
				nOccupied++
			case model.RecordDeleted:
				nDeleted++
			}
		}
	}

	// Check if utilization recorded is in line with expected total number of buckets in file
	if nEmpty+nOccupied+nDeleted != int64(nBuckets) {
		err = fmt.Errorf("got wrong number of buckets from file")
		return
	}

	err = nil
	updatedHeader = header
	updatedHeader.NumberOfEmptyRecords = nEmpty
	updatedHeader.NumberOfOccupiedRecords = nOccupied
	updatedHeader.NumberOfDeletedRecords = nDeleted

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
		NumberOfEmptyRecords:         int64(binary.LittleEndian.Uint64(buf[numberOfEmptyRecordsOffset:])),
		NumberOfOccupiedRecords:      int64(binary.LittleEndian.Uint64(buf[numberOfOccupiedRecordsOffset:])),
		NumberOfDeletedRecords:       int64(binary.LittleEndian.Uint64(buf[numberOfDeletedRecordsOffset:])),
		FileCloseDate:                int64(binary.LittleEndian.Uint64(buf[fileCloseDateOffset:])),
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
	binary.LittleEndian.PutUint64(buf[numberOfEmptyRecordsOffset:], uint64(header.NumberOfEmptyRecords))
	binary.LittleEndian.PutUint64(buf[numberOfOccupiedRecordsOffset:], uint64(header.NumberOfOccupiedRecords))
	binary.LittleEndian.PutUint64(buf[numberOfDeletedRecordsOffset:], uint64(header.NumberOfDeletedRecords))

	// Note! FileCloseDate is intentionally left out when writing header to file (i.e. it is nullified).
	// It is supposed to be set exclusively to prove when later open the file that it was closed in a proper way.

	return
}
