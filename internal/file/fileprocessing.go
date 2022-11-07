package file

import (
	"encoding/binary"
	"fmt"
	"github.com/gostonefire/filehashmap/internal/conf"
	"io"
	"os"
)

// GetHeader - Reads header data from file and returns it as a Header struct
func GetHeader(f *os.File) (header Header, err error) {
	_, err = f.Seek(0, io.SeekStart)
	if err != nil {
		return
	}

	buf := make([]byte, conf.MapFileHeaderLength)
	_, err = f.Read(buf)
	if err != nil {
		return
	}

	header = bytesToHeader(buf)

	return
}

// SetHeader - Takes a Header struct and writes header data to file
func SetHeader(f *os.File, header Header) (err error) {
	_, err = f.Seek(0, io.SeekStart)
	if err != nil {
		return
	}

	buf := headerToBytes(header)

	_, err = f.Write(buf)

	return
}

// GetBucketOverflowAddress - Returns the overflow address for a bucket. Zero means the bucket has not yet
// any overflow.
func GetBucketOverflowAddress(
	f *os.File,
	bucketNo,
	trueRecordLength,
	recordsPerBucket int64,
) (
	overflowAddress int64,
	err error,
) {

	bucketAddress := conf.MapFileHeaderLength + bucketNo*(trueRecordLength*recordsPerBucket+conf.BucketHeaderLength)

	_, err = f.Seek(bucketAddress, io.SeekStart)
	if err != nil {
		return
	}

	buf := make([]byte, conf.OverflowAddressLength)
	_, err = f.Read(buf)
	if err != nil {
		return
	}

	overflowAddress = int64(binary.LittleEndian.Uint64(buf))

	return
}

// GetBucketRecords - Returns all records for a given bucket number in a Bucket struct
func GetBucketRecords(f *os.File, bucketNo, keyLength, valueLength, recordsPerBucket int64) (bucket Bucket, err error) {
	trueRecordLength := keyLength + valueLength + conf.InUseFlagBytes
	bucketAddress := conf.MapFileHeaderLength + bucketNo*(trueRecordLength*recordsPerBucket+conf.BucketHeaderLength)

	_, err = f.Seek(bucketAddress, io.SeekStart)
	if err != nil {
		return
	}

	buf := make([]byte, trueRecordLength*recordsPerBucket+conf.BucketHeaderLength)
	_, err = f.Read(buf)
	if err != nil {
		return
	}

	bucket, err = bytesToBucket(buf, bucketAddress, keyLength, valueLength, recordsPerBucket)

	return
}

// SetBucketRecord - Sets a bucket record in the hash map file
func SetBucketRecord(f *os.File, record Record, keyLength, valueLength int64) (err error) {
	buf := make([]byte, 1, keyLength+valueLength+conf.InUseFlagBytes)
	if record.InUse {
		buf[0] = 1
	}
	buf = append(buf, record.Key...)
	buf = append(buf, record.Value...)

	_, err = f.Seek(record.RecordAddress, io.SeekStart)
	if err != nil {
		return
	}

	_, err = f.Write(buf)

	return
}

// OpenHashMapFile - Opens the hash map file and does some rudimentary checks of its validity
func OpenHashMapFile(fileName string, externalAlg bool) (filePtr *os.File, header Header, err error) {
	if stat, ok := os.Stat(fileName); ok == nil {
		filePtr, err = os.OpenFile(fileName, os.O_RDWR, 0644)
		if err != nil {
			err = fmt.Errorf("unable to open existing hash map file: %s", err)
			return
		}

		header, err = GetHeader(filePtr)
		if err != nil {
			_ = filePtr.Close()
			filePtr = nil
			err = fmt.Errorf("unable to read header from hash map file: %s", err)
			return
		}

		if stat.Size() != header.FileSize {
			_ = filePtr.Close()
			filePtr = nil
			err = fmt.Errorf("actual file size doesn't conform with header indicated file size")
			return
		}

		if header.InternalAlg && externalAlg {
			_ = filePtr.Close()
			filePtr = nil
			err = fmt.Errorf("seems the hash map file was used with the internal hash algorithm but an external was given")
			return
		}
	} else {
		err = fmt.Errorf("hash map file not found")
		return
	}

	return
}

// OpenOverflowFile - Opens the overflow file and does som rudimentary checks of its validity
func OpenOverflowFile(fileName string) (filePtr *os.File, err error) {
	if stat, ok := os.Stat(fileName); ok == nil {
		filePtr, err = os.OpenFile(fileName, os.O_RDWR, 0644)
		if err != nil {
			err = fmt.Errorf("unable to open existing overflow file: %s", err)
			return
		}

		if stat.Size() < conf.OvflFileHeaderLength {
			_ = filePtr.Close()
			filePtr = nil
			err = fmt.Errorf("actual file size is smaller than minimum overflow file size")
			return
		}
	} else {
		err = fmt.Errorf("overflow file not found")
		return
	}

	return
}

// CreateNewHashMapFile - Creates a new hash map file. If it already exists it will first be truncated to zero length
// and then to expected length, hence deleting all existing data.
func CreateNewHashMapFile(fileName string, fileSize int64) (filePtr *os.File, err error) {
	filePtr, err = os.OpenFile(fileName, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0644)
	if err != nil {
		err = fmt.Errorf("error while open/create new map file: %s", err)
		return
	}
	err = filePtr.Truncate(fileSize)
	if err != nil {
		_ = filePtr.Close()
		filePtr = nil
		err = fmt.Errorf("error while truncate new map file to length %d: %s", fileSize, err)
	}

	return
}

// CreateNewOverflowFile - Creates a new overflow file. If it already exists it will first be truncated to zero length
// and then to expected length, hence deleting all existing data.
func CreateNewOverflowFile(fileName string) (filePtr *os.File, err error) {
	filePtr, err = os.OpenFile(fileName, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0644)
	if err != nil {
		err = fmt.Errorf("error while open/create new overflow file: %s", err)
		return
	}
	err = filePtr.Truncate(conf.OvflFileHeaderLength)
	if err != nil {
		_ = filePtr.Close()
		filePtr = nil
		err = fmt.Errorf("error while truncate new overflow file to length %d: %s", conf.OvflFileHeaderLength, err)
	}

	return
}

// CloseFiles - Closes the map files
func CloseFiles(mapFile, ovflFile *os.File) {
	if ovflFile != nil {
		_ = ovflFile.Sync()
		_ = ovflFile.Close()
	}

	if mapFile != nil {
		_ = mapFile.Sync()
		_ = mapFile.Close()
	}
}

// RemoveFiles - Removes the map files, make sure to close them first before calling this function
func RemoveFiles(mapFileName, ovflFileName string) (err error) {
	// Only try to remove if exists, and are not by accident directories (could happen when testing things out)
	if stat, ok := os.Stat(ovflFileName); ok == nil {
		if !stat.IsDir() {
			err = os.Remove(ovflFileName)
			if err != nil {
				err = fmt.Errorf("error while removing overflow file: %s", err)
				return
			}
		}
	}
	if stat, ok := os.Stat(mapFileName); ok == nil {
		if !stat.IsDir() {
			err = os.Remove(mapFileName)
			if err != nil {
				err = fmt.Errorf("error while removing map file: %s", err)
				return
			}
		}
	}

	return
}

// NewBucketOverflow - Adds a new overflow record to a file.
func NewBucketOverflow(f *os.File, key, value []byte, keyLength, valueLength int64) (overflowAddress int64, err error) {
	overflowAddress, err = f.Seek(0, io.SeekEnd)
	if err != nil {
		return
	}

	buf := make([]byte, conf.OverflowAddressLength+conf.InUseFlagBytes, keyLength+valueLength+conf.OverflowAddressLength)
	buf[conf.OverflowAddressLength] = conf.RecordInUse
	buf = append(buf, key...)
	buf = append(buf, value...)

	_, err = f.Write(buf)
	if err != nil {
		return
	}

	return
}

// SetBucketOverflowAddress - Sets the overflow address for a bucket identified by its address in file
func SetBucketOverflowAddress(f *os.File, bucketAddress, overflowAddress int64) (err error) {
	_, err = f.Seek(bucketAddress+conf.BucketOverflowAddressOffset, io.SeekStart)
	if err != nil {
		return
	}

	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(overflowAddress))

	_, err = f.Write(buf)
	if err != nil {
		return
	}

	return
}

// GetOverflowRecord - Gets a Record from the overflow file
func GetOverflowRecord(f *os.File, recordAddress, keyLength, valueLength int64) (record Record, err error) {
	trueRecordLength := keyLength + valueLength + conf.InUseFlagBytes
	_, err = f.Seek(recordAddress, io.SeekStart)
	if err != nil {
		return
	}

	buf := make([]byte, trueRecordLength+conf.OverflowAddressLength)
	_, err = f.Read(buf)
	if err != nil {
		return
	}

	record, err = overflowBytesToRecord(buf, recordAddress, keyLength, valueLength)
	return
}

// SetOverflowRecord - Sets a Record in the overflow file
func SetOverflowRecord(f *os.File, record Record, keyLength, valueLength int64) (err error) {
	buf := recordToOverflowBytes(record, keyLength, valueLength)

	_, err = f.Seek(record.RecordAddress, io.SeekStart)
	if err != nil {
		return
	}

	_, err = f.Write(buf)

	return
}

// AppendOverflowRecord - Appends a record to the overflow file and updates the linking record with the new
// records address
func AppendOverflowRecord(f *os.File, linkingRecord Record, key, value []byte, keyLength, valueLength int64) (err error) {
	overflowAddress, err := NewBucketOverflow(f, key, value, keyLength, valueLength)
	if err != nil {
		return
	}

	buf := make([]byte, conf.OverflowAddressLength)
	binary.LittleEndian.PutUint64(buf, uint64(overflowAddress))

	_, err = f.Seek(linkingRecord.RecordAddress, io.SeekStart)
	if err != nil {
		return
	}

	_, err = f.Write(buf)

	return
}
