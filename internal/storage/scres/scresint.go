package scres

import (
	"encoding/binary"
	"fmt"
	"github.com/gostonefire/filehashmap/internal/model"
	"github.com/gostonefire/filehashmap/internal/storage"
	"io"
	"os"
)

// openHashMapFile - Opens the hash map file and does some rudimentary checks of its validity and
// returns a Header struct read from file
func (S *SCFiles) openHashMapFile() (header storage.Header, err error) {
	if stat, ok := os.Stat(S.mapFileName); ok == nil {
		S.mapFile, err = os.OpenFile(S.mapFileName, os.O_RDWR, 0644)
		if err != nil {
			err = fmt.Errorf("unable to open existing hash map file: %s", err)
			return
		}

		header, err = storage.GetHeader(S.mapFile)
		if err != nil {
			_ = S.mapFile.Close()
			S.mapFile = nil
			err = fmt.Errorf("unable to read header from hash map file: %s", err)
			return
		}

		if stat.Size() != header.FileSize {
			_ = S.mapFile.Close()
			S.mapFile = nil
			err = fmt.Errorf("actual file size doesn't conform with header indicated file size")
			return
		}
	} else {
		err = fmt.Errorf("hash map file not found")
		return
	}

	return
}

// openOverflowFile - Opens the overflow file and does som rudimentary checks of its validity
func (S *SCFiles) openOverflowFile() (err error) {
	if stat, ok := os.Stat(S.ovflFileName); ok == nil {
		S.ovflFile, err = os.OpenFile(S.ovflFileName, os.O_RDWR, 0644)
		if err != nil {
			err = fmt.Errorf("unable to open existing overflow file: %s", err)
			return
		}

		if stat.Size() < ovflFileHeaderLength {
			_ = S.ovflFile.Close()
			S.ovflFile = nil
			err = fmt.Errorf("actual file size is smaller than minimum overflow file size")
			return
		}
	} else {
		err = fmt.Errorf("overflow file not found")
		return
	}

	return
}

// createNewHashMapFile - Creates a new hash map file and writes Header data to it.
// If it already exists it will first be truncated to zero length and then to expected length,
// hence deleting all existing data.
func (S *SCFiles) createNewHashMapFile(header storage.Header) (err error) {
	S.mapFile, err = os.OpenFile(S.mapFileName, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0644)
	if err != nil {
		err = fmt.Errorf("error while open/create new map file: %s", err)
		return
	}
	err = S.mapFile.Truncate(S.mapFileSize)
	if err != nil {
		_ = S.mapFile.Close()
		S.mapFile = nil
		err = fmt.Errorf("error while truncate new map file to length %d: %s", S.mapFileSize, err)
		return
	}

	err = storage.SetHeader(S.mapFile, header)
	if err != nil {
		err = fmt.Errorf("error while writing header to map file: %s", err)
		return
	}

	return
}

// createNewOverflowFile - Creates a new overflow file. If it already exists it will first be truncated to zero length
// and then to expected length, hence deleting all existing data.
func (S *SCFiles) createNewOverflowFile() (err error) {
	S.ovflFile, err = os.OpenFile(S.ovflFileName, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0644)
	if err != nil {
		err = fmt.Errorf("error while open/create new overflow file: %s", err)
		return
	}
	err = S.ovflFile.Truncate(ovflFileHeaderLength)
	if err != nil {
		_ = S.ovflFile.Close()
		S.ovflFile = nil
		err = fmt.Errorf("error while truncate new overflow file to length %d: %s", ovflFileHeaderLength, err)
	}

	return
}

// getBucketRecord - Returns all records for a given bucket number in a model.Bucket struct
func (S *SCFiles) getBucketRecord(bucketNo int64) (bucket model.Bucket, err error) {
	trueRecordLength := S.keyLength + S.valueLength + stateBytes
	bucketAddress := storage.MapFileHeaderLength + bucketNo*(trueRecordLength+bucketHeaderLength)

	_, err = S.mapFile.Seek(bucketAddress, io.SeekStart)
	if err != nil {
		return
	}

	buf := make([]byte, trueRecordLength+bucketHeaderLength)
	_, err = S.mapFile.Read(buf)
	if err != nil {
		return
	}

	bucket, err = bytesToBucket(buf, bucketAddress, S.keyLength, S.valueLength)

	return
}

// setBucketRecord - Sets a bucket record in the hash map file
func (S *SCFiles) setBucketRecord(record model.Record) (err error) {
	buf := make([]byte, 1, S.keyLength+S.valueLength+stateBytes)
	buf[0] = record.State

	buf = append(buf, record.Key...)
	buf = append(buf, record.Value...)

	_, err = S.mapFile.Seek(record.RecordAddress, io.SeekStart)
	if err != nil {
		return
	}

	_, err = S.mapFile.Write(buf)

	return
}

// setBucketOverflowAddress - Sets the overflow address for a bucket identified by its address in file
func (S *SCFiles) setBucketOverflowAddress(bucketAddress, overflowAddress int64) (err error) {
	_, err = S.mapFile.Seek(bucketAddress+bucketOverflowAddressOffset, io.SeekStart)
	if err != nil {
		return
	}

	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(overflowAddress))

	_, err = S.mapFile.Write(buf)
	if err != nil {
		return
	}

	return
}

// getOverflowRecord - Gets a model.Record from the overflow file
func (S *SCFiles) getOverflowRecord(recordAddress int64) (record model.Record, err error) {
	trueRecordLength := S.keyLength + S.valueLength + stateBytes
	_, err = S.ovflFile.Seek(recordAddress, io.SeekStart)
	if err != nil {
		return
	}

	buf := make([]byte, trueRecordLength+overflowAddressLength)
	_, err = S.ovflFile.Read(buf)
	if err != nil {
		return
	}

	record, err = overflowBytesToRecord(buf, recordAddress, S.keyLength, S.valueLength)
	return
}

// setOverflowRecord - Sets a model.Record in the overflow file
func (S *SCFiles) setOverflowRecord(record model.Record) (err error) {
	buf := recordToOverflowBytes(record, S.keyLength, S.valueLength)

	_, err = S.ovflFile.Seek(record.RecordAddress, io.SeekStart)
	if err != nil {
		return
	}

	_, err = S.ovflFile.Write(buf)

	return
}

// appendOverflowRecord - Appends a model.Record to the overflow file and updates the linking record with the new
// records address
func (S *SCFiles) appendOverflowRecord(linkingRecord model.Record, key, value []byte) (err error) {
	overflowAddress, err := S.newBucketOverflow(key, value)
	if err != nil {
		return
	}

	buf := make([]byte, overflowAddressLength)
	binary.LittleEndian.PutUint64(buf, uint64(overflowAddress))

	_, err = S.ovflFile.Seek(linkingRecord.RecordAddress, io.SeekStart)
	if err != nil {
		return
	}

	_, err = S.ovflFile.Write(buf)

	return
}

// getBucketNo - Returns which bucket number that the given key results in
func (S *SCFiles) getBucketNo(key []byte) (bucketNo int64, err error) {
	bucketNo = S.hashAlgorithm.HashFunc1(key) - S.minBucketNo
	if bucketNo < 0 || bucketNo >= S.numberOfBucketsAvailable {
		err = fmt.Errorf("recieved bucket number from bucket algorithm is outside permitted range")
		return
	}

	return
}

// newBucketOverflow - Adds a new overflow record to a file.
func (S *SCFiles) newBucketOverflow(key, value []byte) (overflowAddress int64, err error) {
	overflowAddress, err = S.ovflFile.Seek(0, io.SeekEnd)
	if err != nil {
		return
	}

	buf := make([]byte, overflowAddressLength+stateBytes, S.keyLength+S.valueLength+overflowAddressLength)
	buf[overflowAddressLength] = model.RecordOccupied
	buf = append(buf, key...)
	buf = append(buf, value...)

	_, err = S.ovflFile.Write(buf)
	if err != nil {
		return
	}

	return
}
