package scres

import (
	"encoding/binary"
	"fmt"
	"github.com/gostonefire/filehashmap/internal/model"
	"github.com/gostonefire/filehashmap/internal/utils"
	"github.com/gostonefire/filehashmap/storage"
	"io"
	"os"
)

// getHeader - Reads header data from file and returns it as a model.Header struct
func (S *SCFiles) getHeader() (header model.Header, err error) {
	_, err = S.mapFile.Seek(0, io.SeekStart)
	if err != nil {
		return
	}

	buf := make([]byte, mapFileHeaderLength)
	_, err = S.mapFile.Read(buf)
	if err != nil {
		return
	}

	header = bytesToHeader(buf)

	return
}

// setHeader - Takes a model.Header struct and writes header data to file
func (S *SCFiles) setHeader(header model.Header) (err error) {
	_, err = S.mapFile.Seek(0, io.SeekStart)
	if err != nil {
		return
	}

	buf := headerToBytes(header)

	_, err = S.mapFile.Write(buf)

	return
}

// openHashMapFile - Opens the hash map file and does some rudimentary checks of its validity and
// returns a model.Header struct read from file
func (S *SCFiles) openHashMapFile() (header model.Header, err error) {
	if stat, ok := os.Stat(S.mapFileName); ok == nil {
		S.mapFile, err = os.OpenFile(S.mapFileName, os.O_RDWR, 0644)
		if err != nil {
			err = fmt.Errorf("unable to open existing hash map file: %s", err)
			return
		}

		header, err = S.getHeader()
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

// createNewHashMapFile - Creates a new hash map file and writes model.Header data to it.
// If it already exists it will first be truncated to zero length and then to expected length,
// hence deleting all existing data.
func (S *SCFiles) createNewHashMapFile(header model.Header) (err error) {
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

	err = S.setHeader(header)
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

// getBucketRecords - Returns all records for a given bucket number in a model.Bucket struct
func (S *SCFiles) getBucketRecords(bucketNo int64) (bucket model.Bucket, err error) {
	trueRecordLength := S.keyLength + S.valueLength + inUseFlagBytes
	bucketAddress := mapFileHeaderLength + bucketNo*(trueRecordLength*S.recordsPerBucket+bucketHeaderLength)

	_, err = S.mapFile.Seek(bucketAddress, io.SeekStart)
	if err != nil {
		return
	}

	buf := make([]byte, trueRecordLength*S.recordsPerBucket+bucketHeaderLength)
	_, err = S.mapFile.Read(buf)
	if err != nil {
		return
	}

	bucket, err = bytesToBucket(buf, bucketAddress, S.keyLength, S.valueLength, S.recordsPerBucket)

	return
}

// setBucketRecord - Sets a bucket record in the hash map file
func (S *SCFiles) setBucketRecord(record model.Record) (err error) {
	buf := make([]byte, 1, S.keyLength+S.valueLength+inUseFlagBytes)
	if record.InUse {
		buf[0] = 1
	}
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
	trueRecordLength := S.keyLength + S.valueLength + inUseFlagBytes
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
	bucketNo = S.hashAlgorithm.BucketNumber(key) - S.minBucketNo
	if bucketNo < 0 || bucketNo >= S.numberOfBuckets {
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

	buf := make([]byte, overflowAddressLength+inUseFlagBytes, S.keyLength+S.valueLength+overflowAddressLength)
	buf[overflowAddressLength] = recordInUse
	buf = append(buf, key...)
	buf = append(buf, value...)

	_, err = S.ovflFile.Write(buf)
	if err != nil {
		return
	}

	return
}

// getBucketRecordToUpdate - Searches the bucket records for a matching record to return. If no match, then
// any eventual free bucket record are returned instead.
// It returns an error of type storage.NoRecordFound if no matching record or free record was found
func (S *SCFiles) getBucketRecordToUpdate(bucket model.Bucket, recordId []byte) (record model.Record, err error) {

	var hasAvailable bool
	var availableRecord model.Record
	for _, r := range bucket.Records {
		if r.InUse {
			if utils.IsEqual(recordId, r.Key) {
				record = r
				return
			}
		} else if !hasAvailable {
			hasAvailable = true
			availableRecord = model.Record{
				InUse:         false,
				RecordAddress: r.RecordAddress,
				Key:           nil,
				Value:         nil,
			}
		}
	}

	if hasAvailable {
		record = availableRecord
		return
	}

	err = storage.NoRecordFound{}
	return
}

// getOverflowRecordToUpdate - Searches the overflow for the bucket for a matching record to return.
// It returns:
//   - record is either a record to update or the linking record if no match, the latter comes together with an error of type storage.NoRecordFound.
//   - err is either of type storage.NoRecordFound or a standard error if something went wrong
func (S *SCFiles) getOverflowRecordToUpdate(iter *OverflowRecords, key []byte) (record model.Record, err error) {
	var hasAvailable bool
	var availableRecord model.Record
	for iter.HasNext() {
		record, err = iter.Next()
		if err != nil {
			return
		}
		if record.InUse {
			if utils.IsEqual(key, record.Key) {
				return
			}
		} else if !hasAvailable {
			hasAvailable = true
			availableRecord = model.Record{
				InUse:         false,
				IsOverflow:    true,
				RecordAddress: record.RecordAddress,
				NextOverflow:  record.NextOverflow,
				Key:           nil,
				Value:         nil,
			}
		}
	}

	if hasAvailable {
		record = availableRecord
	} else {
		err = storage.NoRecordFound{}
	}

	return
}
