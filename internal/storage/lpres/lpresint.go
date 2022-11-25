package lpres

import (
	"fmt"
	"github.com/gostonefire/filehashmap/crt"
	"github.com/gostonefire/filehashmap/internal/model"
	"github.com/gostonefire/filehashmap/internal/storage"
	"github.com/gostonefire/filehashmap/internal/utils"
	"io"
	"os"
)

// createNewHashMapFile - Creates a new hash map file and writes Header data to it.
// If it already exists it will first be truncated to zero length and then to expected length,
// hence deleting all existing data.
func (L *LPFiles) createNewHashMapFile(header storage.Header) (err error) {
	L.mapFile, err = os.OpenFile(L.mapFileName, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0644)
	if err != nil {
		err = fmt.Errorf("error while open/create new map file: %s", err)
		return
	}
	err = L.mapFile.Truncate(L.mapFileSize)
	if err != nil {
		_ = L.mapFile.Close()
		L.mapFile = nil
		err = fmt.Errorf("error while truncate new map file to length %d: %s", L.mapFileSize, err)
		return
	}

	err = storage.SetHeader(L.mapFile, header)
	if err != nil {
		err = fmt.Errorf("error while writing header to map file: %s", err)
		return
	}

	return
}

// openHashMapFile - Opens the hash map file and does some rudimentary checks of its validity and
// returns a Header struct read from file
func (L *LPFiles) openHashMapFile() (header storage.Header, err error) {
	if stat, ok := os.Stat(L.mapFileName); ok == nil {
		L.mapFile, err = os.OpenFile(L.mapFileName, os.O_RDWR, 0644)
		if err != nil {
			err = fmt.Errorf("unable to open existing hash map file: %s", err)
			return
		}

		header, err = storage.GetHeader(L.mapFile)
		if err != nil {
			_ = L.mapFile.Close()
			L.mapFile = nil
			err = fmt.Errorf("unable to read header from hash map file: %s", err)
			return
		}

		if stat.Size() != header.FileSize {
			_ = L.mapFile.Close()
			L.mapFile = nil
			err = fmt.Errorf("actual file size doesn't conform with header indicated file size")
			return
		}
	} else {
		err = fmt.Errorf("hash map file not found")
		return
	}

	return
}

// getBucketRecord - Returns record for a given bucket number in a model.Bucket struct
func (L *LPFiles) getBucketRecord(bucketNo int64) (bucket model.Bucket, err error) {
	trueRecordLength := L.keyLength + L.valueLength + stateBytes
	bucketAddress := storage.MapFileHeaderLength + bucketNo*trueRecordLength

	_, err = L.mapFile.Seek(bucketAddress, io.SeekStart)
	if err != nil {
		return
	}

	buf := make([]byte, trueRecordLength)
	_, err = L.mapFile.Read(buf)
	if err != nil {
		return
	}

	bucket, err = bytesToBucket(buf, bucketAddress, L.keyLength, L.valueLength)

	return
}

// getBucketNo - Returns which bucket number that the given key results in
func (L *LPFiles) getBucketNo(key []byte) (bucketNo int64, err error) {
	bucketNo = L.hashAlgorithm.HashFunc1(key) - L.minBucketNo
	if bucketNo < 0 || bucketNo >= L.numberOfBucketsAvailable {
		err = fmt.Errorf("recieved bucket number from bucket algorithm is outside permitted range")
		return
	}

	return
}

// setBucketRecord - Sets a bucket record in the hash map file
func (L *LPFiles) setBucketRecord(record model.Record) (err error) {
	buf := make([]byte, 1, L.keyLength+L.valueLength+stateBytes)
	buf[0] = record.State

	buf = append(buf, record.Key...)
	buf = append(buf, record.Value...)

	_, err = L.mapFile.Seek(record.RecordAddress, io.SeekStart)
	if err != nil {
		return
	}

	_, err = L.mapFile.Write(buf)

	return
}

// linearProbingForGet - Is the Linear Probing Collision Resolution Technique algorithm for getting a record.
func (L *LPFiles) linearProbingForGet(bucketNo int64, key []byte) (record model.Record, err error) {
	// Loop through at most the entire set of buckets
	var bucket model.Bucket
	for i := int64(0); i < L.numberOfBucketsAvailable; i++ {
		if bucketNo+i == L.numberOfBucketsAvailable {
			bucketNo = -i
		}
		bucket, err = L.getBucketRecord(bucketNo + i)
		if err != nil {
			err = fmt.Errorf("error while reading bucket from file: %s", err)
			return
		}

		// If record is occupied (but not with correct key) or deleted (default in if clause below) then keep searching,
		// but if record is empty then the key can never have been added in the map file.
		if bucket.Record.State == model.RecordOccupied && utils.IsEqual(key, bucket.Record.Key) {
			record = bucket.Record
			return
		} else if bucket.Record.State == model.RecordEmpty {
			break
		}
	}

	record = model.Record{}
	err = crt.NoRecordFound{}

	return
}

// linearProbingForSet - Is the Linear Probing Collision Resolution Technique algorithm for getting a record for set.
func (L *LPFiles) linearProbingForSet(bucketNo int64, key []byte) (record model.Record, err error) {
	// Loop through at most the entire set of buckets
	var bucket model.Bucket
	var deletedRecord model.Record
	var hasDeleted bool
	var i int64
	for {
		if bucketNo+i == L.numberOfBucketsAvailable {
			bucketNo = -i
		}
		bucket, err = L.getBucketRecord(bucketNo + i)
		if err != nil {
			err = fmt.Errorf("error while reading bucket from file: %s", err)
			return
		}

		// If record is occupied and with correct key, then return it.
		// If record is empty then the key can never have been added in the map file, so return it or any
		// previously found deleted record.
		// If a record is deleted we still need to keep searching since the matching record might still be in the map file,
		// but we store it for use if no matching record is found.
		if bucket.Record.State == model.RecordOccupied && utils.IsEqual(key, bucket.Record.Key) {
			record = bucket.Record
			return
		} else if bucket.Record.State == model.RecordEmpty {
			if !hasDeleted {
				record = bucket.Record
			} else {
				record = deletedRecord
			}
			return
		} else if !hasDeleted && bucket.Record.State == model.RecordDeleted {
			deletedRecord = bucket.Record
			hasDeleted = true
		}

		i++

		// When we have traversed through the entire set of buckets we just have to face that the map file is full
		if i == L.numberOfBucketsAvailable {
			err = crt.MapFileFull{}
			return
		}
	}
}
