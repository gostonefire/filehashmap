package qpres

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
func (Q *QPFiles) createNewHashMapFile(header storage.Header) (err error) {
	Q.mapFile, err = os.OpenFile(Q.mapFileName, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0644)
	if err != nil {
		err = fmt.Errorf("error while open/create new map file: %s", err)
		return
	}
	err = Q.mapFile.Truncate(Q.mapFileSize)
	if err != nil {
		_ = Q.mapFile.Close()
		Q.mapFile = nil
		err = fmt.Errorf("error while truncate new map file to length %d: %s", Q.mapFileSize, err)
		return
	}

	err = storage.SetHeader(Q.mapFile, header)
	if err != nil {
		err = fmt.Errorf("error while writing header to map file: %s", err)
		return
	}

	return
}

// openHashMapFile - Opens the hash map file and does some rudimentary checks of its validity and
// returns a Header struct read from file
func (Q *QPFiles) openHashMapFile() (header storage.Header, err error) {
	if stat, ok := os.Stat(Q.mapFileName); ok == nil {
		Q.mapFile, err = os.OpenFile(Q.mapFileName, os.O_RDWR, 0644)
		if err != nil {
			err = fmt.Errorf("unable to open existing hash map file: %s", err)
			return
		}

		header, err = storage.GetHeader(Q.mapFile)
		if err != nil {
			_ = Q.mapFile.Close()
			Q.mapFile = nil
			err = fmt.Errorf("unable to read header from hash map file: %s", err)
			return
		}

		if stat.Size() != header.FileSize {
			_ = Q.mapFile.Close()
			Q.mapFile = nil
			err = fmt.Errorf("actual file size doesn't conform with header indicated file size")
			return
		}

		// Check if we need update header with hash map utilization info
		if header.FileCloseDate == 0 {
			header, err = storage.GetFileUtilization(Q.mapFile, 0, header)
			if err != nil {
				_ = Q.mapFile.Close()
				Q.mapFile = nil
				return
			}
		} else {
			err = storage.SetFileCloseDate(Q.mapFile, true)
			if err != nil {
				_ = Q.mapFile.Close()
				Q.mapFile = nil
				err = fmt.Errorf("error when trying to write to hash map file")
				return
			}
		}

	} else {
		err = fmt.Errorf("hash map file not found")
		return
	}

	return
}

// getBucketRecord - Returns record for a given bucket number in a model.Bucket struct
func (Q *QPFiles) getBucketRecord(bucketNo int64) (bucket model.Bucket, err error) {
	trueRecordLength := 1 + Q.keyLength + Q.valueLength // First byte is record state
	bucketAddress := storage.MapFileHeaderLength + bucketNo*trueRecordLength

	_, err = Q.mapFile.Seek(bucketAddress, io.SeekStart)
	if err != nil {
		return
	}

	buf := make([]byte, trueRecordLength)
	_, err = Q.mapFile.Read(buf)
	if err != nil {
		return
	}

	bucket, err = Q.bytesToBucket(buf, bucketAddress)

	return
}

// getBucketNo - Returns which bucket number that the given key results in
func (Q *QPFiles) getBucketNo(key []byte) (bucketNo int64, err error) {
	bucketNo = Q.hashAlgorithm.HashFunc1(key)
	if bucketNo < 0 || bucketNo >= Q.numberOfBucketsAvailable {
		err = fmt.Errorf("recieved bucket number from bucket algorithm is outside permitted range")
		return
	}

	return
}

// setBucketRecord - Sets a bucket record in the hash map file
func (Q *QPFiles) setBucketRecord(record model.Record) (err error) {
	buf := make([]byte, 1, 1+Q.keyLength+Q.valueLength) // First byte is record state
	buf[0] = record.State

	buf = append(buf, record.Key...)
	buf = append(buf, record.Value...)

	_, err = Q.mapFile.Seek(record.RecordAddress, io.SeekStart)
	if err != nil {
		return
	}

	_, err = Q.mapFile.Write(buf)

	return
}

// bytesToBucket - Converts bucket raw data to a Bucket struct
func (Q *QPFiles) bytesToBucket(buf []byte, bucketAddress int64) (bucket model.Bucket, err error) {
	keyStart := int64(1) // First byte is record state
	valueStart := keyStart + Q.keyLength

	key := make([]byte, Q.keyLength)
	value := make([]byte, Q.valueLength)
	_ = copy(key, buf[keyStart:keyStart+Q.keyLength])
	_ = copy(value, buf[valueStart:valueStart+Q.valueLength])

	bucket = model.Bucket{
		Record: model.Record{
			State:         buf[0],
			RecordAddress: bucketAddress,
			Key:           key,
			Value:         value,
		},
		BucketAddress:   bucketAddress,
		OverflowAddress: 0,
		HasOverflow:     false,
	}

	return
}

// createHeader - Creates a header instance
func (Q *QPFiles) createHeader() (header storage.Header) {
	header = storage.Header{
		InternalHash:                 Q.internalAlgorithm,
		KeyLength:                    Q.keyLength,
		ValueLength:                  Q.valueLength,
		NumberOfBucketsNeeded:        Q.numberOfBucketsNeeded,
		NumberOfBucketsAvailable:     Q.numberOfBucketsAvailable,
		MaxBucketNo:                  Q.maxBucketNo,
		FileSize:                     Q.mapFileSize,
		CollisionResolutionTechnique: int64(crt.QuadraticProbing),
		NumberOfEmptyRecords:         Q.nEmpty,
		NumberOfOccupiedRecords:      Q.nOccupied,
		NumberOfDeletedRecords:       Q.nDeleted,
	}

	return
}

// updateUtilizationInfo - Updates information about current utilization
func (Q *QPFiles) updateUtilizationInfo(fromState, toState uint8) {
	if fromState != toState {
		switch fromState {
		case model.RecordEmpty:
			Q.nEmpty--
		case model.RecordOccupied:
			Q.nOccupied--
		case model.RecordDeleted:
			Q.nDeleted--
		}

		switch toState {
		case model.RecordEmpty:
			Q.nEmpty++
		case model.RecordOccupied:
			Q.nOccupied++
		case model.RecordDeleted:
			Q.nDeleted++
		}
	}
}

// quadraticProbingForGet - Is the Quadratic Probing Collision Resolution Technique algorithm for getting a record.
func (Q *QPFiles) quadraticProbingForGet(bucketNo int64, key []byte) (record model.Record, err error) {
	// Loop through at most the entire set of buckets
	var bucket model.Bucket
	var probe int64
	iMax := Q.numberOfBucketsAvailable * 10

	for i := int64(0); i < iMax; i++ {
		probe = (bucketNo + ((i*i + i) / 2)) % Q.roundUp2
		if probe < Q.numberOfBucketsAvailable {
			bucket, err = Q.getBucketRecord(probe)
			if err != nil {
				err = fmt.Errorf("error while reading bucket from file: %s", err)
				return
			}

			switch bucket.Record.State {
			case model.RecordEmpty:
				record = model.Record{}
				err = crt.NoRecordFound{}
				return

			case model.RecordOccupied:
				if utils.IsEqual(key, bucket.Record.Key) {
					record = bucket.Record
					return
				}
			}
		}
	}

	// When we have traversed long enough we just have to give up
	// This is just a failsafe, should (with emphasis on should) never occur
	record = model.Record{}
	err = crt.ProbingAlgorithm{}
	return
}

// quadraticProbingForSet - Is the Quadratic Probing Collision Resolution Technique algorithm for getting a record for set.
func (Q *QPFiles) quadraticProbingForSet(bucketNo int64, key []byte) (record model.Record, err error) {
	// Loop through at most the entire set of buckets
	var bucket model.Bucket
	var deletedRecord model.Record
	var hasCached, isSeekDeletedRecord bool
	var probe int64
	iMax := Q.numberOfBucketsAvailable * 10

	for i := int64(0); i < iMax; i++ {
		probe = (bucketNo + ((i*i + i) / 2)) % Q.roundUp2
		if probe < Q.numberOfBucketsAvailable {
			bucket, err = Q.getBucketRecord(probe)
			if err != nil {
				err = fmt.Errorf("error while reading bucket from file: %s", err)
				return
			}

			switch bucket.Record.State {
			case model.RecordEmpty:
				if !isSeekDeletedRecord {
					if hasCached {
						record = deletedRecord
						return
					}
					if Q.nEmpty > 1 {
						record = bucket.Record
						return
					}
					if Q.nDeleted == 0 {
						err = crt.MapFileFull{}
						return
					}
					isSeekDeletedRecord = true
				}

			case model.RecordOccupied:
				if !isSeekDeletedRecord && utils.IsEqual(key, bucket.Record.Key) {
					record = bucket.Record
					return
				}

			case model.RecordDeleted:
				if isSeekDeletedRecord {
					record = bucket.Record
					return
				}
				if !hasCached {
					deletedRecord = bucket.Record
					hasCached = true
				}
			}
		}
	}

	// When we have traversed long enough we just have to give up
	// This is just a failsafe, should (with emphasis on should) never occur
	err = crt.ProbingAlgorithm{}
	return
}
