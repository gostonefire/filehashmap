package openaddressing

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
func (Q *OAFiles) createNewHashMapFile(header storage.Header) (err error) {
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
func (Q *OAFiles) openHashMapFile() (header storage.Header, err error) {
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

	} else {
		err = fmt.Errorf("hash map file not found")
		return
	}

	return
}

// getBucketRecord - Returns record for a given bucket number in a model.Bucket struct
func (Q *OAFiles) getBucketRecord(bucketNo int64) (bucket model.Bucket, err error) {
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

// setBucketRecord - Sets a bucket record in the hash map file
func (Q *OAFiles) setBucketRecord(record model.Record) (err error) {
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
func (Q *OAFiles) bytesToBucket(buf []byte, bucketAddress int64) (bucket model.Bucket, err error) {
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
func (Q *OAFiles) createHeader() (header storage.Header) {
	header = storage.Header{
		InternalHash:                 Q.internalAlgorithm,
		KeyLength:                    Q.keyLength,
		ValueLength:                  Q.valueLength,
		NumberOfBucketsNeeded:        Q.numberOfBucketsNeeded,
		NumberOfBucketsAvailable:     Q.numberOfBucketsAvailable,
		MaxBucketNo:                  Q.maxBucketNo,
		FileSize:                     Q.mapFileSize,
		CollisionResolutionTechnique: int64(Q.CollisionResolutionTechnique),
	}

	return
}

// probingForGet - Is the Probing Collision Resolution Technique algorithm for getting a record.
func (Q *OAFiles) probingForGet(key []byte) (record model.Record, err error) {
	var bucket model.Bucket
	var probe, n int64

	hf1Value := Q.hashAlgorithm.HashFunc1(key)
	hf2Value := Q.hashAlgorithm.HashFunc2(key)

	iMax := Q.numberOfBucketsAvailable * 10 // To avoid infinite loop if hash algorithm is behaving bad

	for i := int64(0); i < iMax; i++ {
		probe = Q.hashAlgorithm.ProbeIteration(hf1Value, hf2Value, i)
		if probe < Q.numberOfBucketsAvailable && probe >= 0 {
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

			// Relies on the underlying probing function to distinctively go through the entire set of buckets
			n++
			if n >= Q.numberOfBucketsAvailable {
				record = model.Record{}
				err = crt.NoRecordFound{}
				return
			}
		}
	}

	// When we have traversed long enough we just have to give up
	// This is just a failsafe, should (with emphasis on should) never occur
	record = model.Record{}
	err = crt.ProbingAlgorithm{}
	return
}

// probingForSet - Is the Probing Collision Resolution Technique algorithm for getting a record for set.
func (Q *OAFiles) probingForSet(key []byte) (record model.Record, err error) {
	var bucket model.Bucket
	var deletedRecord model.Record
	var hasCached bool
	var probe, n int64

	hf1Value := Q.hashAlgorithm.HashFunc1(key)
	hf2Value := Q.hashAlgorithm.HashFunc2(key)

	iMax := Q.numberOfBucketsAvailable * 10 // To avoid infinite loop if hash algorithm is behaving bad

	for i := int64(0); i < iMax; i++ {
		probe = Q.hashAlgorithm.ProbeIteration(hf1Value, hf2Value, i)
		if probe < Q.numberOfBucketsAvailable && probe >= 0 {
			bucket, err = Q.getBucketRecord(probe)
			if err != nil {
				err = fmt.Errorf("error while reading bucket from file: %s", err)
				return
			}

			switch bucket.Record.State {
			case model.RecordEmpty:
				if hasCached {
					record = deletedRecord
					return
				} else {
					record = bucket.Record
				}
				return

			case model.RecordOccupied:
				if utils.IsEqual(key, bucket.Record.Key) {
					record = bucket.Record
					return
				}

			case model.RecordDeleted:
				if !hasCached {
					deletedRecord = bucket.Record
					hasCached = true
				}
			}

			// Relies on the underlying probing function to distinctively go through the entire set of buckets
			n++
			if n >= Q.numberOfBucketsAvailable {
				err = crt.MapFileFull{}
				return
			}
		}
	}

	// When we have traversed long enough we just have to give up
	// This is just a failsafe, should (with emphasis on should) never occur
	err = crt.ProbingAlgorithm{}
	return
}
