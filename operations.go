package filehashmap

import (
	"errors"
	"fmt"
	"github.com/gostonefire/filehashmap/internal/model"
	"github.com/gostonefire/filehashmap/internal/utils"
)

// Get - Gets record that corresponds to the given recordId.
//   - key is the identifier of a record, it has to be of same length as given in call to NewFileHashMap
//
// It returns:
//   - value is the value of the matching record if found, if not found an error of type NoRecordFound is also returned.
//   - err is either of type NoRecordFound or a standard error, if something went wrong
func (F *FileHashMap) Get(key []byte) (value []byte, err error) {
	// Check validity of the key
	if int64(len(key)) != F.keyLength {
		err = fmt.Errorf("wrong length of key, should be %d", F.keyLength)
		return
	}

	record, err := F.get(key)
	if err != nil {
		return
	}

	value = record.Value

	return
}

// Set - Updates an existing record with new data or add it if no existing is found with same ID.
//   - key is the identifier of a record, it has to be of same length as given in call to NewFileHashMap
//   - value is the bytes to be written to the bucket along with its key, length must be recordLength - keyLength (as given in call to NewFileHashMap)
//
// It returns:
//   - err is a standard error, if something went wrong
func (F *FileHashMap) Set(key []byte, value []byte) (err error) {
	// Check validity of the key
	if int64(len(key)) != F.keyLength {
		err = fmt.Errorf("wrong length of key, should be %d", F.keyLength)
		return
	}
	// Check validity of the value
	if int64(len(value)) != F.valueLength {
		err = fmt.Errorf("wrong length of value, should be %d", F.valueLength)
		return
	}

	// Get current contents from within the bucket
	bucketNo, err := F.GetBucketNo(key)
	if err != nil {
		return
	}
	bucket, ovflIter, err := F.getBucket(bucketNo)
	if err != nil {
		return
	}

	// Try to find an existing record with matching ID, or add to overflow
	var r model.Record
	r, err = F.getBucketRecordToUpdate(bucket, key)
	if err == nil {
		r.InUse = true
		r.Key = key
		r.Value = value
		err = F.fileProcessing.SetBucketRecord(r)
	} else if errors.Is(err, NoRecordFound{}) {
		if ovflIter.hasNext() {
			r, err = F.getOverflowRecordToUpdate(ovflIter, key)
			if err == nil {
				r.InUse = true
				r.Key = key
				r.Value = value
				err = F.fileProcessing.SetOverflowRecord(r)
			} else if errors.Is(err, NoRecordFound{}) {
				err = F.fileProcessing.AppendOverflowRecord(r, key, value)
			}
		} else {
			err = F.newBucketOverflow(key, value, bucket.BucketAddress)
		}
	}

	if err != nil {
		err = fmt.Errorf("error while updating or adding record to bucket or overflow: %s", err)
	}

	return
}

// Pop - Returns the record corresponding to key and removes it from the file hash map.
//   - key is the identifier of a record, it has to be of same length as given in call to NewFileHashMap
//
// It returns:
//   - value is the value of the matching record if found, if not found an error of type NoRecordFound is also returned.
//   - err is either of type NoRecordFound or a standard error, if something went wrong
func (F *FileHashMap) Pop(key []byte) (value []byte, err error) {
	// Check validity of the key
	if int64(len(key)) != F.keyLength {
		err = fmt.Errorf("wrong length of key, should be %d", F.keyLength)
		return
	}

	record, err := F.get(key)
	if err != nil {
		return
	}

	nilRecord := model.Record{
		InUse:         false,
		IsOverflow:    record.IsOverflow,
		RecordAddress: record.RecordAddress,
		NextOverflow:  record.NextOverflow,
		Key:           make([]byte, F.keyLength),
		Value:         make([]byte, F.valueLength),
	}
	if record.IsOverflow {
		err = F.fileProcessing.SetOverflowRecord(nilRecord)
	} else {
		err = F.fileProcessing.SetBucketRecord(nilRecord)
	}

	value = record.Value

	return
}

// Stat - Walks through the entire set of buckets and produce a HashMapStat struct with information.
// If the hash map file and overflow file are very big, this can take a considerable amount of time and
// the HashMapStat.BucketDistribution slice can be very memory heavy (there will be one entry per bucket).
//   - includeDistribution set to true will include a slice of length NumberOfBuckets with number of records per bucket, false will set HashMapStat.BucketDistribution to nil.
func (F *FileHashMap) Stat(includeDistribution bool) (hashMapStat *HashMapStat, err error) {
	var bucket model.Bucket
	var record model.Record
	var iter *OverflowRecords
	var hms HashMapStat

	if includeDistribution {
		hms.BucketDistribution = make([]int64, F.numberOfBuckets)
	}

	// Iterate over every available bucket
	for i := int64(0); i < F.numberOfBuckets; i++ {
		bucket, iter, err = F.getBucket(i)
		if err != nil {
			return
		}

		// Process map file records
		for _, record = range bucket.Records {
			if record.InUse {
				hms.Records++
				hms.MapFileRecords++
				if includeDistribution {
					hms.BucketDistribution[i]++
				}
			}
		}

		// Process overflow file records
		for iter.hasNext() {
			record, err = iter.next()
			if err != nil {
				return
			}
			if record.InUse {
				hms.Records++
				hms.OverflowRecords++
				if includeDistribution {
					hms.BucketDistribution[i]++
				}
			}
		}
	}

	hashMapStat = &hms
	return
}

// GetBucketNo - Returns which bucket number that the given key results in
//   - key is the identifier of a record
func (F *FileHashMap) GetBucketNo(key []byte) (bucketNo int64, err error) {
	bucketNo = F.bucketAlg.BucketNumber(key) - F.minBucketNo
	if bucketNo < 0 || bucketNo >= F.numberOfBuckets {
		err = fmt.Errorf("recieved bucket number from bucket algorithm is outside permitted range")
		return
	}

	return
}

// get - Gets record that corresponds to the given recordId.
//   - key is the identifier of a record, it has to be of same length as given in call to NewFileHashMap
//
// It returns:
//   - record is the value of the matching record if found, if not found an error of type NoRecordFound is also returned.
//   - err is either of type NoRecordFound or a standard error, if something went wrong
func (F *FileHashMap) get(key []byte) (record model.Record, err error) {
	// Get current contents from within the bucket
	bucketNo, err := F.GetBucketNo(key)
	if err != nil {
		return
	}
	bucket, ovflIter, err := F.getBucket(bucketNo)
	if err != nil {
		return
	}

	// Sort out record with correct key
	for _, record = range bucket.Records {
		if record.InUse && utils.IsEqual(key, record.Key) {
			return
		}
	}

	// Check if record may be in overflow file
	for ovflIter.hasNext() {
		record, err = ovflIter.next()
		if err != nil {
			return
		}
		if record.InUse && utils.IsEqual(key, record.Key) {
			return
		}
	}

	record = model.Record{}
	err = NoRecordFound{}

	return
}

// getBucket - Returns a bucket with its records given the recordId
//   - bucketNo is the identifier of a bucket, the number can be retrieved by call to GetBucketNo
//
// It returns:
//   - bucket is a file.Bucket struct containing all records in the map file
//   - overflowIterator is a OverflowRecords struct that can be used to get any overflow records belonging to the bucket.
//   - err is standard error
func (F *FileHashMap) getBucket(bucketNo int64) (bucket model.Bucket, overflowIterator *OverflowRecords, err error) {
	// Get current contents from within the bucket
	bucket, err = F.fileProcessing.GetBucketRecords(bucketNo)
	if err != nil {
		err = fmt.Errorf("error while getting existing bucket records from hash map file: %s", err)
		return
	}

	overflowIterator = newOverflowRecords(F.fileProcessing, bucket.OverflowAddress)

	return
}

// newBucketOverflow - Adds a new overflow for a bucket (assuming it has not already got one). It also writes the
// given record to that new spot.
func (F *FileHashMap) newBucketOverflow(key, value []byte, bucketAddress int64) (err error) {
	overflowAddress, err := F.fileProcessing.NewBucketOverflow(key, value)
	if err != nil {
		return
	}
	err = F.fileProcessing.SetBucketOverflowAddress(bucketAddress, overflowAddress)
	if err != nil {
		return
	}

	return
}

// getBucketRecordToUpdate - Searches the bucket records for a matching record to return. If no match, then
// any eventual free bucket record are returned instead.
// It returns an error of type fhmerrors.NoRecordFound if no matching record or free record was found
func (F *FileHashMap) getBucketRecordToUpdate(bucket model.Bucket, recordId []byte) (record model.Record, err error) {

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

	err = NoRecordFound{}
	return
}

// getOverflowRecordToUpdate - Searches the overflow for the bucket for a matching record to return.
// It returns:
//   - record is either a record to update or the linking record if no match, the latter comes together with an error of type fhmerrors.NoRecordFound.
//   - err is either of type fhmerrors.NoRecordFound or a standard error if something went wrong
func (F *FileHashMap) getOverflowRecordToUpdate(iter *OverflowRecords, key []byte) (record model.Record, err error) {
	var hasAvailable bool
	var availableRecord model.Record
	for iter.hasNext() {
		record, err = iter.next()
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
		err = NoRecordFound{}
	}

	return
}
