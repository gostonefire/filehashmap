package filehashmap

import (
	"github.com/gostonefire/filehashmap/internal/model"
	"github.com/gostonefire/filehashmap/internal/overflow"
)

// Get - Gets record that corresponds to the given recordId.
//   - key is the identifier of a record, it has to be of same length as given in call to NewFileHashMap
//
// It returns:
//   - value is the value of the matching record if found, if not found an error of type crt.NoRecordFound is also returned.
//   - err is either of type crt.NoRecordFound or a standard error, if something went wrong
func (F *FileHashMap) Get(key []byte) (value []byte, err error) {
	record, err := F.fileManagement.Get(model.Record{Key: key})
	if err != nil {
		return
	}

	value = record.Value

	return
}

// Set - Updates an existing record with new data or add it if no existing is found with same key.
//   - key is the identifier of a record, it has to be of same length as given in call to NewFileHashMap
//   - value is the bytes to be written to the bucket along with its key, length must be as was given in call to NewFileHashMap
//
// It returns:
//   - err is a standard error, if something went wrong
func (F *FileHashMap) Set(key []byte, value []byte) (err error) {
	err = F.fileManagement.Set(model.Record{Key: key, Value: value})

	return
}

// Pop - Returns the record corresponding to key and removes it from the file hash map.
//   - key is the identifier of a record, it has to be of same length as given in call to NewFileHashMap
//
// It returns:
//   - value is the value of the matching record if found, if not found an error of type crt.NoRecordFound is also returned.
//   - err is either of type crt.NoRecordFound or a standard error, if something went wrong
func (F *FileHashMap) Pop(key []byte) (value []byte, err error) {
	record, err := F.fileManagement.Get(model.Record{Key: key})
	if err != nil {
		return
	}

	err = F.fileManagement.Delete(
		model.Record{
			IsOverflow:    record.IsOverflow,
			RecordAddress: record.RecordAddress,
			NextOverflow:  record.NextOverflow,
		})

	value = record.Value

	return
}

// Stat - Walks through the entire set of buckets and produce a HashMapStat struct with information.
// If the hash map file and overflow file are very big, this can take a considerable amount of time and
// the HashMapStat.BucketDistribution slice can be very memory heavy (there will be one entry per bucket).
//   - includeDistribution set to true will include a slice of length numberOfBuckets with number of records per bucket, false will set HashMapStat.BucketDistribution to nil.
func (F *FileHashMap) Stat(includeDistribution bool) (hashMapStat *HashMapStat, err error) {
	var bucket model.Bucket
	var record model.Record
	var iter *overflow.Records
	var hms HashMapStat

	sp := F.fileManagement.GetStorageParameters()

	if includeDistribution {
		hms.BucketDistribution = make([]int, sp.NumberOfBucketsAvailable)
	}

	// Iterate over every available bucket
	for i := int64(0); i < sp.NumberOfBucketsAvailable; i++ {
		bucket, iter, err = F.fileManagement.GetBucket(i)
		if err != nil {
			return
		}

		// Process map file records
		for _, r := range bucket.Records {
			if r.State == model.RecordOccupied {
				hms.Records++
				hms.MapFileRecords++
				if includeDistribution {
					hms.BucketDistribution[i]++
				}
			}

		}

		// Process overflow file records
		for iter != nil && iter.HasNext() {
			record, err = iter.Next()
			if err != nil {
				return
			}
			if record.State == model.RecordOccupied {
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
