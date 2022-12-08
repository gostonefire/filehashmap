package separatechaining

import (
	"fmt"
	"github.com/gostonefire/filehashmap/crt"
	"github.com/gostonefire/filehashmap/hashfunc"
	"github.com/gostonefire/filehashmap/internal/hash"
	"github.com/gostonefire/filehashmap/internal/model"
	"github.com/gostonefire/filehashmap/internal/overflow"
	"github.com/gostonefire/filehashmap/internal/storage"
	"github.com/gostonefire/filehashmap/internal/utils"
	"os"
)

// SCFiles - Represents an implementation of file support for the Separate Chaining Collision Resolution Technique.
// It uses two files in this particular implementation where one stores directly addressable buckets and the
// other manages overflow in single linked lists.
type SCFiles struct {
	mapFileName              string
	ovflFileName             string
	mapFile                  *os.File
	ovflFile                 *os.File
	keyLength                int64
	valueLength              int64
	numberOfBucketsNeeded    int64
	numberOfBucketsAvailable int64
	minBucketNo              int64
	maxBucketNo              int64
	mapFileSize              int64
	hashAlgorithm            hashfunc.HashAlgorithm
	internalAlgorithm        bool
}

// NewSCFiles - Returns a pointer to a new instance of Separate Chaining file implementation.
// It always creates new files (or opens and truncate existing files)
//   - crtConf is a model.CRTConf struct providing configuration parameter affecting files creation and processing
//
// It returns:
//   - scFiles which is a pointer to the created instance
//   - err which is a standard Go type of error
func NewSCFiles(crtConf model.CRTConf) (scFiles *SCFiles, err error) {
	// If no HashAlgorithm was given then use the default internal
	var internalAlg bool
	if crtConf.HashAlgorithm == nil {
		crtConf.HashAlgorithm = hash.NewSeparateChainingHashAlgorithm(crtConf.NumberOfBucketsNeeded)
		internalAlg = true
	} else {
		crtConf.HashAlgorithm.SetTableSize(crtConf.NumberOfBucketsNeeded)
	}

	// Calculate the hash map file various parameters
	bucketLength := bucketHeaderLength + 1 + crtConf.KeyLength + crtConf.ValueLength // First byte is record state
	maxBucketNo := crtConf.HashAlgorithm.GetTableSize() - 1
	numberOfBuckets := maxBucketNo + 1
	fileSize := bucketLength*numberOfBuckets + storage.MapFileHeaderLength

	scFiles = &SCFiles{
		mapFileName:              storage.GetMapFileName(crtConf.Name),
		ovflFileName:             storage.GetOvflFileName(crtConf.Name),
		keyLength:                crtConf.KeyLength,
		valueLength:              crtConf.ValueLength,
		numberOfBucketsNeeded:    crtConf.NumberOfBucketsNeeded,
		numberOfBucketsAvailable: numberOfBuckets,
		maxBucketNo:              maxBucketNo,
		mapFileSize:              fileSize,
		hashAlgorithm:            crtConf.HashAlgorithm,
		internalAlgorithm:        internalAlg,
	}

	header := scFiles.createHeader()

	err = scFiles.createNewHashMapFile(header)
	if err != nil {
		return
	}
	err = scFiles.createNewOverflowFile()
	if err != nil {
		return
	}

	return
}

// NewSCFilesFromExistingFiles - Returns a pointer to a new instance of Separate Chaining file implementation given
// existing files. If files doesn't exist, doesn't have a valid header or if its file size seems wrong given
// size from header it fails with error.
//   - Name is the name to base map and overflow file names on
//
// It returns:
//   - scFiles which is a pointer to the created instance
//   - err which is a standard Go type of error
func NewSCFilesFromExistingFiles(name string, hashAlgorithm hashfunc.HashAlgorithm) (scFiles *SCFiles, err error) {
	mapFileName := storage.GetMapFileName(name)
	ovflFileName := storage.GetOvflFileName(name)

	scFiles = &SCFiles{mapFileName: mapFileName, ovflFileName: ovflFileName}

	header, err := scFiles.openHashMapFile()
	if err != nil {
		return
	}
	err = scFiles.openOverflowFile()
	if err != nil {
		return
	}

	// Check for mismatch in choice of hash algorithm
	if header.InternalHash && hashAlgorithm != nil {
		scFiles.CloseFiles()
		err = fmt.Errorf("seems the hash map file was used with the internal hash algorithm but an external was given")
		return
	}
	if !header.InternalHash && hashAlgorithm == nil {
		scFiles.CloseFiles()
		err = fmt.Errorf("seems the hash map file was used with the external hash algorithm but no external was given")
		return
	}

	// If no HashAlgorithm was given then use the default internal
	var internalAlg bool
	if hashAlgorithm == nil {
		hashAlgorithm = hash.NewSeparateChainingHashAlgorithm(header.NumberOfBucketsNeeded)
		internalAlg = true
	} else {
		hashAlgorithm.SetTableSize(header.NumberOfBucketsNeeded)
	}

	//scFiles.initialUniqueKeys = header.NumberOfBucketsNeeded
	scFiles.keyLength = header.KeyLength
	scFiles.valueLength = header.ValueLength
	scFiles.numberOfBucketsNeeded = header.NumberOfBucketsNeeded
	scFiles.numberOfBucketsAvailable = header.NumberOfBucketsAvailable
	scFiles.maxBucketNo = header.MaxBucketNo
	scFiles.mapFileSize = header.FileSize
	scFiles.hashAlgorithm = hashAlgorithm
	scFiles.internalAlgorithm = internalAlg

	return
}

// CloseFiles - Closes the map files
func (S *SCFiles) CloseFiles() {
	if S.ovflFile != nil {
		_ = S.ovflFile.Sync()
		_ = S.ovflFile.Close()
	}

	if S.mapFile != nil {
		_ = S.mapFile.Sync()
		_ = S.mapFile.Close()
	}
}

// RemoveFiles - Removes the map files, make sure to close them first before calling this function
func (S *SCFiles) RemoveFiles() (err error) {
	// Only try to remove if exists, and are not by accident directories (could happen when testing things out)
	if stat, ok := os.Stat(S.ovflFileName); ok == nil {
		if !stat.IsDir() {
			err = os.Remove(S.ovflFileName)
			if err != nil {
				err = fmt.Errorf("error while removing overflow file: %s", err)
				return
			}
		}
	}
	if stat, ok := os.Stat(S.mapFileName); ok == nil {
		if !stat.IsDir() {
			err = os.Remove(S.mapFileName)
			if err != nil {
				err = fmt.Errorf("error while removing map file: %s", err)
				return
			}
		}
	}

	return
}

// GetStorageParameters - Returns a struct with storage parameters from SCFiles
func (S *SCFiles) GetStorageParameters() (params model.StorageParameters) {
	params = model.StorageParameters{
		CollisionResolutionTechnique: crt.SeparateChaining,
		KeyLength:                    S.keyLength,
		ValueLength:                  S.valueLength,
		NumberOfBucketsNeeded:        S.numberOfBucketsNeeded,
		NumberOfBucketsAvailable:     S.numberOfBucketsAvailable,
		MapFileSize:                  S.mapFileSize,
		InternalAlgorithm:            S.internalAlgorithm,
	}

	return
}

// GetBucket - Returns a bucket with its records given the bucket number
//   - bucketNo is the identifier of a bucket, the number can be retrieved by call to getBucketNo
//
// It returns:
//   - bucket is a model.Bucket struct containing all records in the map file
//   - overflowIterator is a Record struct that can be used to get any overflow records belonging to the bucket.
//   - err is standard error
func (S *SCFiles) GetBucket(bucketNo int64) (bucket model.Bucket, overflowIterator *overflow.Records, err error) {
	// Get current contents from within the bucket
	bucket, err = S.getBucketRecord(bucketNo)
	if err != nil {
		err = fmt.Errorf("error while getting existing bucket records from hash map file: %s", err)
		return
	}

	getOvflFunc := func(recordAddress int64) (model.Record, error) { return S.getOverflowRecord(recordAddress) }
	overflowIterator = overflow.NewRecords(getOvflFunc, bucket.OverflowAddress)

	return
}

// Get - Gets record that corresponds to the given key.
// The model.Record that is returned contains also addresses to the actual files that it came from, this is to speed
// up higher levels functions such as Pop where the same record is also supposed to be deleted in a call to Delete
//   - keyRecord is the identifier of a record, it has to have the Key set and with the same length as given in call to NewFileHashMap
//
// It returns:
//   - record is the value of the matching record if found, if not found an error of type crt.NoRecordFound is also returned.
//   - err is either of type crt.NoRecordFound or a standard error, if something went wrong
func (S *SCFiles) Get(keyRecord model.Record) (record model.Record, err error) {
	// Check validity of the key
	if int64(len(keyRecord.Key)) != S.keyLength {
		err = fmt.Errorf("wrong length of key, should be %d", S.keyLength)
		return
	}

	// Get current contents from within the bucket
	bucketNo, err := S.getBucketNo(keyRecord.Key)
	if err != nil {
		return
	}
	bucket, ovflIter, err := S.GetBucket(bucketNo)
	if err != nil {
		return
	}

	// Sort out record with correct key
	if bucket.Record.State == model.RecordOccupied && utils.IsEqual(keyRecord.Key, bucket.Record.Key) {
		record = bucket.Record
		return
	}

	// Check if record may be in overflow file
	for ovflIter.HasNext() {
		record, err = ovflIter.Next()
		if err != nil {
			return
		}
		if record.State == model.RecordOccupied && utils.IsEqual(keyRecord.Key, record.Key) {
			return
		}
	}

	record = model.Record{}
	err = crt.NoRecordFound{}

	return
}

// Set - Updates an existing record with new data or add it if no existing is found with same key.
//   - record is the record to set, it needs only to contain Key and Value, and they have to conform to lengths given when creating the SCFiles
//
// It returns:
//   - err is a standard error, if something went wrong
func (S *SCFiles) Set(record model.Record) (err error) {
	// Check validity of the key
	if int64(len(record.Key)) != S.keyLength {
		err = fmt.Errorf("wrong length of key, should be %d", S.keyLength)
		return
	}
	// Check validity of the value
	if int64(len(record.Value)) != S.valueLength {
		err = fmt.Errorf("wrong length of value, should be %d", S.valueLength)
		return
	}

	// Get current contents from within the bucket
	bucketNo, err := S.getBucketNo(record.Key)
	if err != nil {
		return
	}
	bucket, ovflIter, err := S.GetBucket(bucketNo)
	if err != nil {
		return
	}

	// First check if there is a record to update in the map file bucket, if there is or if the bucket record is
	// empty (never used) then we now that we can set the record and avoid searching in overflow file.
	// If we have a deleted record then save that for potential later use, but we have to search in overflow file as well.
	var hasDeleted bool
	var deletedRecord, ovflRecord model.Record
	if (bucket.Record.State == model.RecordOccupied && utils.IsEqual(record.Key, bucket.Record.Key)) || bucket.Record.State == model.RecordEmpty {
		bucket.Record.State = model.RecordOccupied
		bucket.Record.Key = record.Key
		bucket.Record.Value = record.Value
		err = S.setBucketRecord(bucket.Record)
		if err != nil {
			err = fmt.Errorf("error while updating or adding record to bucket or overflow: %s", err)
		}
		return
	} else if bucket.Record.State == model.RecordDeleted {
		hasDeleted = true
		deletedRecord = bucket.Record
	}

	// Search through all overflow records until we find a matching record, in the process save first deleted record for
	// potential later use (unless we already have a deleted record from the bucket file).
	// If we have no match in overflow records we have to continue our search for best option.
	for ovflIter.HasNext() {
		ovflRecord, err = ovflIter.Next()
		if err != nil {
			err = fmt.Errorf("error while updating or adding record to bucket or overflow: %s", err)
			return
		}
		if ovflRecord.State == model.RecordOccupied && utils.IsEqual(ovflRecord.Key, record.Key) {
			ovflRecord.Key = record.Key
			ovflRecord.Value = record.Value
			err = S.setOverflowRecord(ovflRecord)
			if err != nil {
				err = fmt.Errorf("error while updating or adding record to bucket or overflow: %s", err)
			}
			return
		} else if !hasDeleted && ovflRecord.State == model.RecordDeleted {
			hasDeleted = true
			deletedRecord = ovflRecord
		}
	}

	// Having come to this part we didn't find any matching record, so set our new record in an available (deleted) spot
	// if such was found earlier.
	if hasDeleted {
		deletedRecord.State = model.RecordOccupied
		deletedRecord.Key = record.Key
		deletedRecord.Value = record.Value
		if deletedRecord.IsOverflow {
			err = S.setOverflowRecord(deletedRecord)
			if err != nil {
				err = fmt.Errorf("error while updating or adding record to bucket or overflow: %s", err)
			}
		} else {
			err = S.setBucketRecord(deletedRecord)
			if err != nil {
				err = fmt.Errorf("error while updating or adding record to bucket or overflow: %s", err)
			}
		}
		return
	}

	// There was no available (deleted) record to use, so now we will either append (link) a new record in overflow file.
	// Or if the bucket has no overflow since earlier, create a new overflow for it and update the bucket accordingly.
	if ovflRecord.IsOverflow {
		err = S.appendOverflowRecord(ovflRecord, record.Key, record.Value)
		if err != nil {
			err = fmt.Errorf("error while updating or adding record to bucket or overflow: %s", err)
		}
		return
	} else {
		var overflowAddress int64
		overflowAddress, err = S.newBucketOverflow(record.Key, record.Value)
		if err != nil {
			return
		}
		err = S.setBucketOverflowAddress(bucket.BucketAddress, overflowAddress)
		if err != nil {
			return
		}
	}

	if err != nil {
		err = fmt.Errorf("error while updating or adding record to bucket or overflow: %s", err)
	}

	return
}

// Delete - Deletes a record by setting it to in use is false
//   - record is the model.Record to mark as deleted, and it must contain IsOverflow, RecordAddress and NextOverflow
//
// It returns:
//   - err is a standard error, if something went wrong
func (S *SCFiles) Delete(record model.Record) (err error) {
	record.State = model.RecordDeleted
	record.Key = make([]byte, S.keyLength)
	record.Value = make([]byte, S.valueLength)

	if record.IsOverflow {
		err = S.setOverflowRecord(record)
		if err != nil {
			err = fmt.Errorf("error while updating record in overflow: %s", err)
		}
	} else {
		err = S.setBucketRecord(record)
		if err != nil {
			err = fmt.Errorf("error while updating record in bucket: %s", err)
		}
	}

	return
}
