package scres

import (
	"errors"
	"fmt"
	hashfunc "github.com/gostonefire/filehashmap/interfaces"
	"github.com/gostonefire/filehashmap/internal/hash"
	"github.com/gostonefire/filehashmap/internal/model"
	"github.com/gostonefire/filehashmap/internal/utils"
	"github.com/gostonefire/filehashmap/storage"
	"math"
	"os"
)

// SCFilesConf - Is a struct to be passed in the call to NewSCFiles and contains configuration that affects
// file processing.
//   - Name is the name to base map and overflow file names on
//   - KeyLength is the fixed length of keys to store
//   - ValueLength is the fixed length of values to store
//   - HashAlgorithm is the hash function(s) to use
type SCFilesConf struct {
	Name              string
	InitialUniqueKeys int64
	KeyLength         int64
	ValueLength       int64
	HashAlgorithm     hashfunc.HashAlgorithm
}

// SCFiles - Represents an implementation of file support for the Separate Chaining Collision Resolution Technique.
// It uses two files in this particular implementation where one stores directly addressable buckets and the
// other manages overflow in single linked lists.
type SCFiles struct {
	mapFileName       string
	ovflFileName      string
	mapFile           *os.File
	ovflFile          *os.File
	initialUniqueKeys int64
	keyLength         int64
	valueLength       int64
	recordsPerBucket  int64
	numberOfBuckets   int64
	fillFactor        float64
	minBucketNo       int64
	maxBucketNo       int64
	mapFileSize       int64
	hashAlgorithm     hashfunc.HashAlgorithm
	internalAlgorithm bool
}

// NewSCFiles - Returns a pointer to a new instance of Separate Chaining file implementation.
// It always creates new files (or opens and truncate existing files)
//   - scFilesConf is a SCFilesConf struct providing configuration parameter affecting files creation and processing
//
// It returns:
//   - scFiles which is a pointer to the created instance
//   - err which is a standard Go type of error
func NewSCFiles(scFilesConf SCFilesConf) (scFiles *SCFiles, err error) {
	// If no HashAlgorithm was given then use the default internal
	var internalAlg bool
	if scFilesConf.HashAlgorithm == nil {
		scFilesConf.HashAlgorithm = hash.NewBucketAlgorithm(scFilesConf.InitialUniqueKeys)
		internalAlg = true
	}

	// Calculate the hash map file various parameters
	trueRecordLength := scFilesConf.KeyLength + scFilesConf.ValueLength + inUseFlagBytes
	minBucketNo, maxBucketNo := scFilesConf.HashAlgorithm.BucketNumberRange()
	numberOfBuckets := maxBucketNo - minBucketNo + 1
	recordsPerBucket := int64(math.Ceil(float64(scFilesConf.InitialUniqueKeys) / float64(numberOfBuckets)))
	bucketLength := trueRecordLength*recordsPerBucket + bucketHeaderLength
	fileSize := bucketLength*numberOfBuckets + mapFileHeaderLength
	fillFactor := float64(scFilesConf.InitialUniqueKeys) / float64(recordsPerBucket*numberOfBuckets)

	scFiles = &SCFiles{
		mapFileName:       fmt.Sprintf("%s-map.bin", scFilesConf.Name),
		ovflFileName:      fmt.Sprintf("%s-ovfl.bin", scFilesConf.Name),
		initialUniqueKeys: scFilesConf.InitialUniqueKeys,
		keyLength:         scFilesConf.KeyLength,
		valueLength:       scFilesConf.ValueLength,
		recordsPerBucket:  recordsPerBucket,
		numberOfBuckets:   numberOfBuckets,
		fillFactor:        fillFactor,
		minBucketNo:       minBucketNo,
		maxBucketNo:       maxBucketNo,
		mapFileSize:       fileSize,
		hashAlgorithm:     scFilesConf.HashAlgorithm,
		internalAlgorithm: internalAlg,
	}

	header := model.Header{
		InternalAlg:       internalAlg,
		InitialUniqueKeys: scFilesConf.InitialUniqueKeys,
		KeyLength:         scFilesConf.KeyLength,
		ValueLength:       scFilesConf.ValueLength,
		RecordsPerBucket:  recordsPerBucket,
		NumberOfBuckets:   numberOfBuckets,
		MinBucketNo:       minBucketNo,
		MaxBucketNo:       maxBucketNo,
		FileSize:          fileSize,
	}

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
	mapFileName := fmt.Sprintf("%s-map.bin", name)
	ovflFileName := fmt.Sprintf("%s-ovfl.bin", name)

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
	if header.InternalAlg && hashAlgorithm != nil {
		scFiles.CloseFiles()
		err = fmt.Errorf("seems the hash map file was used with the internal hash algorithm but an external was given")
		return
	}
	if !header.InternalAlg && hashAlgorithm == nil {
		scFiles.CloseFiles()
		err = fmt.Errorf("seems the hash map file was used with the external hash algorithm but no external was given")
		return
	}

	// If no HashAlgorithm was given then use the default internal
	var internalAlg bool
	if hashAlgorithm == nil {
		hashAlgorithm = hash.NewBucketAlgorithm(header.InitialUniqueKeys)
		internalAlg = true
	}

	scFiles.initialUniqueKeys = header.InitialUniqueKeys
	scFiles.keyLength = header.KeyLength
	scFiles.valueLength = header.ValueLength
	scFiles.recordsPerBucket = header.RecordsPerBucket
	scFiles.numberOfBuckets = header.NumberOfBuckets
	scFiles.fillFactor = float64(scFiles.initialUniqueKeys) / float64(scFiles.recordsPerBucket*scFiles.numberOfBuckets)
	scFiles.minBucketNo = header.MinBucketNo
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
		InitialUniqueKeys: S.initialUniqueKeys,
		KeyLength:         S.keyLength,
		ValueLength:       S.valueLength,
		RecordsPerBucket:  S.recordsPerBucket,
		NumberOfBuckets:   S.numberOfBuckets,
		FillFactor:        S.fillFactor,
		MapFileSize:       S.mapFileSize,
		InternalAlgorithm: S.internalAlgorithm,
	}

	return
}

// GetBucket - Returns a bucket with its records given the bucket number
//   - bucketNo is the identifier of a bucket, the number can be retrieved by call to getBucketNo
//
// It returns:
//   - bucket is a model.Bucket struct containing all records in the map file
//   - overflowIterator is a OverflowRecords struct that can be used to get any overflow records belonging to the bucket.
//   - err is standard error
func (S *SCFiles) GetBucket(bucketNo int64) (bucket model.Bucket, overflowIterator *OverflowRecords, err error) {
	// Get current contents from within the bucket
	bucket, err = S.getBucketRecords(bucketNo)
	if err != nil {
		err = fmt.Errorf("error while getting existing bucket records from hash map file: %s", err)
		return
	}

	overflowIterator = newOverflowRecords(S, bucket.OverflowAddress)

	return
}

// Get - Gets record that corresponds to the given key.
// The model.Record that is returned contains also addresses to the actual files that it came from, this is to speed
// up higher levels functions such as Pop where the same record is also supposed to be deleted in a call to Delete
//   - keyRecord is the identifier of a record, it has to have the Key set and with the same length as given in call to NewFileHashMap
//
// It returns:
//   - record is the value of the matching record if found, if not found an error of type storage.NoRecordFound is also returned.
//   - err is either of type storage.NoRecordFound or a standard error, if something went wrong
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
	for _, record = range bucket.Records {
		if record.InUse && utils.IsEqual(keyRecord.Key, record.Key) {
			return
		}
	}

	// Check if record may be in overflow file
	for ovflIter.HasNext() {
		record, err = ovflIter.Next()
		if err != nil {
			return
		}
		if record.InUse && utils.IsEqual(keyRecord.Key, record.Key) {
			return
		}
	}

	record = model.Record{}
	err = storage.NoRecordFound{}

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

	// Try to find an existing record with matching ID, or add to overflow
	var r model.Record
	r, err = S.getBucketRecordToUpdate(bucket, record.Key)
	if err == nil {
		r.InUse = true
		r.Key = record.Key
		r.Value = record.Value
		err = S.setBucketRecord(r)
	} else if errors.Is(err, storage.NoRecordFound{}) {
		if ovflIter.HasNext() {
			r, err = S.getOverflowRecordToUpdate(ovflIter, record.Key)
			if err == nil {
				r.InUse = true
				r.Key = record.Key
				r.Value = record.Value
				err = S.setOverflowRecord(r)
			} else if errors.Is(err, storage.NoRecordFound{}) {
				err = S.appendOverflowRecord(r, record.Key, record.Value)
			}
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
	record.InUse = false
	record.Key = make([]byte, S.keyLength)
	record.Value = make([]byte, S.valueLength)

	if record.IsOverflow {
		err = S.setOverflowRecord(record)
	} else {
		err = S.setBucketRecord(record)
	}

	return
}
