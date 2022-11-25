package lpres

import (
	"fmt"
	"github.com/gostonefire/filehashmap/crt"
	hashfunc "github.com/gostonefire/filehashmap/interfaces"
	"github.com/gostonefire/filehashmap/internal/hash"
	"github.com/gostonefire/filehashmap/internal/model"
	"github.com/gostonefire/filehashmap/internal/overflow"
	"github.com/gostonefire/filehashmap/internal/storage"
	"os"
)

// LPFiles - Represents an implementation of file support for the Linear Probing Collision Resolution Technique.
// It uses one file of buckets where each bucket represents a record. In case of a collision, it probes through
// the hash table linearly, looking for an empty slot, and assigns the free slot to the value. Once all free slots are
// occupied the table will accept no more records.
type LPFiles struct {
	mapFileName              string
	mapFile                  *os.File
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

// NewLPFiles - Returns a pointer to a new instance of Linear Probing file implementation.
// It always creates a new file (or opens and truncate existing file)
//   - crtConf is a model.CRTConf struct providing configuration parameter affecting files creation and processing
//
// It returns:
//   - lpFiles which is a pointer to the created instance
//   - err which is a standard Go type of error
func NewLPFiles(crtConf model.CRTConf) (lpFiles *LPFiles, err error) {
	// If no HashAlgorithm was given then use the default internal
	var internalAlg bool
	if crtConf.HashAlgorithm == nil {
		crtConf.HashAlgorithm = hash.NewSingleHashAlgorithm(crtConf.NumberOfBucketsNeeded)
		internalAlg = true
	}

	// Calculate the hash map file various parameters
	bucketLength := crtConf.KeyLength + crtConf.ValueLength + stateBytes
	minBucketNo, maxBucketNo := crtConf.HashAlgorithm.RangeHashFunc1()
	numberOfBuckets := maxBucketNo - minBucketNo + 1
	fileSize := bucketLength*numberOfBuckets + storage.MapFileHeaderLength

	lpFiles = &LPFiles{
		mapFileName:              storage.GetMapFileName(crtConf.Name),
		keyLength:                crtConf.KeyLength,
		valueLength:              crtConf.ValueLength,
		numberOfBucketsNeeded:    crtConf.NumberOfBucketsNeeded,
		numberOfBucketsAvailable: numberOfBuckets,
		minBucketNo:              minBucketNo,
		maxBucketNo:              maxBucketNo,
		mapFileSize:              fileSize,
		hashAlgorithm:            crtConf.HashAlgorithm,
		internalAlgorithm:        internalAlg,
	}

	header := storage.Header{
		InternalHash:                 internalAlg,
		KeyLength:                    crtConf.KeyLength,
		ValueLength:                  crtConf.ValueLength,
		NumberOfBucketsNeeded:        crtConf.NumberOfBucketsNeeded,
		NumberOfBucketsAvailable:     numberOfBuckets,
		MinBucketNo:                  minBucketNo,
		MaxBucketNo:                  maxBucketNo,
		FileSize:                     fileSize,
		CollisionResolutionTechnique: int64(crt.LinearProbing),
	}

	err = lpFiles.createNewHashMapFile(header)
	if err != nil {
		return
	}

	return
}

// NewLPFilesFromExistingFiles - Returns a pointer to a new instance of Linear Probing file implementation given
// existing files. If files doesn't exist, doesn't have a valid header or if its file size seems wrong given
// size from header it fails with error.
//   - Name is the name to base map file name on
//
// It returns:
//   - lpFiles which is a pointer to the created instance
//   - err which is a standard Go type of error
func NewLPFilesFromExistingFiles(name string, hashAlgorithm hashfunc.HashAlgorithm) (lpFiles *LPFiles, err error) {
	mapFileName := storage.GetMapFileName(name)

	lpFiles = &LPFiles{mapFileName: mapFileName}

	header, err := lpFiles.openHashMapFile()
	if err != nil {
		return
	}

	// Check for mismatch in choice of hash algorithm
	if header.InternalHash && hashAlgorithm != nil {
		lpFiles.CloseFiles()
		err = fmt.Errorf("seems the hash map file was used with the internal hash algorithm but an external was given")
		return
	}
	if !header.InternalHash && hashAlgorithm == nil {
		lpFiles.CloseFiles()
		err = fmt.Errorf("seems the hash map file was used with the external hash algorithm but no external was given")
		return
	}

	// If no HashAlgorithm was given then use the default internal
	var internalAlg bool
	if hashAlgorithm == nil {
		hashAlgorithm = hash.NewSingleHashAlgorithm(header.NumberOfBucketsAvailable)
		internalAlg = true
	}

	lpFiles.keyLength = header.KeyLength
	lpFiles.valueLength = header.ValueLength
	lpFiles.numberOfBucketsNeeded = header.NumberOfBucketsNeeded
	lpFiles.numberOfBucketsAvailable = header.NumberOfBucketsAvailable
	lpFiles.minBucketNo = header.MinBucketNo
	lpFiles.maxBucketNo = header.MaxBucketNo
	lpFiles.mapFileSize = header.FileSize
	lpFiles.hashAlgorithm = hashAlgorithm
	lpFiles.internalAlgorithm = internalAlg

	return
}

// CloseFiles - Closes the map files
func (L *LPFiles) CloseFiles() {
	if L.mapFile != nil {
		_ = L.mapFile.Sync()
		_ = L.mapFile.Close()
	}
}

// RemoveFiles - Removes the map files, make sure to close them first before calling this function
func (L *LPFiles) RemoveFiles() (err error) {
	// Only try to remove if exists, and are not by accident directories (could happen when testing things out)
	if stat, ok := os.Stat(L.mapFileName); ok == nil {
		if !stat.IsDir() {
			err = os.Remove(L.mapFileName)
			if err != nil {
				err = fmt.Errorf("error while removing map file: %s", err)
				return
			}
		}
	}

	return
}

// GetStorageParameters - Returns a struct with storage parameters from SCFiles
func (L *LPFiles) GetStorageParameters() (params model.StorageParameters) {
	params = model.StorageParameters{
		CollisionResolutionTechnique: crt.LinearProbing,
		KeyLength:                    L.keyLength,
		ValueLength:                  L.valueLength,
		NumberOfBucketsNeeded:        L.numberOfBucketsNeeded,
		NumberOfBucketsAvailable:     L.numberOfBucketsAvailable,
		MapFileSize:                  L.mapFileSize,
		InternalAlgorithm:            L.internalAlgorithm,
	}

	return
}

// OverflowRecords - Is used to iterate over overflow records one by one.
// In the implementation of Linear Probing Collision Resolution Technique, overflow is not used so OverflowRecords is
// provided only to conform with interface in the GetBucket method.
type OverflowRecords struct{}

// GetBucket - Returns a bucket with its records given the bucket number
//   - bucketNo is the identifier of a bucket, the number can be retrieved by call to getBucketNo
//
// It returns:
//   - bucket is a model.Bucket struct containing all records in the map file
//   - overflowIterator is a OverflowRecords struct that can be used to get any overflow records belonging to the bucket. This will always be nil in Linear Probing.
//   - err is standard error
func (L *LPFiles) GetBucket(bucketNo int64) (bucket model.Bucket, overflowIterator *overflow.Records, err error) {
	// Get current contents from within the bucket
	bucket, err = L.getBucketRecord(bucketNo)
	if err != nil {
		err = fmt.Errorf("error while getting existing bucket records from hash map file: %s", err)
		return
	}

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
func (L *LPFiles) Get(keyRecord model.Record) (record model.Record, err error) {
	// Check validity of the key
	if int64(len(keyRecord.Key)) != L.keyLength {
		err = fmt.Errorf("wrong length of key, should be %d", L.keyLength)
		return
	}

	// Get current contents from within the bucket
	bucketNo, err := L.getBucketNo(keyRecord.Key)
	if err != nil {
		return
	}

	// Tro to find the key in the file
	record, err = L.linearProbingForGet(bucketNo, keyRecord.Key)

	return
}

// Set - Updates an existing record with new data or add it if no existing is found with same key.
//   - record is the record to set, it needs only to contain Key and Value, and they have to conform to lengths given when creating the SCFiles
//
// It returns:
//   - err is a standard error, if something went wrong
func (L *LPFiles) Set(record model.Record) (err error) {
	// Check validity of the key
	if int64(len(record.Key)) != L.keyLength {
		err = fmt.Errorf("wrong length of key, should be %d", L.keyLength)
		return
	}
	// Check validity of the value
	if int64(len(record.Value)) != L.valueLength {
		err = fmt.Errorf("wrong length of value, should be %d", L.valueLength)
		return
	}

	// Get current contents from within the bucket
	bucketNo, err := L.getBucketNo(record.Key)
	if err != nil {
		return
	}

	selectedRecord, err := L.linearProbingForSet(bucketNo, record.Key)
	if err != nil {
		return
	}

	selectedRecord.State = model.RecordOccupied
	selectedRecord.Key = record.Key
	selectedRecord.Value = record.Value

	err = L.setBucketRecord(selectedRecord)
	if err != nil {
		err = fmt.Errorf("error while updating or adding record to bucket: %s", err)
	}

	return
}

// Delete - Deletes a record by setting state to RecordDeleted
//   - record is the model.Record to mark as deleted, and it must contain RecordAddress
//
// It returns:
//   - err is a standard error, if something went wrong
func (L *LPFiles) Delete(record model.Record) (err error) {
	record.State = model.RecordDeleted
	record.Key = make([]byte, L.keyLength)
	record.Value = make([]byte, L.valueLength)

	err = L.setBucketRecord(record)

	return
}
