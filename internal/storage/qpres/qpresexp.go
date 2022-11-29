package qpres

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

// QPFiles - Represents an implementation of file support for the Quadratic Probing Collision Resolution Technique.
// It uses one file of buckets where each bucket represents a record. In case of a collision, it probes through
// the hash table using a quadratic algorithm, looking for an empty slot, and assigns the free slot to the value.
// Once all free slots are occupied the table will accept no more records.
type QPFiles struct {
	mapFileName              string
	mapFile                  *os.File
	keyLength                int64
	valueLength              int64
	numberOfBucketsNeeded    int64
	numberOfBucketsAvailable int64
	maxBucketNo              int64
	mapFileSize              int64
	hashAlgorithm            hashfunc.HashAlgorithm
	internalAlgorithm        bool
	roundUp2                 int64
	nEmpty                   int64
	nOccupied                int64
	nDeleted                 int64
}

// NewQPFiles - Returns a pointer to a new instance of Quadratic Probing file implementation.
// It always creates a new file (or opens and truncate existing file)
//   - crtConf is a model.CRTConf struct providing configuration parameter affecting files creation and processing
//
// It returns:
//   - qpFiles which is a pointer to the created instance
//   - err which is a standard Go type of error
func NewQPFiles(crtConf model.CRTConf) (qpFiles *QPFiles, err error) {
	// If no HashAlgorithm was given then use the default internal
	var internalAlg bool
	if crtConf.HashAlgorithm == nil {
		crtConf.HashAlgorithm = hash.NewSingleHashAlgorithm(crtConf.NumberOfBucketsNeeded)
		internalAlg = true
	}

	// Add one extra bucket to be the golden bucket never to be occupied to assist the Quadratic Probing algorithm
	crtConf.HashAlgorithm.UpdateTableSize(1)

	// Calculate the hash map file various parameters
	bucketLength := 1 + crtConf.KeyLength + crtConf.ValueLength // First byte is record state
	maxBucketNo := crtConf.HashAlgorithm.HashFunc1MaxValue()
	numberOfBuckets := maxBucketNo + 1
	fileSize := bucketLength*numberOfBuckets + storage.MapFileHeaderLength

	// Calculate the round up number of buckets to the closest power of 2, will be used in Quadratic Probing
	r := uint64(numberOfBuckets - 1)
	r |= r >> 1
	r |= r >> 2
	r |= r >> 4
	r |= r >> 8
	r |= r >> 16
	r |= r >> 32
	roundUp2 := int64(r + 1)

	qpFiles = &QPFiles{
		mapFileName:              storage.GetMapFileName(crtConf.Name),
		keyLength:                crtConf.KeyLength,
		valueLength:              crtConf.ValueLength,
		numberOfBucketsNeeded:    crtConf.NumberOfBucketsNeeded,
		numberOfBucketsAvailable: numberOfBuckets,
		maxBucketNo:              maxBucketNo,
		mapFileSize:              fileSize,
		hashAlgorithm:            crtConf.HashAlgorithm,
		internalAlgorithm:        internalAlg,
		roundUp2:                 roundUp2,
		nEmpty:                   numberOfBuckets,
		nOccupied:                0,
		nDeleted:                 0,
	}

	header := qpFiles.createHeader()

	err = qpFiles.createNewHashMapFile(header)
	if err != nil {
		return
	}

	return
}

// NewQPFilesFromExistingFiles - Returns a pointer to a new instance of Quadratic Probing file implementation given
// existing files. If files doesn't exist, doesn't have a valid header or if its file size seems wrong given
// size from header it fails with error.
//   - Name is the name to base map file name on
//
// It returns:
//   - qpFiles which is a pointer to the created instance
//   - err which is a standard Go type of error
func NewQPFilesFromExistingFiles(name string, hashAlgorithm hashfunc.HashAlgorithm) (qpFiles *QPFiles, err error) {
	mapFileName := storage.GetMapFileName(name)

	qpFiles = &QPFiles{mapFileName: mapFileName}

	header, err := qpFiles.openHashMapFile()
	if err != nil {
		return
	}

	// Check for mismatch in choice of hash algorithm
	if header.InternalHash && hashAlgorithm != nil {
		qpFiles.CloseFiles()
		err = fmt.Errorf("seems the hash map file was used with the internal hash algorithm but an external was given")
		return
	}
	if !header.InternalHash && hashAlgorithm == nil {
		qpFiles.CloseFiles()
		err = fmt.Errorf("seems the hash map file was used with the external hash algorithm but no external was given")
		return
	}

	// If no HashAlgorithm was given then use the default internal
	var internalAlg bool
	if hashAlgorithm == nil {
		hashAlgorithm = hash.NewSingleHashAlgorithm(header.NumberOfBucketsNeeded)
		internalAlg = true
	}

	// Add one extra bucket to be the golden bucket never to be occupied to assist the Quadratic Probing algorithm
	hashAlgorithm.UpdateTableSize(1)

	// Calculate the round up number of buckets to the closest power of 2, will be used in Quadratic Probing
	r := uint64(header.NumberOfBucketsAvailable - 1)
	r |= r >> 1
	r |= r >> 2
	r |= r >> 4
	r |= r >> 8
	r |= r >> 16
	r |= r >> 32
	roundUp2 := int64(r + 1)

	qpFiles.keyLength = header.KeyLength
	qpFiles.valueLength = header.ValueLength
	qpFiles.numberOfBucketsNeeded = header.NumberOfBucketsNeeded
	qpFiles.numberOfBucketsAvailable = header.NumberOfBucketsAvailable
	qpFiles.maxBucketNo = header.MaxBucketNo
	qpFiles.mapFileSize = header.FileSize
	qpFiles.hashAlgorithm = hashAlgorithm
	qpFiles.internalAlgorithm = internalAlg
	qpFiles.roundUp2 = roundUp2
	qpFiles.nEmpty = header.NumberOfEmptyRecords
	qpFiles.nOccupied = header.NumberOfOccupiedRecords
	qpFiles.nDeleted = header.NumberOfDeletedRecords

	return
}

// CloseFiles - Closes the map files
func (Q *QPFiles) CloseFiles() {
	if Q.mapFile != nil {
		header := Q.createHeader()
		err := storage.SetHeader(Q.mapFile, header)
		if err == nil {
			_ = storage.SetFileCloseDate(Q.mapFile, false)
		}
		_ = Q.mapFile.Sync()
		_ = Q.mapFile.Close()
	}
}

// RemoveFiles - Removes the map files, make sure to close them first before calling this function
func (Q *QPFiles) RemoveFiles() (err error) {
	// Only try to remove if exists, and are not by accident directories (could happen when testing things out)
	if stat, ok := os.Stat(Q.mapFileName); ok == nil {
		if !stat.IsDir() {
			err = os.Remove(Q.mapFileName)
			if err != nil {
				err = fmt.Errorf("error while removing map file: %s", err)
				return
			}
		}
	}

	return
}

// GetStorageParameters - Returns a struct with storage parameters from SCFiles
func (Q *QPFiles) GetStorageParameters() (params model.StorageParameters) {
	params = model.StorageParameters{
		CollisionResolutionTechnique: crt.QuadraticProbing,
		KeyLength:                    Q.keyLength,
		ValueLength:                  Q.valueLength,
		NumberOfBucketsNeeded:        Q.numberOfBucketsNeeded,
		NumberOfBucketsAvailable:     Q.numberOfBucketsAvailable,
		MapFileSize:                  Q.mapFileSize,
		InternalAlgorithm:            Q.internalAlgorithm,
	}

	return
}

// GetBucket - Returns a bucket with its records given the bucket number
//   - bucketNo is the identifier of a bucket, the number can be retrieved by call to getBucketNo
//
// It returns:
//   - bucket is a model.Bucket struct containing all records in the map file
//   - overflowIterator is a OverflowRecords struct that can be used to get any overflow records belonging to the bucket. This will always be nil in Linear Probing.
//   - err is standard error
func (Q *QPFiles) GetBucket(bucketNo int64) (bucket model.Bucket, overflowIterator *overflow.Records, err error) {
	// Get current contents from within the bucket
	bucket, err = Q.getBucketRecord(bucketNo)
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
func (Q *QPFiles) Get(keyRecord model.Record) (record model.Record, err error) {
	// Check validity of the key
	if int64(len(keyRecord.Key)) != Q.keyLength {
		err = fmt.Errorf("wrong length of key, should be %d", Q.keyLength)
		return
	}

	// Get current contents from within the bucket
	bucketNo, err := Q.getBucketNo(keyRecord.Key)
	if err != nil {
		return
	}

	// Tro to find the key in the file
	record, err = Q.quadraticProbingForGet(bucketNo, keyRecord.Key)

	return
}

// Set - Updates an existing record with new data or add it if no existing is found with same key.
//   - record is the record to set, it needs only to contain Key and Value, and they have to conform to lengths given when creating the SCFiles
//
// It returns:
//   - err is a standard error, if something went wrong
func (Q *QPFiles) Set(record model.Record) (err error) {
	// Check validity of the key
	if int64(len(record.Key)) != Q.keyLength {
		err = fmt.Errorf("wrong length of key, should be %d", Q.keyLength)
		return
	}
	// Check validity of the value
	if int64(len(record.Value)) != Q.valueLength {
		err = fmt.Errorf("wrong length of value, should be %d", Q.valueLength)
		return
	}

	// Get current contents from within the bucket
	bucketNo, err := Q.getBucketNo(record.Key)
	if err != nil {
		return
	}

	selectedRecord, err := Q.quadraticProbingForSet(bucketNo, record.Key)
	if err != nil {
		return
	}

	fromState := selectedRecord.State
	selectedRecord.State = model.RecordOccupied
	selectedRecord.Key = record.Key
	selectedRecord.Value = record.Value

	err = Q.setBucketRecord(selectedRecord)
	if err != nil {
		err = fmt.Errorf("error while updating or adding record to bucket: %s", err)
		return
	}

	Q.updateUtilizationInfo(fromState, selectedRecord.State)

	return
}

// Delete - Deletes a record by setting state to RecordDeleted
//   - record is the model.Record to mark as deleted, and it must contain RecordAddress
//
// It returns:
//   - err is a standard error, if something went wrong
func (Q *QPFiles) Delete(record model.Record) (err error) {
	fromState := record.State
	record.State = model.RecordDeleted
	record.Key = make([]byte, Q.keyLength)
	record.Value = make([]byte, Q.valueLength)

	err = Q.setBucketRecord(record)
	if err != nil {
		err = fmt.Errorf("error while updating record in bucket: %s", err)
	} else {
		Q.updateUtilizationInfo(fromState, record.State)
	}

	return
}
