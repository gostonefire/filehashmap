package openaddressing

import (
	"fmt"
	"github.com/gostonefire/filehashmap/crt"
	"github.com/gostonefire/filehashmap/hashfunc"
	"github.com/gostonefire/filehashmap/internal/hash"
	"github.com/gostonefire/filehashmap/internal/model"
	"github.com/gostonefire/filehashmap/internal/overflow"
	"github.com/gostonefire/filehashmap/internal/storage"
	"os"
)

// OAFiles - Represents an implementation of file support for the Open Addressing Collision Resolution Techniques.
// It uses one file of buckets where each bucket represents a record. In case of a collision, it probes through
// the hash table using a collision resolution algorithm, looking for an empty slot, and assigns the free slot to the value.
// Once all free slots are occupied the table will accept no more records.
type OAFiles struct {
	mapFileName                  string
	mapFile                      *os.File
	keyLength                    int64
	valueLength                  int64
	numberOfBucketsNeeded        int64
	numberOfBucketsAvailable     int64
	maxBucketNo                  int64
	mapFileSize                  int64
	hashAlgorithm                hashfunc.HashAlgorithm
	internalAlgorithm            bool
	CollisionResolutionTechnique int
	nEmpty                       int64
	nOccupied                    int64
	nDeleted                     int64
}

// NewOAFiles - Returns a pointer to a new instance of Open Addressing file implementation.
// It always creates a new file (or opens and truncate existing file)
//   - crtConf is a model.CRTConf struct providing configuration parameter affecting files creation and processing
//
// It returns:
//   - oaFiles which is a pointer to the created instance
//   - err which is a standard Go type of error
func NewOAFiles(crtConf model.CRTConf) (oaFiles *OAFiles, err error) {
	// If no HashAlgorithm was given then use the default internal
	var internalAlg bool
	if crtConf.HashAlgorithm == nil {
		switch crtConf.CollisionResolutionTechnique {
		case crt.LinearProbing:
			crtConf.HashAlgorithm = hash.NewLinearProbingHashAlgorithm(crtConf.NumberOfBucketsNeeded)
		case crt.QuadraticProbing:
			crtConf.HashAlgorithm = hash.NewQuadraticProbingHashAlgorithm(crtConf.NumberOfBucketsNeeded)
		case crt.DoubleHashing:
			crtConf.HashAlgorithm = hash.NewDoubleHashAlgorithm(crtConf.NumberOfBucketsNeeded)
		}
		internalAlg = true
	} else {
		crtConf.HashAlgorithm.SetTableSize(crtConf.NumberOfBucketsNeeded)
	}

	// Calculate the hash map file various parameters
	bucketLength := 1 + crtConf.KeyLength + crtConf.ValueLength // First byte is record state
	maxBucketNo := crtConf.HashAlgorithm.GetTableSize() - 1
	numberOfBuckets := maxBucketNo + 1
	fileSize := bucketLength*numberOfBuckets + storage.MapFileHeaderLength

	oaFiles = &OAFiles{
		mapFileName:                  storage.GetMapFileName(crtConf.Name),
		keyLength:                    crtConf.KeyLength,
		valueLength:                  crtConf.ValueLength,
		numberOfBucketsNeeded:        crtConf.NumberOfBucketsNeeded,
		numberOfBucketsAvailable:     numberOfBuckets,
		maxBucketNo:                  maxBucketNo,
		mapFileSize:                  fileSize,
		hashAlgorithm:                crtConf.HashAlgorithm,
		internalAlgorithm:            internalAlg,
		CollisionResolutionTechnique: crtConf.CollisionResolutionTechnique,
		nEmpty:                       numberOfBuckets,
		nOccupied:                    0,
		nDeleted:                     0,
	}

	header := oaFiles.createHeader()

	err = oaFiles.createNewHashMapFile(header)
	if err != nil {
		return
	}

	return
}

// NewOAFilesFromExistingFiles - Returns a pointer to a new instance of Open Addressing file implementation given
// existing files. If files doesn't exist, doesn't have a valid header or if its file size seems wrong given
// size from header it fails with error.
//   - Name is the name to base map file name on
//
// It returns:
//   - oaFiles which is a pointer to the created instance
//   - err which is a standard Go type of error
func NewOAFilesFromExistingFiles(name string, hashAlgorithm hashfunc.HashAlgorithm) (oaFiles *OAFiles, err error) {
	mapFileName := storage.GetMapFileName(name)

	oaFiles = &OAFiles{mapFileName: mapFileName}

	header, err := oaFiles.openHashMapFile()
	if err != nil {
		return
	}

	// Check for mismatch in choice of hash algorithm
	if header.InternalHash && hashAlgorithm != nil {
		oaFiles.CloseFiles()
		err = fmt.Errorf("seems the hash map file was used with the internal hash algorithm but an external was given")
		return
	}
	if !header.InternalHash && hashAlgorithm == nil {
		oaFiles.CloseFiles()
		err = fmt.Errorf("seems the hash map file was used with the external hash algorithm but no external was given")
		return
	}

	// If no HashAlgorithm was given then use the default internal
	var internalAlg bool
	if hashAlgorithm == nil {
		switch int(header.CollisionResolutionTechnique) {
		case crt.LinearProbing:
			hashAlgorithm = hash.NewLinearProbingHashAlgorithm(header.NumberOfBucketsNeeded)
		case crt.QuadraticProbing:
			hashAlgorithm = hash.NewQuadraticProbingHashAlgorithm(header.NumberOfBucketsNeeded)
		case crt.DoubleHashing:
			hashAlgorithm = hash.NewDoubleHashAlgorithm(header.NumberOfBucketsNeeded)
		}
		internalAlg = true
	} else {
		hashAlgorithm.SetTableSize(header.NumberOfBucketsNeeded)
	}

	oaFiles.keyLength = header.KeyLength
	oaFiles.valueLength = header.ValueLength
	oaFiles.numberOfBucketsNeeded = header.NumberOfBucketsNeeded
	oaFiles.numberOfBucketsAvailable = header.NumberOfBucketsAvailable
	oaFiles.maxBucketNo = header.MaxBucketNo
	oaFiles.mapFileSize = header.FileSize
	oaFiles.hashAlgorithm = hashAlgorithm
	oaFiles.internalAlgorithm = internalAlg
	oaFiles.CollisionResolutionTechnique = int(header.CollisionResolutionTechnique)
	oaFiles.nEmpty = header.NumberOfEmptyRecords
	oaFiles.nOccupied = header.NumberOfOccupiedRecords
	oaFiles.nDeleted = header.NumberOfDeletedRecords

	return
}

// CloseFiles - Closes the map files
func (Q *OAFiles) CloseFiles() {
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
func (Q *OAFiles) RemoveFiles() (err error) {
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
func (Q *OAFiles) GetStorageParameters() (params model.StorageParameters) {
	params = model.StorageParameters{
		CollisionResolutionTechnique: Q.CollisionResolutionTechnique,
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
func (Q *OAFiles) GetBucket(bucketNo int64) (bucket model.Bucket, overflowIterator *overflow.Records, err error) {
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
func (Q *OAFiles) Get(keyRecord model.Record) (record model.Record, err error) {
	// Check validity of the key
	if int64(len(keyRecord.Key)) != Q.keyLength {
		err = fmt.Errorf("wrong length of key, should be %d", Q.keyLength)
		return
	}

	// Tro to find the key in the file
	record, err = Q.probingForGet(keyRecord.Key)

	return
}

// Set - Updates an existing record with new data or add it if no existing is found with same key.
//   - record is the record to set, it needs only to contain Key and Value, and they have to conform to lengths given when creating the SCFiles
//
// It returns:
//   - err is a standard error, if something went wrong
func (Q *OAFiles) Set(record model.Record) (err error) {
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

	selectedRecord, err := Q.probingForSet(record.Key)
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
func (Q *OAFiles) Delete(record model.Record) (err error) {
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
