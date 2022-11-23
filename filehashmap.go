package filehashmap

import (
	"fmt"
	"github.com/gostonefire/filehashmap/interfaces"
	"github.com/gostonefire/filehashmap/internal/model"
	"github.com/gostonefire/filehashmap/internal/storage/scres"
	"github.com/gostonefire/filehashmap/internal/utils"
)

// FileManagement - Interface for any file management implementation
type FileManagement interface {
	CloseFiles()
	RemoveFiles() (err error)
	Get(keyRecord model.Record) (record model.Record, err error)
	Set(record model.Record) (err error)
	Delete(record model.Record) (err error)
	GetBucket(bucketNo int64) (bucket model.Bucket, overflowIterator *scres.OverflowRecords, err error)
	GetStorageParameters() (params model.StorageParameters)
}

// HashMapInfo - Information structure containing some information about the hash map created
//   - RecordsPerBucket is the number of record entries available in each bucket
//   - AverageBucketFillFactor is the average fill factor given initial unique values and total number of available records in all buckets
//   - NumberOfBuckets is the total number of available buckets in the file hash map
//   - FileSize is the total size of the map file created.
type HashMapInfo struct {
	RecordsPerBucket        int64
	AverageBucketFillFactor float64
	NumberOfBuckets         int64
	FileSize                int64
}

// HashMapStat - Statistics on the overall usage and distribution over buckets
//   - Records is the total number of records stored
//   - MapFileRecords is the number of records stored in the fixed sized hash map file
//   - OverflowRecords is the number of records that has ended up in the overflow file
//   - BucketDistribution is the number of records stored in each available bucket
type HashMapStat struct {
	Records            int64
	MapFileRecords     int64
	OverflowRecords    int64
	BucketDistribution []int64
}

// FileHashMap - The main implementation struct
type FileHashMap struct {
	fileManagement FileManagement
	name           string
	// CloseFiles - Closes the hash map file and the ovfl file. Use this preferably in a "defer" directly
	// after a CreateNewFile or NewFromExistingFile.
	CloseFiles func()
	// RemoveFiles - Removes the map file and the overflow file if they exist.
	// The function first internally tries to close them using CloseFiles.
	RemoveFiles func() error
}

// NewFileHashMap - Returns a new file (or set of files) prepared to cover a theoretical initial number of unique values.
// If the number is too low or the spread of the values are not uniform it may be that buckets will be overfilled.
// An overfilled bucket will result in data put in an overflow file which will still work but requires more
// disk operations.
//   - name is the name of the file hash map and will be used to form file name(s)
//   - initialUniqueKeys is the theoretical max number of unique keys to be expected, in theory the limit will provide for no overfilled buckets, in practice it will most likely occur.
//   - keyLength is the length of the key part in a record
//   - valueLength is the length of the value part in a record
//   - hashAlgorithm is an optional entry to provide a custom hash algorithm following the HashAlgorithm interfaces.
//
// It returns:
//   - fileHashMap is a pointer to a FileHashMap struct
//   - hashMapInfo is a HashMapInfo struct containing some data regarding the hash map created.
//   - err is a normal go Error which should be nil if everything went ok
func NewFileHashMap(
	name string,
	initialUniqueKeys int64,
	keyLength int64,
	valueLength int64,
	hashAlgorithm hashfunc.HashAlgorithm,
) (
	fileHashMap *FileHashMap,
	hashMapInfo HashMapInfo,
	err error,
) {

	// Check if initialUniqueKeys is valid
	if initialUniqueKeys <= 0 {
		err = fmt.Errorf("initialUniqueKeys must be a positive value higher than 0 (zero)")
		return

	}

	// Check if the key length is valid
	if keyLength <= 0 {
		err = fmt.Errorf("key length must be a positive value higher than 0 (zero)")
		return
	}

	// Check if the valueLength is valid
	if valueLength <= 0 {
		err = fmt.Errorf("value length must be a positive value higher than 0 (zero)")
		return

	}

	// Check if name is empty
	if name == "" {
		err = fmt.Errorf("name can not be empty, it will be used to name physical files")
		return
	}

	fpConf := scres.SCFilesConf{
		Name:              name,
		InitialUniqueKeys: initialUniqueKeys,
		KeyLength:         keyLength,
		ValueLength:       valueLength,
		HashAlgorithm:     hashAlgorithm,
	}

	var fm FileManagement
	fm, err = scres.NewSCFiles(fpConf)
	if err != nil {
		_ = fm.RemoveFiles()
		return
	}

	// Prepare return data
	fileHashMap = &FileHashMap{
		fileManagement: fm,
		name:           name,
		CloseFiles:     func() { fm.CloseFiles() },
		RemoveFiles: func() error {
			fm.CloseFiles()
			return fm.RemoveFiles()
		},
	}

	sp := fm.GetStorageParameters()

	hashMapInfo = HashMapInfo{
		RecordsPerBucket:        sp.RecordsPerBucket,
		AverageBucketFillFactor: sp.FillFactor,
		NumberOfBuckets:         sp.NumberOfBuckets,
		FileSize:                sp.MapFileSize,
	}

	return
}

// NewFromExistingFiles - Opens an existing file containing a hash map. The file must have a valid header, and if the
// file was created and used together with a custom hash algorithm, also that same algorithm has to be supplied.
//   - name is the name of an existing hash map.
//   - hashAlgorithm is an optional entry to provide a custom hash algorithm following the hashfunc.HashAlgorithm interface.
//
// It returns:
//   - fileHashMap is a pointer to a FileHashMap struct
//   - hashMapInfo is a HashMapInfo struct containing some data regarding the hash map opened.
//   - err is a normal Go Error which should be nil if everything went ok
func NewFromExistingFiles(name string, hashAlgorithm hashfunc.HashAlgorithm) (
	fileHashMap *FileHashMap,
	hashMapInfo HashMapInfo,
	err error,
) {
	var fm FileManagement
	fm, err = scres.NewSCFilesFromExistingFiles(name, hashAlgorithm)
	if err != nil {
		return
	}

	// Prepare return data
	fileHashMap = &FileHashMap{
		fileManagement: fm,
		name:           name,
		CloseFiles:     func() { fm.CloseFiles() },
		RemoveFiles: func() error {
			fm.CloseFiles()
			return fm.RemoveFiles()
		},
	}

	sp := fm.GetStorageParameters()

	hashMapInfo = HashMapInfo{
		RecordsPerBucket:        sp.RecordsPerBucket,
		AverageBucketFillFactor: sp.FillFactor,
		NumberOfBuckets:         sp.NumberOfBuckets,
		FileSize:                sp.MapFileSize,
	}

	return
}

// ReorgConf - Is a struct used in the call to ReorgFiles holding configuration for the new file structure.
//   - InitialUniqueKeys is the new estimated number of unique keys to store in the hash map files
//   - KeyExtension is number of bytes to extend the key with
//   - PrependKeyExtension whether to prepend the extra space or append it
//   - ValueExtension is number of bytes to extend the value with
//   - PrependValueExtension whether to prepend the extra space or append it
//   - NewBucketAlgorithm is the algorithm to use
//   - OldBucketAlgorithm is the algorithm that was used in the original file hash map
type ReorgConf struct {
	InitialUniqueKeys     int64
	KeyExtension          int64
	PrependKeyExtension   bool
	ValueExtension        int64
	PrependValueExtension bool
	NewBucketAlgorithm    hashfunc.HashAlgorithm
	OldBucketAlgorithm    hashfunc.HashAlgorithm
}

// ReorgFiles - Is used when existing hash map files needs to reflect new conditions as compared to when they were
// first created. For instance if the first estimate of initial unique keys was way off and too much data ended up
// in overflow, or we need to store more data in each record, or perhaps a better hash algorithm has been found
// for the particular set of data we are processing.
//
// The function will first rename the old files by inserting "-original", then create new files. The old files will
// not be deleted to prevent data loss due to mistakes.
//
// The reorganization will happen only if there are detectable changes coming from the ReorgConf struct. If the original
// file hash map was created with internal hashfunc.HashAlgorithm and an empty (fields are Go zero values) ReorgConf struct is supplied,
// the function returns with no processing. But values higher than zero in any of initialUniqueKeys, KeyExtension or
// ValueExtension will result in processing. Also, if the existing hash file map was created with custom HashAlgorithm and
// HashAlgorithm is nil, processing will happen. A non nil HashAlgorithm will always result in processing
// even if the existing file hash map happens to be created with the exact same.
//
// To force a reorganization even if there are no changes to apply through the ReorgConf struct, use the force flag in the
// call to the function. This can be handy if a file hash map has been utilized with lots of records having ended up in overflow
// and lots of records have been popped leaving records in overflow that could find available spots in the map file.
//   - name is the name of an existing file hash map (including correct path)
//   - reorgConfig is an instance of the ReorgConf struct.
//   - force set to true forces a reorganization regardless of what is changed from the ReorgConf struct
func ReorgFiles(name string, reorgConf ReorgConf, force bool) (fromHashMapInfo, toHashMapInfo HashMapInfo, err error) {
	newName := fmt.Sprintf("%s-reorg", name)

	var fromFhm, toFhm *FileHashMap

	// Get data from existing hash map files (and by that also checking that they exist)
	// Open existing (we won't use get/set/pop so whatever bucket algorithm is used in the original files is not important)
	fromFhm, _, err = NewFromExistingFiles(name, nil)
	if err != nil {
		return
	}
	fromFhm.CloseFiles()

	// Sort out new settings and also make sure there are any changes at all (unless force flag has already overridden that)
	hasChanges := force
	sp := fromFhm.fileManagement.GetStorageParameters()
	var initialUniqueKeys, keyLength, valueLength int64
	var bucketAlgorithm hashfunc.HashAlgorithm
	if sp.InitialUniqueKeys != reorgConf.InitialUniqueKeys && reorgConf.InitialUniqueKeys > 0 {
		initialUniqueKeys = reorgConf.InitialUniqueKeys
		hasChanges = true
	} else {
		initialUniqueKeys = sp.InitialUniqueKeys
	}
	if reorgConf.KeyExtension > 0 {
		keyLength = sp.KeyLength + reorgConf.KeyExtension
		hasChanges = true
	} else {
		keyLength = sp.KeyLength
	}
	if reorgConf.ValueExtension > 0 {
		valueLength = sp.ValueLength + reorgConf.ValueExtension
		hasChanges = true
	} else {
		valueLength = sp.ValueLength
	}
	if reorgConf.NewBucketAlgorithm != nil || (reorgConf.NewBucketAlgorithm == nil && !sp.InternalAlgorithm) {
		bucketAlgorithm = reorgConf.NewBucketAlgorithm
		hasChanges = true
	}
	if !hasChanges {
		return
	}

	// Open existing (we won't use get/set/pop so whatever bucket algorithm is used in the original files is not important)
	fromFhm, fromHashMapInfo, err = NewFromExistingFiles(name, reorgConf.OldBucketAlgorithm)
	if err != nil {
		return
	}
	defer fromFhm.CloseFiles()

	// Create new file hash map
	toFhm, toHashMapInfo, err = NewFileHashMap(newName, initialUniqueKeys, keyLength, valueLength, bucketAlgorithm)
	if err != nil {
		return
	}
	defer toFhm.CloseFiles()

	err = reorgRecords(fromFhm, toFhm, reorgConf, fromFhm.fileManagement.GetStorageParameters().NumberOfBuckets)
	if err != nil {
		return
	}

	return
}

// reorgRecords - Reads bucket by bucket, record by record, transforms, and writes to new hash map files
func reorgRecords(from *FileHashMap, to *FileHashMap, reorgConf ReorgConf, fromNBuckets int64) (err error) {
	var bucket model.Bucket
	var record model.Record
	var iter *scres.OverflowRecords
	var key, value []byte
	for i := int64(0); i < fromNBuckets; i++ {
		bucket, iter, err = from.fileManagement.GetBucket(i)
		if err != nil {
			return
		}

		// Records from map file
		for _, record = range bucket.Records {
			if record.InUse {
				key = utils.ExtendByteSlice(record.Key, reorgConf.KeyExtension, reorgConf.PrependKeyExtension)
				value = utils.ExtendByteSlice(record.Value, reorgConf.ValueExtension, reorgConf.PrependValueExtension)
				err = to.Set(key, value)
				if err != nil {
					return
				}
			}
		}

		// Records from overflow file
		for iter.HasNext() {
			record, err = iter.Next()
			if err != nil {
				return
			}
			if record.InUse {
				key = utils.ExtendByteSlice(record.Key, reorgConf.KeyExtension, reorgConf.PrependKeyExtension)
				value = utils.ExtendByteSlice(record.Value, reorgConf.ValueExtension, reorgConf.PrependValueExtension)
				err = to.Set(key, value)
				if err != nil {
					return
				}
			}
		}
	}

	return
}
