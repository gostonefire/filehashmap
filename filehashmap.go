package filehashmap

import (
	"fmt"
	"github.com/gostonefire/filehashmap/internal/conf"
	"github.com/gostonefire/filehashmap/internal/file"
	"github.com/gostonefire/filehashmap/internal/hash"
	"github.com/gostonefire/filehashmap/internal/model"
	"github.com/gostonefire/filehashmap/internal/utils"
	"math"
	"os"
)

// FileProcessing - Interface for any file processing implementation
type FileProcessing interface {
	CloseFiles()
	RemoveFiles() (err error)
	SetBucketRecord(record model.Record) (err error)
	GetBucketRecords(bucketNo int64) (bucket model.Bucket, err error)
	SetBucketOverflowAddress(bucketAddress, overflowAddress int64) (err error)
	NewBucketOverflow(key, value []byte) (overflowAddress int64, err error)
	SetOverflowRecord(record model.Record) (err error)
	GetOverflowRecord(recordAddress int64) (record model.Record, err error)
	AppendOverflowRecord(linkingRecord model.Record, key, value []byte) (err error)
}

// BucketAlgorithm - Interface that permits an implementation using the FileHashMap to supply a custom bucket
// selection algorithm suited for its particular distribution of keys.
// The internally used algorithm is implemented using crc32.ChecksumIEEE to create a hash value over the key and
// then applying bucket = hash & (1<<exp - 1) to get the bucket number, where 1<<exp (2 to the power of exp)
// is the total number of buckets to distribute over.
type BucketAlgorithm interface {
	// BucketNumber - Given key it generates a bucket number between minValue and maxValue (inclusive)
	// Any number returned outside the minValue/maxValue (inclusive) range will result in an error down stream.
	BucketNumber(key []byte) int64
	// BucketNumberRange - Returns the min and max (inclusive) that BucketNumber will ever return.
	BucketNumberRange() (minValue, maxValue int64)
}

// HashMapInfo - Information structure containing important information that should be studied before
// calling the file create function.
//   - RecordsPerBucket is the number of record entries available in each bucket
//   - AverageBucketFillFactor is the average fill factor given initial unique values and total number of available records in all buckets
//   - NumberOfBuckets is the total number of available buckets in the file hash map
//   - FileSize is the total size of the file to create, it is TrueRecordLength * RecordsPerBucket * NumberOfBuckets + conf.MapFileHeaderLength.
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
	bucketAlg         BucketAlgorithm
	fileProcessing    FileProcessing
	mapFileName       string
	ovflFileName      string
	initialUniqueKeys int64
	keyLength         int64
	valueLength       int64
	recordsPerBucket  int64
	numberOfBuckets   int64
	minBucketNo       int64
	maxBucketNo       int64
	fileSize          int64
	internalAlg       bool
	// CloseFiles - Closes the hash map file and the ovfl file. Use this preferably in a "defer" directly
	// after a CreateNewFile or NewFromExistingFile.
	CloseFiles func()
	// RemoveFiles - Removes the map file and the overflow file if they exist.
	// The function first internally tries to close them using CloseFiles.
	RemoveFiles func() error
}

// NewFileHashMap - Returns a new file prepared to cover a theoretical initial number of unique values.
// If the number is too low or the spread of the values are not uniform it may be that buckets will be overfilled.
// An overfilled bucket will result in data put in an overflow file which will still work but requires more
// disk operations.
//   - name is the name of the file hash map and will be used when the function CreateNewFiles are used (se documentation for that function)
//   - initialUniqueKeys is the theoretical max number of unique keys to be expected, in theory the limit will provide for no overfilled buckets, in practice it will most likely occur.
//   - keyLength is the length of the key part in a record
//   - valueLength is the length of the value part in a record
//   - bucketAlgorithm is an optional entry to provide a custom bucket selection algorithm following the BucketAlgorithm interface.
//
// It returns:
//   - fileHashMap is a pointer to a FileHashMap struct
//   - hashMapInfo is a struct containing some data regarding the hash map to create. Most important is probably the file size to avoid extreme file sizes by mistake.
//   - err is a normal go Error which should be nil if everything went ok
func NewFileHashMap(
	name string,
	initialUniqueKeys int64,
	keyLength int64,
	valueLength int64,
	bucketAlgorithm BucketAlgorithm,
) (
	fileHashMap *FileHashMap,
	hashMapInfo HashMapInfo,
	err error,
) {

	// If no BucketAlgorithm was given then use the default internal
	var internalAlg bool
	if bucketAlgorithm == nil {
		bucketAlgorithm = hash.NewBucketAlgorithm(initialUniqueKeys)
		internalAlg = true
	}

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

	// Calculate the hash map file various parameters
	trueRecordLength := keyLength + valueLength + conf.InUseFlagBytes
	minBucketNo, maxBucketNo := bucketAlgorithm.BucketNumberRange()
	numberOfBuckets := maxBucketNo - minBucketNo + 1
	recordsPerBucket := int64(math.Ceil(float64(initialUniqueKeys) / float64(numberOfBuckets)))
	bucketLength := trueRecordLength*recordsPerBucket + conf.BucketHeaderLength
	fileSize := bucketLength*numberOfBuckets + conf.MapFileHeaderLength
	fillFactor := float64(initialUniqueKeys) / float64(recordsPerBucket*numberOfBuckets)

	// Prepare return data
	fileHashMap = &FileHashMap{
		bucketAlg:         bucketAlgorithm,
		mapFileName:       fmt.Sprintf("%s-map.bin", name),
		ovflFileName:      fmt.Sprintf("%s-ovfl.bin", name),
		initialUniqueKeys: initialUniqueKeys,
		keyLength:         keyLength,
		valueLength:       valueLength,
		recordsPerBucket:  recordsPerBucket,
		numberOfBuckets:   numberOfBuckets,
		minBucketNo:       minBucketNo,
		maxBucketNo:       maxBucketNo,
		fileSize:          fileSize,
		internalAlg:       internalAlg,
	}

	hashMapInfo = HashMapInfo{
		RecordsPerBucket:        recordsPerBucket,
		AverageBucketFillFactor: fillFactor,
		NumberOfBuckets:         numberOfBuckets,
		FileSize:                fileSize,
	}

	return
}

// NewFromExistingFiles - Opens an existing file containing a hash map. The file must have a valid header, and if the
// file was created and used together with a custom bucket algorithm, also that same algorithm has to be supplied.
//   - name is the name of an existing hash map.
//   - bucketAlgorithm is an optional entry to provide a custom bucket selection algorithm following the BucketAlgorithm interface.
//
// It returns:
//   - fileHashMap is a pointer to a FileHashMap struct
//   - hashMapInfo is a struct containing some data regarding the hash map to create.
//   - err is a normal Go Error which should be nil if everything went ok
func NewFromExistingFiles(name string, bucketAlgorithm BucketAlgorithm) (
	fileHashMap *FileHashMap,
	hashMapInfo HashMapInfo,
	err error,
) {
	var header model.Header
	var fp FileProcessing
	mapFileName := fmt.Sprintf("%s-map.bin", name)
	ovflFileName := fmt.Sprintf("%s-ovfl.bin", name)

	fp, header, err = file.NewSCFilesFromExistingFiles(mapFileName, ovflFileName)
	if err != nil {
		return
	}

	// Check for mismatch in choice of bucket algorithm
	if header.InternalAlg && bucketAlgorithm != nil {
		fp.CloseFiles()
		err = fmt.Errorf("seems the hash map file was used with the internal hash algorithm but an external was given")
		return
	}
	if !header.InternalAlg && bucketAlgorithm == nil {
		fp.CloseFiles()
		err = fmt.Errorf("seems the hash map file was used with the external hash algorithm but no external was given")
		return
	}

	// If no BucketAlgorithm was given then use the default internal
	if bucketAlgorithm == nil {
		bucketAlgorithm = hash.NewBucketAlgorithm(header.InitialUniqueKeys)
	}

	// Prepare return data
	fileHashMap = &FileHashMap{
		bucketAlg:         bucketAlgorithm,
		fileProcessing:    fp,
		mapFileName:       mapFileName,
		ovflFileName:      ovflFileName,
		initialUniqueKeys: header.InitialUniqueKeys,
		keyLength:         header.KeyLength,
		valueLength:       header.ValueLength,
		recordsPerBucket:  header.RecordsPerBucket,
		numberOfBuckets:   header.NumberOfBuckets,
		minBucketNo:       header.MinBucketNo,
		maxBucketNo:       header.MaxBucketNo,
		fileSize:          header.FileSize,
		internalAlg:       header.InternalAlg,
		CloseFiles:        func() { fp.CloseFiles() },
		RemoveFiles: func() error {
			fp.CloseFiles()
			return fp.RemoveFiles()
		},
	}

	fillFactor := float64(header.InitialUniqueKeys) / float64(header.RecordsPerBucket*header.NumberOfBuckets)

	hashMapInfo = HashMapInfo{
		RecordsPerBucket:        header.RecordsPerBucket,
		AverageBucketFillFactor: fillFactor,
		NumberOfBuckets:         header.NumberOfBuckets,
		FileSize:                header.FileSize,
	}

	return
}

// CreateNewFiles - Creates new files according to name given in call to NewFileHashMap, there will be two files
// created, one fixed sized file of the size that was calculated when instantiating the file hash map and one
// dynamic that will grow whenever there is overflow in any buckets.
//
// If name given in call to NewFileHashMap doesn't contain any path they will end up in from wherever the application
// has its base path.
//
// The two files will be named:
//   - <name>-map.bin
//   - <name>-ovfl.bin
//
// If the files already exists they will first be truncated to zero and then to calculated length,
// hence removing all existing data.
func (F *FileHashMap) CreateNewFiles() (err error) {
	header := model.Header{
		InternalAlg:       F.internalAlg,
		InitialUniqueKeys: F.initialUniqueKeys,
		KeyLength:         F.keyLength,
		ValueLength:       F.valueLength,
		RecordsPerBucket:  F.recordsPerBucket,
		NumberOfBuckets:   F.numberOfBuckets,
		MinBucketNo:       F.minBucketNo,
		MaxBucketNo:       F.maxBucketNo,
		FileSize:          F.fileSize,
	}

	fpConf := file.SCFilesConf{
		MapFileName:      F.mapFileName,
		OvflFileName:     F.ovflFileName,
		KeyLength:        F.keyLength,
		ValueLength:      F.valueLength,
		RecordsPerBucket: F.recordsPerBucket,
		FileSize:         F.fileSize,
	}

	fp, err := file.NewSCFiles(fpConf, header)
	if err != nil {
		_ = fp.RemoveFiles()
		return
	}

	F.fileProcessing = fp
	F.CloseFiles = func() { fp.CloseFiles() }
	F.RemoveFiles = func() error {
		fp.CloseFiles()
		return fp.RemoveFiles()
	}

	return
}

// ReorgConf - Is a struct used in the call to ReorgFiles holding configuration for the new file structure.
//   - InitialUniqueKeys is the new estimated number of unique keys to store in the hash map files
//   - KeyExtension is number of bytes to extend the key with
//   - PrependKeyExtension whether to prepend the extra space or append it
//   - ValueExtension is number of bytes to extend the value with
//   - PrependValueExtension whether to prepend the extra space or append it
//   - BucketAlgorithm is the algorithm to use
type ReorgConf struct {
	InitialUniqueKeys     int64
	KeyExtension          int64
	PrependKeyExtension   bool
	ValueExtension        int64
	PrependValueExtension bool
	BucketAlgorithm       BucketAlgorithm
}

// ReorgFiles - Is used when existing hash map files needs to reflect new conditions as compared to when they were
// first created. For instance if the first estimate of initial unique keys was way off and too much data ended up
// in overflow, or we need to store more data in each record, or perhaps a better bucket algorithm has been found
// for the particular set of data we are processing.
//
// The function will first rename the old files by inserting "-original", then create new files. The old files will
// not be deleted to prevent data loss due to mistakes.
//
// The reorganization will happen only if there are detectable changes coming from the ReorgConf struct. If the original
// file hash map was created with internal BucketAlgorithm and an empty (fields are Go zero values) ReorgConf struct is supplied,
// the function returns with no processing. But values higher than zero in any of InitialUniqueKeys, KeyExtension or
// ValueExtension will result in processing. Also, if the existing hash file map was created with custom BucketAlgorithm and
// BucketAlgorithm is nil, processing will happen. A non nil BucketAlgorithm will always result in processing
// even if the existing file hash map happens to be created with the exact same.
//
// To force a reorganization even if there are no changes to apply through the ReorgConf struct, use the force flag in the
// call to the function. This can be handy if a file hash map has been utilized with lots of records having ended up in overflow
// and lots of records have been popped leaving records in overflow that could find available spots in the map file.
//   - name is the name of an existing file hash map (including correct path)
//   - reorgConfig is an instance of the ReorgConf struct.
//   - force set to true forces a reorganization regardless of what is changed from the ReorgConf struct
func ReorgFiles(name string, reorgConf ReorgConf, force bool) (fromHashMapInfo, toHashMapInfo HashMapInfo, err error) {
	bakName := fmt.Sprintf("%s-original", name)
	mapFileName := fmt.Sprintf("%s-map.bin", name)
	ovflFileName := fmt.Sprintf("%s-ovfl.bin", name)
	bakMapFileName := fmt.Sprintf("%s-map.bin", bakName)
	bakOvflFileName := fmt.Sprintf("%s-ovfl.bin", bakName)

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
	var initialUniqueKeys, keyLength, valueLength int64
	var bucketAlgorithm BucketAlgorithm
	if fromFhm.initialUniqueKeys != reorgConf.InitialUniqueKeys && reorgConf.InitialUniqueKeys > 0 {
		initialUniqueKeys = reorgConf.InitialUniqueKeys
		hasChanges = true
	} else {
		initialUniqueKeys = fromFhm.initialUniqueKeys
	}
	if reorgConf.KeyExtension > 0 {
		keyLength = fromFhm.keyLength + reorgConf.KeyExtension
		hasChanges = true
	} else {
		keyLength = fromFhm.keyLength
	}
	if reorgConf.ValueExtension > 0 {
		valueLength = fromFhm.valueLength + reorgConf.ValueExtension
		hasChanges = true
	} else {
		valueLength = fromFhm.valueLength
	}
	if reorgConf.BucketAlgorithm != nil || (reorgConf.BucketAlgorithm == nil && !fromFhm.internalAlg) {
		bucketAlgorithm = reorgConf.BucketAlgorithm
		hasChanges = true
	}
	if !hasChanges {
		return
	}

	// Rename files
	err = os.Rename(mapFileName, bakMapFileName)
	if err != nil {
		err = fmt.Errorf("error while renaming existing file %s to %s", mapFileName, bakMapFileName)
		return
	}
	err = os.Rename(ovflFileName, bakOvflFileName)
	if err != nil {
		err = fmt.Errorf("error while renaming existing file %s to %s", ovflFileName, bakOvflFileName)
		return
	}

	// Open existing (we won't use get/set/pop so whatever bucket algorithm is used in the original files is not important)
	fromFhm, fromHashMapInfo, err = NewFromExistingFiles(bakName, nil)
	if err != nil {
		return
	}
	defer fromFhm.CloseFiles()

	// Create new file hash map
	toFhm, toHashMapInfo, err = NewFileHashMap(name, initialUniqueKeys, keyLength, valueLength, bucketAlgorithm)
	if err != nil {
		return
	}
	err = toFhm.CreateNewFiles()
	if err != nil {
		return
	}
	defer toFhm.CloseFiles()

	err = reorgRecords(fromFhm, toFhm, reorgConf)
	if err != nil {
		return
	}

	return
}

// reorgRecords - Reads bucket by bucket, record by record, transforms, and writes to new hash map files
func reorgRecords(from *FileHashMap, to *FileHashMap, reorgConf ReorgConf) (err error) {
	var bucket model.Bucket
	var record model.Record
	var iter *OverflowRecords
	var key, value []byte
	for i := int64(0); i < from.numberOfBuckets; i++ {
		bucket, iter, err = from.getBucket(i)
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
		for iter.hasNext() {
			record, err = iter.next()
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
