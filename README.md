# FileHashMap
## Purpose
Implements a file based map for managing large amounts of data that wouldn't fit in memory based maps such 
as the standard Go map type.

## Importing the module
```
go get github.com/gostonefire/filehashmap@v0.6.0
```

## Quick example
```
package main

import (
	"errors"
	"fmt"
	"github.com/gostonefire/filehashmap"
)

func main() {
	keyA := []byte{0, 1, 2, 3, 4, 5, 6, 7}
	keyB := []byte{7, 6, 5, 4, 3, 2, 1, 0}
	keyC := []byte{1, 1, 1, 1, 1, 1, 1, 1}

	dataA := []byte{8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19}
	dataB := []byte{19, 18, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8}
	dataC := []byte{2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2}

	fhm, info, _ := filehashmap.NewFileHashMap("test", 100, 8, 12, nil)
	fmt.Printf("RecordsPerBucket: %d, AverageBucketFillFactor: %.4f, NumberOfBuckets: %d, FileSize: %d\n",
		info.RecordsPerBucket, info.AverageBucketFillFactor, info.NumberOfBuckets, info.FileSize)

	_ = fhm.CreateNewFiles()
	defer fhm.CloseFiles()

	_ = fhm.Set(keyA, dataA)
	_ = fhm.Set(keyB, dataB)
	_ = fhm.Set(keyC, dataC)

	_, _ = fhm.Pop(keyC)

	recordA, _ := fhm.Get(keyA)
	fmt.Printf("recordA: %v\n", recordA)

	recordB, _ := fhm.Get(keyB)
	fmt.Printf("recordB: %v\n", recordB)

	recordC, err := fhm.Get(keyC)
	if errors.Is(err, filehashmap.NoRecordFound{}) {
		fmt.Println("Record not found")
	}
	fmt.Printf("recordC: %v\n", recordC)

	stat, _ := fhm.Stat(true)
	fmt.Printf("Records: %d, MapFileRecords: %d, OverflowRecords: %d, BucketDistribution length: %d\n",
		stat.Records, stat.MapFileRecords, stat.OverflowRecords, len(stat.BucketDistribution))

	_ = fhm.RemoveFiles()
}

// RecordsPerBucket: 2, AverageBucketFillFactor: 0.7812, NumberOfBuckets: 64, FileSize: 4224
// recordA: [8 9 10 11 12 13 14 15 16 17 18 19]
// recordB: [19 18 17 16 15 14 13 12 11 10 9 8]
// Record not found
// recordC: []
// Records: 2, MapFileRecords: 2, OverflowRecords: 0, BucketDistribution length: 64
```

## Description
FileHashMap is a filed backed map that can be used to store large amount of data with quick access through 
a hash algorithm that gives a bucket number which in turn more or less directly points out the bucket address in
file. Hence, the map file is a fixed sized file with most often 2 record spots per bucket. A special case is when
the number of buckets from the bucket algorithm is equal to the initialUniqueKeys parameter given at creation time,
which then gives an allocation of one record per bucket.

Of course, keys will never be perfectly distributed over available buckets so an overflow file is also created.
Once a bucket is filled and a new record is about to be assigned, the bucket gets an entry in the overflow file.
The overflow file is not fixed size but rather grows as needed, and the records are instead working as single
linked lists. Hence, using linked list technique in overflow leads to more disk access so well distributed keys over
buckets to keep most traffic in the map file is desirable.

### Creating a file hash map:
The NewFileHashMap function creates a new instance, although at this stage no physical files are created.
The calling parameters are:
  * name - The name of the file hash map that will eventually form the name (and path) of the physical files.
  * initialUniqueKeys - The number of estimated unique keys that will be entered. The closer to what is actually going into the map reduces the usage of overflow.
  * keyLength - Is the fixed key length that will later be accepted
  * valueLength - Is the fixed value length that will later be accepted
  * bucketAlgorithm - Makes it possible to supply your own algorithm (will be discussed further down), set to nil to use the internal one.

```
fhm, info, err := filehashmap.NewFileHashMap("test", 100, 8, 12, nil)
if err != nil {
    // Do some logging or whatever
    ...
    return
}
```

In the example above we create a rather small hash map suited for 100 unique keys.
We decided that the length of each key is 8 bytes and the value we store together with it is 12 bytes long.
We didn't supply any custom bucket algorithm.

Returned data are:
  * fhm - a pointer to the FileHashMap instantiation. It exports only functions:
    * Get(key []byte) (value []byte, err error)
    * Set(key []byte, value []byte) (err error)
    * Pop(key []byte) (record file.Record, err error)
    * Stat() (hashMapStat *HashMapStat, err error)
    * GetBucketNo(key []byte) (bucketNo int64, err error)
    * CreateNewFiles() (err error)
    * CloseFiles()
    * RemoveFiles() (err error)
  * info - a pointer to a HashMapInfo struct which contains:
    * RecordsPerBucket - Number of records allocated per bucket in the map file
    * AverageBucketFillFactor - The calculated expected average filled records per bucket, should the keys be perfectly distributed over bucket numbers.
    * NumberOfBuckets - Total number of buckets allocated in map file
    * FileSize - Size of the file **to be created**
  * err - which is a standard Go error

At this point no physical files has been created. Reason for not auto create files is that the file size can be really large,
and by having a separate CreateNewFiles function it can be programmatically checked with the HashMapInfo data if the size
is reasonable given e.g. space available on the selected drive.

### Creating the physical files
The CreateNewFiles function creates two physical files; the map file and the overflow file.
File names are constructed using the name that was given in the call to NewFileHashMap.
  * Map file - <name>-map.bin
  * Overflow file - <name>-ovfl.bin

If name includes a path the files will end up in that path, otherwise they will end upp from within where the application
is executed.

```
err := fhm.CreateNewFiles()
if err != nil {
    // Do some logging or whatever
    ...
    return
}
defer fhm.CloseFiles()
```

The map file is fixed size with a header space of 1024 bytes. Each bucket has its own header 
of 8 bytes to hold the address in the overflow (should overflow occur, otherwise uint64(0)).
Each record in a bucket has a one byte header indicating whether the record is in use or not.

The map file structure permits minimum reads/writes to get/set data since a read fetches the entire bucket which gives
all information necessary to get records and whether to also iterate in the overflow file. Each record write is one
file operation since the "in use" flag is a header per data record.

The overflow file has a header of 1024 bytes for future use, current version does not use it. Records in the overflow
file are single linked records, end the entry point to the starting record is held in the bucket header in the map file.
There is no reason to have double linked records since we are talking about files here. If a record that happens to
exist in overflow file is popped, it is very hard to reclaim space overall in the file. Furthermore, to keep track on
every freed record for overall use by all buckets, together with all the extra file access to update double linked list 
would be a performance hit. Instead, a freed record will be reused if a Set with new key on the bucket needs space in overflow.

### Opening an existing file hash map
The NewFromExistingFiles opens an existing file hash map. 
The calling parameters are:
  * name - The name of the hash map, including whatever path the physical files is within.
  * bucketAlgorithm - The same, and it has to be the same, algorithm that was used when it was first created.

Returned data is the same as for NewFileHashMap, but of course the FileSize in the HashMapInfo struct is now the 
actual size of the physical files.

```
fhm, info, err := filehashmap.NewFromExistingFiles("test", nil)
if err != nil {
    // Do some logging or whatever
    ...
    return
}
defer fhm.CloseFiles()
```

### Closing files
The CloseFiles function just closes the physical files and removes the pointer to them in the FileHashMap instance.
Preferably it is used together with a defer.

```
defer fhm.CloseFiles()
```

### Removing files
The RemoveFiles function first tries to close the files, and then physically removes them.

```
err := fhm.RemoveFiles()
if err != nil {
    // Do some logging or whatever
    ...
    return
}
```

It returns only an error, should something bad had happened while removing files.

### Reorganizing files
Since FileHashMap relies on fixed length records and pre-allocated file space to be as high performant as possible, there 
are some guesswork involved when setting the first instance up. But things may change down the line:
  * Estimation of number of unique keys to store values for may have been way of
  * Key and/or value lengths may have to be expanded
  * The nature of the key construction may gain on a customized bucket algorithm to get better distribution over buckets, hence lower the amount of records in overflow.

The ReorgFiles function does that reorganization in one command.

Under the hood it renames the original hash map files (a "-original" is inserted in the names), a new set of files are 
created given configuration data provided to the ReorgFiles function. Records are pulled from original files, bucket by 
bucket, and set in the new files. After processing is finished the original files are left in the file system for the caller
to decide what to do with them. Auto delete is not done.

Configuration:
```
type ReorgConf struct {
	InitialUniqueKeys     int64
	KeyExtension          int64
	PrependKeyExtension   bool
	ValueExtension        int64
	PrependValueExtension bool
	BucketAlgorithm       BucketAlgorithm
}
```

Fill in whatever parameter needs change and leave the others with Go zero values (integers are zero, booleans are false and 
interfaces are nil). Hence, a configuration to change only the length of the value in a record with the extended bytes 
prepended to the existing values will look like this:
```
reorgConf := filehashmap.ReorgConf{
	ValueExtension:        10,
	PrependValueExtension: true,
}

fmt.Printf("%+v\n", reorgConf)

// {InitialUniqueKeys:0 KeyExtension:0 PrependKeyExtension:false ValueExtension:10 PrependValueExtension:true BucketAlgorithm:<nil>}
```

The BucketAlgorithm is managed slightly different. Sending in a nil in the config struct will result in reorganization if
the original FileHashMap was created with a custom bucket algorithm. Setting the BucketAlgorithm config parameter to a 
custom one always result in reorganization since the original FileHashMap does only keep track on whether a custom algorithm 
was used or not, but doesn't know which particular one was used.

Given all of the above, sending in an empty ReorgConf struct for a hash map file created with internal bucket algorithm will result
in no processing at all. To force a reorganization even if there are no changes to apply through the ReorgConf struct, 
use the force flag in the call to the function. This can be handy if a file hash map has been utilized with lots of records 
having ended up in overflow and lots of records have been popped leaving records in overflow that could find available spots in the map file.

Once the config struct is created it is passed together with the name (including any path) of the current file hash map
to the ReorgFiles function. Below is a complete example start to finish (error handling removed for better readability):
```
package main

import (
	"fmt"
	"github.com/gostonefire/filehashmap"
)

func main() {
	// Create a new FileHashMap with initialUniqueKeys=100, keyLength=5 and valueLength=10
	fhm, _, _ := filehashmap.NewFileHashMap("test", 100, 5, 10, nil)
	_ = fhm.CreateNewFiles()

	// Add a record
	key := []byte{1, 2, 3, 4, 5}
	value := []byte{10, 9, 8, 7, 6, 5, 4, 3, 2, 1}

	_ = fhm.Set(key, value)

	// Close files
	fhm.CloseFiles()

	// Reorganize the files with 5 bytes appended to keys and 5 bytes prepended to values
	reorgConf := filehashmap.ReorgConf{
		KeyExtension:          5,
		ValueExtension:        5,
		PrependValueExtension: true,
	}

	fromInfo, toInfo, _ := filehashmap.ReorgFiles("test", reorgConf, false)

	fmt.Printf("%+v\n%+v\n", fromInfo, toInfo)

	// Open the reorganized files
	fhm, _, _ = filehashmap.NewFromExistingFiles("test", nil)
	defer fhm.CloseFiles()

	// Get the stored value given the new key length
	value, _ = fhm.Get([]byte{1, 2, 3, 4, 5, 0, 0, 0, 0, 0})

	fmt.Printf("%+v\n", value)
}

// {RecordsPerBucket:2 AverageBucketFillFactor:0.78125 NumberOfBuckets:64 FileSize:3584}
// {RecordsPerBucket:2 AverageBucketFillFactor:0.78125 NumberOfBuckets:64 FileSize:4864}
// [0 0 0 0 0 10 9 8 7 6 5 4 3 2 1]
```

The file size changed by 1280 bytes which comes from 5 extra key bytes and 5 extra value bytes, times total number of
available records which is 2 times 64, i.e. (5+5)\*2\*64 = 1280.

We had to use the new key size (5 bytes appended) when getting the original but extended value (5 bytes prepended).

## Operations
#### Set(key []byte, value []byte) (err error)
Sets a new value to the map or updates an existing if the key is already present.

The calling parameters are:
  * key - The key that identifies the record to be written. Must be of same length as indicated when the FileHashMap was created.
  * value - The value of the record to be written. Must be of same length as indicated when the FileHashMap was created.

Returned data is:
  * err - Is a standard Go error indicating what might have gone wrong

```
keyA := []byte{0, 1, 2, 3, 4, 5, 6, 7}
dataA := []byte{8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19}
	
err = fhm.Set(keyA, dataA)
if err != nil {
    // Do some logging or whatever
    ...
    return
}
```

#### Get(key []byte) (value []byte, err error)
Gets value given a key.

The calling parameters are:
  * key - The key that identifies the record to be fetched. Must be of same length as indicated when the FileHashMap was created.

Returned data is:
  * value - The value of the record identified by the key, or nil if no record was found.
  * err - An error of type filehashmap.NoRecordFound if no record was found, or a standard Go error if something else went wrong.

```
recordC, err := fhm.Get(keyC)
if errors.Is(err, filehashmap.NoRecordFound{}) {
	// Manage the not found record or whatever
	...
} else if err != nil {
    // Do some logging or whatever
    ...
    return	
}
```

#### Pop(key []byte) (value []byte, err error)
Gets value given a key and then removes the record from the map

The calling parameters are:
  * key - The key that identifies the record to be fetched. Must be of same length as indicated when the FileHashMap was created.

Returned data is:
  * value - The value of the record identified by the key, or nil if no record was found.
  * err - An error of type filehashmap.NoRecordFound if no record was found, or a standard Go error if something else went wrong.

```
recordC, err := fhm.Pop(keyC)
if errors.Is(err, filehashmap.NoRecordFound{}) {
	// Manage the not found record or whatever
	...
} else if err != nil {
    // Do some logging or whatever
    ...
    return	
}
```

#### Stat(includeDistribution bool) (hashMapStat *HashMapStat, err error)
Gathers some statistics from the hash map files

Note:
All buckets are visited, including traversing through overflow linked lists, so the operation can be very time-consuming.
Also, if the number of buckets are very high the BucketDistribution slice may occupy a decent amount of memory, which is why
the hashMapStat is returned as a pointer and not a copy (the latter which is often the preferred way in Go).

The calling parameters are:
  * includeDistribution - Set to true will include a slice of length NumberOfBuckets with number of records per bucket, false will set HashMapStat.BucketDistribution to nil.

Returned data is:
  * hashMapStat - A pointer to a HashMapStat struct that includes the following data:
    * Records - Total number of records stored in files
    * MapFileRecords - Total number of records stored in the map file
    * OverflowRecords - Total number of records stored in the overflow file 
    * BucketDistribution []int64 - A slice of length that equals total number of buckets with number of records per bucket, or nil if includeDistribution was set to false
  * err - An error of standard Go error type if something went wrong

```
stat, err := fhm.Stat(true)
if err != nil {
    // Do some logging or whatever
    ...
    return
}

fmt.Println("HashMapStat for a FileHashMap with eight buckets and two records stored:")
fmt.Printf("%#v\n", stat)

// HashMapStat for a FileHashMap with eight buckets and two records stored:
// &filehashmap.HashMapStat{Records:2, MapFileRecords:2, OverflowRecords:0, BucketDistribution:[]int64{1, 0, 0, 0, 0, 0, 0, 1}}
```

#### GetBucketNo(key []byte) (bucketNo int64, err error)
Returns the zero based bucket number given a key

This function has little practical use from a map perspective, but rather present an assistant when supplying
a custom BucketAlgorithm, i.e. to test things out.

Returned data is:
  * bucketNo - A bucket number from 0 (zero) to NumberOfBucket - 1
  * err - An error of standard Go error type if something went wrong, and currently the only thing that can go wrong is if the BucketAlgorithm returns a number outside accepted range.

```
keyA := []byte{0, 1, 2, 3, 4, 5, 6, 7}

bucketNo, err := fhm.GetBucketNo(keyA)
if err != nil {
    // Do some logging or whatever
    ...
    return
}

fmt.Printf("Bucket number: %d", bucketNo)

// Bucket number: 7
```

## Custom bucket algorithm
When creating a new FileHashMap instance a custom bucket algorithm can be supplied given it implements the
BucketAlgorithm interface. The reason for doing so can be if the distribution of keys for the data to store is very 
awkward and could gain from having a better fitting algorithm to avoid too much data in overflow.

```
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
```

Internally every bucket number is transformed to zero based before applying to the map, i.e. number from BucketNumber -
minValue from BucketNumberRange. So when testing with GetBucketNo the used bucket number is shown, not necessarily 
the one created from the interface implementation.

The internal implementation should result in good enough keys for most situation though:

```
package hash

import (
	"hash/crc32"
	"math"
)

// BucketAlgorithm - The internally used bucket selection algorithm is implemented using crc32.ChecksumIEEE to
// create a hash value over the key and then applying bucket = hash & (1<<exp - 1) to get the bucket number,
// where 1<<exp (2 to the power of exp) is the total number of buckets to distribute over.
type BucketAlgorithm struct {
	exp int64
}

// NewBucketAlgorithm - Returns a pointer to a new BucketAlgorithm instance
func NewBucketAlgorithm(initialUniqueValues int64) *BucketAlgorithm {
	exp := int64(math.Floor(math.Log2(float64(initialUniqueValues)) / math.Log2(2)))
	return &BucketAlgorithm{exp: exp}
}

// BucketNumber - Given key it generates a bucket number between minValue and maxValue (inclusive)
func (B *BucketAlgorithm) BucketNumber(key []byte) int64 {
	h := int64(crc32.ChecksumIEEE(key))
	return h & (1<<B.exp - 1)
}

// BucketNumberRange - Returns the min and max (inclusive) that BucketNumber will ever return.
func (B *BucketAlgorithm) BucketNumberRange() (minValue, maxValue int64) {
	minValue = 0
	maxValue = 1<<B.exp - 1
	return
}
```
