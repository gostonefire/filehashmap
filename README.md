# FileHashMap
## Purpose
Implements a file based map for managing large amounts of data that wouldn't fit in memory based maps such 
as the standard Go map type.

## Importing the module
```
go get github.com/gostonefire/filehashmap@latest
```

## Quick example
```
package main

import (
	"errors"
	"fmt"
	"github.com/gostonefire/filehashmap"
	"github.com/gostonefire/filehashmap/crt"
)

func main() {
	keyA := []byte{0, 1, 2, 3, 4, 5, 6, 7}
	keyB := []byte{7, 6, 5, 4, 3, 2, 1, 0}
	keyC := []byte{1, 1, 1, 1, 1, 1, 1, 1}

	dataA := []byte{8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19}
	dataB := []byte{19, 18, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8}
	dataC := []byte{2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2}

	fhm, info, _ := filehashmap.NewFileHashMap("test", crt.OpenChaining, 100, 8, 12, nil)
	fmt.Printf("NumberOfBucketsNeeded: %d, NumberOfBucketsAvailable: %d, FileSize: %d\n",
		info.NumberOfBucketsNeeded, info.NumberOfBucketsAvailable, info.FileSize)
	defer fhm.CloseFiles()

	_ = fhm.Set(keyA, dataA)
	_ = fhm.Set(keyB, dataB)
	_ = fhm.Set(keyC, dataC)

	_, _ = fhm.Pop(keyC)

	valueA, _ := fhm.Get(keyA)
	fmt.Printf("valueA: %v\n", valueA)

	valueB, _ := fhm.Get(keyB)
	fmt.Printf("valueB: %v\n", valueB)

	valueC, err := fhm.Get(keyC)
	if errors.Is(err, crt.NoRecordFound{}) {
		fmt.Println("Record not found")
	}
	fmt.Printf("valueC: %v\n", valueC)

	stat, _ := fhm.Stat(true)
	fmt.Printf("Records: %d, MapFileRecords: %d, OverflowRecords: %d, BucketDistribution length: %d\n",
		stat.Records, stat.MapFileRecords, stat.OverflowRecords, len(stat.BucketDistribution))

	_ = fhm.RemoveFiles()
}

// NumberOfBucketsNeeded: 100, NumberOfBucketsAvailable: 128, FileSize: 4736
// valueA: [8 9 10 11 12 13 14 15 16 17 18 19]
// valueB: [19 18 17 16 15 14 13 12 11 10 9 8]
// Record not found
// valueC: []
// Records: 2, MapFileRecords: 2, OverflowRecords: 0, BucketDistribution length: 12
```

## Description
FileHashMap is a filed backed map that can be used to store large amount of data with quick access through 
a hash algorithm that gives a bucket number which in turn more or less directly points out the bucket address in
file. Hence, the map file is a fixed sized file with one record per bucket.

Of course, keys will never be perfectly distributed over available buckets so a collision resolution technique is
needed. The FileHashMap implements several that can be chosen from, and they all work slightly different.
  * Open Chaining
  * Linear Probing
  * Quadratic Probing
  * Double Hashing (not yet implemented)

Out of the four, the Open Chaining is the one that differs the most. It resolves conflict by linking conflicting record
in a linked list. Hence, in FileHashMap it uses two files, one master file called a map file and one overflow file.
The map file is fixed size depending on number of buckets and record lengths (key, value and some header data), whilst the
overflow file grows as more records ends up in overflow due to bucket collisions. If it is highly unknown how many unique
keys will be needed to store, this option is the best.

The other three options uses a fixed size map table only, hence it will not allow any more unique keys than number of
available buckets. Also, they use a probing technique which means at time of collision they seek (using different techniques)
for an available bucket. This can be very time-consuming when number of buckets is very large and when we are having only a few 
free buckets left. Linear and Quadratic Probing also suffers from clustering issues.

#### Note on Quadratic Probing
Quadratic probing uses a quadratic formula to ensure that probing jumps around and not continue to build on a local cluster, but this
also means that without choosing some specific parameters it could end up not finding specific empty records in the map file.

There are several techniques that can be used to solve this issue and the one chosen in this implementation is using a roundUp2(m) function
which can be read about in [Wikipedia](https://en.wikipedia.org/wiki/Quadratic_probing#Quadratic_function) the fifth bullet under examples.

### Creating a file hash map:
The NewFileHashMap function creates a new instance and file(s) are created according choice of collision resolution technique.

The calling parameters are:
  * name - The name of the file hash map that will eventually form the name (and path) of the physical files.
  * crtType - Choice of Collision Resolution Technique (crt.OpenChaining, crt.LinearProbing, crt.QuadraticProbing or crt.DoubleHashing)
  * bucketsNeeded - The number of buckets to create space for in the map file.
  * keyLength - Is the fixed key length that will later be accepted
  * valueLength - Is the fixed value length that will later be accepted
  * hashAlgorithm - Makes it possible to supply your own algorithm (will be discussed further down), set to nil to use the internal one.

```
fhm, info, err := filehashmap.NewFileHashMap("test", crt.OpenChaining, 100, 8, 12, nil)
if err != nil {
    // Do some logging or whatever
    ...
    return
}
defer fhm.CloseFiles()
```

In the example above we create a rather small hash map suited for 100 unique keys.
We decided that the length of each key is 8 bytes and the value we store together with it is 12 bytes long.
We didn't supply any custom bucket algorithm.

Returned data are:
  * fhm - a pointer to the FileHashMap instantiation. It exports only functions:
    * Get(key []byte) (value []byte, err error)
    * Set(key []byte, value []byte) (err error)
    * Pop(key []byte) (value []byte, err error)
    * Stat(includeDistribution bool) (hashMapStat *HashMapStat, err error)
    * CloseFiles()
    * RemoveFiles() (err error)
  * info - a pointer to a HashMapInfo struct which contains:
    * NumberOfBucketsNeeded - Total number of buckets needed as in the call to NewFileHashMap
    * NumberOfBucketsAvailable - Total number of buckets available (this can be a different value compared to NumberOfBucketsNeeded depending on choice of hash algorithm)
    * FileSize - Size of the file created
  * err - which is a standard Go error

### Physical files created
The NewFileHashMap function creates one or two physical files (depending on choice of Collision Resolution Technique); a map file and potentially an overflow file.
File names are constructed using the name that was given in the call to NewFileHashMap.
  * Map file - <name>-map.bin
  * Overflow file - <name>-ovfl.bin

If name includes a path the files will end up in that path, otherwise they will end upp from within where the application
is executed.

The map file is fixed size with a header space of 1024 bytes. Each bucket has its own header consisting of
a one byte header indicating whether the record is empty, deleted or occupied.
In the case of OpenChaining each bucket also has a header of 8 bytes which is the address to any linked list within 
the overflow file (address is uint64(0) until first overflow in a bucket is needed).

The overflow file (if present) has a header of 1024 bytes for future use, current version does not use it. Records in the overflow
file are single linked records, end the entry point to the starting record is held in the bucket header in the map file.
There is no reason to have double linked records since we are talking about files here. If a record that happens to
exist in overflow file is popped, it is very hard to reclaim space overall in the file. Furthermore, to keep track on
every freed record for overall use by all buckets, together with all the extra file access to update double linked list 
would be a performance hit. Instead, a freed record will be reused if a Set with new key on the bucket needs space in overflow.

### Opening an existing file hash map
The NewFromExistingFiles opens an existing file hash map. 
The calling parameters are:
  * name - The name of the hash map, including whatever path the physical files is within.
  * hashAlgorithm - The same, and it has to be the same, algorithm that was used when it was first created (nil if it was first created using internal hash algorithm).

Returned data is the same as for NewFileHashMap.

The map file header contains what collision resolution technique was used when it was first created.

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
The CloseFiles function just closes the physical files.
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
  * Key and/or Value lengths may have to be expanded
  * The nature of the key construction may gain on a customized bucket algorithm to get better distribution over buckets, hence lower the amount of records in overflow.
  * The choice of collision resolution technique may have been wrong.

The ReorgFiles function does that reorganization in one command.

Under the hood it opens the existing files denoted by the parameter "name" and creates a new set of files which has the
names constructed by inserting a "-reorg" in them. The new files are created given configuration data provided to the ReorgFiles function. 
Records are pulled from original files, bucket by bucket, and set in the new files. After processing is finished the original 
files are left in the file system for the caller to decide what to do with them. Auto delete is not done.

Configuration:
```
// ReorgConf - Is a struct used in the call to ReorgFiles holding configuration for the new file structure.
//   - CollisionResolutionTechnique is the new CRT to use
//   - NumberOfBucketsNeeded is the new estimated number of buckets needed store in the hash map files
//   - KeyExtension is number of bytes to extend the key with
//   - PrependKeyExtension whether to prepend the extra space or append it
//   - ValueExtension is number of bytes to extend the value with
//   - PrependValueExtension whether to prepend the extra space or append it
//   - NewHashAlgorithm is the algorithm to use
//   - OldHashAlgorithm is the algorithm that was used in the original file hash map
type ReorgConf struct {
	CollisionResolutionTechnique int
	NumberOfBucketsNeeded        int
	KeyExtension                 int
	PrependKeyExtension          bool
	ValueExtension               int
	PrependValueExtension        bool
	NewHashAlgorithm             hashfunc.HashAlgorithm
	OldHashAlgorithm             hashfunc.HashAlgorithm
}
```

Fill in whatever parameter needs change and leave the others with Go zero values (integers are zero, booleans are false and 
interfaces are nil). Hence, a configuration to change only the length of the value in a record with the extended bytes 
prepended to the existing values will look like this (assuming internal hash algorithm was used in the old file(s):
```
reorgConf := filehashmap.ReorgConf{
	ValueExtension:        10,
	PrependValueExtension: true,
}

fmt.Printf("%+v\n", reorgConf)

// {CollisionResolutionTechnique:0 NumberOfBucketsNeeded:0 KeyExtension:0 PrependKeyExtension:false ValueExtension:10 PrependValueExtension:true NewHashAlgorithm:<nil> OldHashAlgorithm:<nil>}
```

The HashAlgorithm is managed slightly different. Sending in a nil in the config struct will result in reorganization if
the original FileHashMap was created with a custom hash algorithm. Setting the NewHashAlgorithm config parameter to a 
custom one always result in reorganization since the original FileHashMap does only keep track on whether a custom algorithm 
was used or not, but doesn't know which particular one was used.

Given all of the above, sending in an empty ReorgConf struct for a hash map file created with internal hash algorithm will result
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
	"github.com/gostonefire/filehashmap/crt"
)

func main() {
	// Create a new FileHashMap with initialUniqueKeys=100, keyLength=5 and valueLength=10
	fhm, _, _ := filehashmap.NewFileHashMap("test", crt.OpenChaining, 100, 5, 10, nil)

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
	fhm, _, _ = filehashmap.NewFromExistingFiles("test-reorg", nil)
	defer fhm.CloseFiles()

	// Get the stored value given the new key length
	value, _ = fhm.Get([]byte{1, 2, 3, 4, 5, 0, 0, 0, 0, 0})

	fmt.Printf("%+v\n", value)
}

// {NumberOfBucketsNeeded:100 NumberOfBucketsAvailable:128 FileSize:4096}
// {NumberOfBucketsNeeded:100 NumberOfBucketsAvailable:128 FileSize:5376}
// [0 0 0 0 0 10 9 8 7 6 5 4 3 2 1]

// Files after operation:
// test-map.bin
// test-ovfl.bin
// test-reorg-map.bin
// test-reorg-ovfl.bin
```

The file size changed by 1280 bytes which comes from 5 extra key bytes and 5 extra value bytes, times total number of
available buckets which is 128, i.e. (5+5)\*128 = 1280.

We had to use the new key size (5 bytes appended) when getting the original but extended value (5 bytes prepended).

## Operations
#### Set(key []byte, value []byte) (err error)
Sets a new value to the map or updates an existing if the key is already present.

The calling parameters are:
  * key - The key that identifies the record to be written. Must be of same length as indicated when the FileHashMap was created.
  * value - The value of the record to be written. Must be of same length as indicated when the FileHashMap was created.

Returned data is:
  * err - An error of type crt.MapFileFull if no available buckets were found (N/A for crt.OpenChaining) or a standard Go error if something else went wrong.
    For the Quadratic Probing there is also a built-in failsafe that could (but should not) throw an error of type crt.ProbingAlgorithm 

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
  * err - An error of type crt.NoRecordFound if no record was found, or a standard Go error if something else went wrong.
    For the Quadratic Probing there is also a built-in failsafe that could (but should not) throw an error of type crt.ProbingAlgorithm


```
valueC, err := fhm.Get(keyC)
if errors.Is(err, crt.NoRecordFound{}) {
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
  * err - An error of type crt.NoRecordFound if no record was found, or a standard Go error if something else went wrong.
    For the Quadratic Probing there is also a built-in failsafe that could (but should not) throw an error of type crt.ProbingAlgorithm


```
valueC, err := fhm.Pop(keyC)
if errors.Is(err, crt.NoRecordFound{}) {
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

## Custom hash algorithm
When creating a new FileHashMap instance a custom hash algorithm can be supplied given it implements the
hashfunc.HashAlgorithm interface. The reason for doing so can be if the distribution of keys for the data to store is very 
awkward and could gain from having a better fitting algorithm to avoid too much data in overflow.

```
// HashAlgorithm - Interface that permits an implementation using the FileHashMap to supply a custom bucket
// selection algorithm suited for its particular distribution of keys.
type HashAlgorithm interface {
	// UpdateTableSize - Updates the table size for the hash algorithm.
	// This function will be used in for instance Quadratic Probing where we need one extra always empty bucket to
	// stop probing for finding existing records for a Get or for update in a Set
	//   - deltaSize is the number of buckets to extend (or decrease if a negative number is given) the table size with
	UpdateTableSize(deltaSize int64)
	// HashFunc1 - Given key it generates a bucket number between minValue and maxValue (inclusive)
	// Any number returned outside the minValue/maxValue (inclusive) range will result in an error down stream.
	HashFunc1(key []byte) int64
	// HashFunc2 - Given key it generates a bucket number between minValue and maxValue (inclusive)
	// Any number returned outside the minValue/maxValue (inclusive) range will result in an error down stream.
	HashFunc2(key []byte) int64
	// HashFunc1MaxValue - Returns the max value that HashFunc1 will ever return.
	HashFunc1MaxValue() int64
	// HashFunc2MaxValue - Returns the max value that HashFunc2 will ever return.
	HashFunc2MaxValue() int64
	// CombinedHash - Returns a combined hash value given values from hash functions 1 and 2 with iteration.
	CombinedHash(hashValue1, hashValue2, iteration int64) int64
}
```

The internal implementation should result in good enough keys for most situation though:
NOTE! It will likely change once Double Hashing is implemented.

```
import (
	"hash/crc32"
	"math"
)

// SingleHashAlgorithm - The internally used bucket selection algorithm is implemented using crc32.ChecksumIEEE to
// create a hash value over the key and then applying bucket = hash & (1<<exp - 1) to get the bucket number,
// where 1<<exp (2 to the power of exp) is the total number of buckets to distribute over.
type SingleHashAlgorithm struct {
	tableSize int64
	exp       int64
}

// NewSingleHashAlgorithm - Returns a pointer to a new SingleHashAlgorithm instance
// It sets an initial value for the table size but that size may be updated to a new value depending on
// chosen Collision Probing Algorithm
func NewSingleHashAlgorithm(tableSize int64) *SingleHashAlgorithm {
	ha := &SingleHashAlgorithm{}
	ha.UpdateTableSize(tableSize)
	return ha
}

// UpdateTableSize - Updates the table size for the hash algorithm.
// This function will be used in for instance Quadratic Probing where we need one extra always empty bucket to
// stop probing for finding existing records for a Get or for update in a Set
//   - deltaSize is the number of buckets to extend (or decrease if a negative number is given) the table size with
func (B *SingleHashAlgorithm) UpdateTableSize(deltaSize int64) {
	B.tableSize += deltaSize
	B.exp = int64(math.Ceil(math.Log2(float64(B.tableSize)) / math.Log2(2)))
}

// HashFunc1 - Given key it generates a bucket number between minValue and maxValue (inclusive)
func (B *SingleHashAlgorithm) HashFunc1(key []byte) int64 {
	h := int64(crc32.ChecksumIEEE(key))
	return h & (1<<B.exp - 1)
}

// HashFunc2 - Given key it generates a bucket number between minValue and maxValue (inclusive)
// This function is only used in Double Hash algorithms, but implemented here to follow the interface.
func (B *SingleHashAlgorithm) HashFunc2(key []byte) int64 {
	h := int64(crc32.ChecksumIEEE(key))
	return h % B.tableSize
}

// HashFunc1MaxValue - Returns the max value that HashFunc1 will ever return.
func (B *SingleHashAlgorithm) HashFunc1MaxValue() int64 {
	return 1<<B.exp - 1
}

// HashFunc2MaxValue - Returns the max value that HashFunc2 will ever return.
// This function is only used in Double Hash algorithms, but implemented here to follow the interface.
func (B *SingleHashAlgorithm) HashFunc2MaxValue() int64 {
	return B.tableSize - 1
}

// CombinedHash - Returns a combined hash value given values from hash functions 1 and 2 with iteration.
// This function is only used in Double Hash algorithms, but implemented here to follow the interface.
func (B *SingleHashAlgorithm) CombinedHash(hashValue1, hashValue2, iteration int64) int64 {
	return (hashValue1 + iteration*hashValue2) % B.tableSize
}
```
