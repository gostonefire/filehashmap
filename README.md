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

	fhm, info, _ := filehashmap.NewFileHashMap("test", crt.SeparateChaining, 10, 2, 8, 12, nil)
	fmt.Printf("NumberOfBucketsNeeded: %d, NumberOfBucketsAvailable: %d, TotalRecords: %d, FileSize: %d\n",
		info.NumberOfBucketsNeeded, info.NumberOfBucketsAvailable, info.TotalRecords, info.FileSize)
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

	fmt.Printf("BucketDistribution: %+v\n", stat.BucketDistribution)

	_ = fhm.RemoveFiles()
}

// NumberOfBucketsNeeded: 10, NumberOfBucketsAvailable: 16, TotalRecords: 32, FileSize: 1824
// valueA: [8 9 10 11 12 13 14 15 16 17 18 19]
// valueB: [19 18 17 16 15 14 13 12 11 10 9 8]
// Record not found
// valueC: []
// Records: 2, MapFileRecords: 2, OverflowRecords: 0, BucketDistribution length: 16
// BucketDistribution: [0 0 0 0 0 0 0 0 1 0 0 0 0 0 0 1]
```

In the example above we created a file hash map with 10 buckets needed and 2 records per bucket. The hash algorithm used
is utilizing a bucket selection technique using the bitwise AND, it needs number of bucket to be 2 to the power of X so 
the actual number of buckets became 16 and since we wanted 2 records per bucket the total number of records in the map file 
is 32.

## Description
FileHashMap is a filed backed map that can be used to store large amount of data with quick access through 
a hash algorithm that gives a bucket number which in turn more or less directly points out the bucket address in
file. Hence, the map file is a fixed sized file with one record per bucket.

Of course, keys will never be perfectly distributed over available buckets so a collision resolution technique is
needed. The FileHashMap implements several that can be chosen from, and they all work slightly different.
  * Separate Chaining
  * Linear Probing
  * Quadratic Probing
  * Double Hashing

Out of the four, the Separate Chaining is the one that differs the most. It resolves conflict by linking conflicting record
in a linked list. Hence, in FileHashMap it uses two files, one master file called a map file and one overflow file.
The map file is fixed size depending on number of buckets and record lengths (key, value and some header data), whilst the
overflow file grows as more records ends up in overflow due to bucket collisions. If it is highly unknown how many unique
keys will be needed to store, this option is the best.

The other three options uses a fixed size map table only, hence it will not allow any more unique keys than number of
available buckets. Also, they use probing, which means at time of collision they seek (using different techniques)
for an available bucket. This can be very time-consuming when number of buckets is very large and when we are having only a few 
free buckets left. Linear and Quadratic Probing also suffers from clustering issues.

With all Open Addressing collision techniques implemented (Linear Probing, Quadratic Probing and Double Hashing) comes that 
one should keep utilization below a load factor of some 75%. This has to do with the probe iterations that gets longer the harder it is to find a
free bucket (or finding a record in a get/pop of course). 

Below are some benchmarks between the four available collision resolution techniques. They were executed using a quite standard 
laptop with SSD disk and used 1048576 actual buckets (a few more in Double Hashing since that one rounds up to the nearest prime).
Random data was inserted until hash maps was completely full via Set operations, and each Set operation was timed.
Each measure in the table represents max/avg statistics over 10% of the total operation from start to end (min values was always 0.000000 given 
precision used in display). There isn't any dramatic difference between them besides the last 10% where all the Open Addressing techniques 
started to go up with max insertion time over 4 seconds for Linear Probing. Whilst Separate Chaining didn't suffer in the end since 
it just links another record in the overflow file when map file bucket is occupied.

```
Separate Chaining: max -> 0.027754, 0.010190, 0.010167, 0.010115, 0.010139, 0.010224, 0.010142, 0.010201, 0.010109, 0.010321
Linear Probing:    max -> 0.014198, 0.010556, 0.010097, 0.010608, 0.010248, 0.010179, 0.010226, 0.010338, 0.010661, 4.273701
Quadratic Probing: max -> 0.012590, 0.010109, 0.010177, 0.010504, 0.010526, 0.010103, 0.010323, 0.010201, 0.010192, 3.464684
Double Hashing:    max -> 0.013908, 0.010116, 0.010514, 0.010242, 0.010168, 0.010113, 0.010182, 0.010210, 0.010275, 2.047390

Separate Chaining: avg -> 0.000020, 0.000013, 0.000014, 0.000014, 0.000014, 0.000015, 0.000017, 0.000016, 0.000018, 0.000018
Linear Probing:    avg -> 0.000016, 0.000013, 0.000012, 0.000014, 0.000016, 0.000019, 0.000024, 0.000038, 0.000098, 0.020106
Quadratic Probing: avg -> 0.000017, 0.000011, 0.000011, 0.000013, 0.000014, 0.000015, 0.000019, 0.000024, 0.000035, 0.000535
Double Hashing:    avg -> 0.000016, 0.000012, 0.000017, 0.000015, 0.000014, 0.000015, 0.000020, 0.000024, 0.000036, 0.000472
```

So how does this work in relation to choosing more than one record per bucket (as was the case above)?
Well, in the lower end it didn't seem to matter that much, but when available free records started to become sparse there
is a difference for some techniques. Records per bucket chosen was 1, 2, 4, 8 and 16.

```
Separate Chaining:
  1:  max -> 0.027754, 0.010190, 0.010167, 0.010115, 0.010139, 0.010224, 0.010142, 0.010201, 0.010109, 0.010321
  2:  max -> 0.015573, 0.010108, 0.010119, 0.010110, 0.015643, 0.010157, 0.010176, 0.010185, 0.010187, 0.024244
  4:  max -> 0.010245, 0.010080, 0.010098, 0.010175, 0.010112, 0.010129, 0.010541, 0.010112, 0.010110, 0.010103
  8:  max -> 0.011108, 0.010111, 0.010589, 0.010164, 0.010152, 0.010165, 0.010101, 0.010148, 0.010179, 0.010444
  16: max -> 0.018398, 0.010098, 0.010221, 0.010131, 0.010305, 0.010187, 0.010099, 0.010134, 0.010269, 0.010114

  1:  avg -> 0.000020, 0.000013, 0.000014, 0.000014, 0.000014, 0.000015, 0.000017, 0.000016, 0.000018, 0.000018
  2:  avg -> 0.000017, 0.000011, 0.000011, 0.000013, 0.000012, 0.000014, 0.000015, 0.000016, 0.000016, 0.000018
  4:  avg -> 0.000016, 0.000010, 0.000010, 0.000011, 0.000012, 0.000013, 0.000013, 0.000015, 0.000016, 0.000018
  8:  avg -> 0.000018, 0.000012, 0.000012, 0.000013, 0.000012, 0.000012, 0.000014, 0.000015, 0.000018, 0.000020
  16: avg -> 0.000017, 0.000012, 0.000012, 0.000012, 0.000011, 0.000012, 0.000013, 0.000013, 0.000016, 0.000021
  
Linear Probing:
  1:  max -> 0.014198, 0.010556, 0.010097, 0.010608, 0.010248, 0.010179, 0.010226, 0.010338, 0.010661, 4.273701
  2:  max -> 0.012113, 0.010195, 0.010125, 0.010115, 0.010126, 0.010286, 0.010145, 0.010203, 0.010608, 1.744032
  4:  max -> 0.010079, 0.010129, 0.010162, 0.010323, 0.010126, 0.010153, 0.010573, 0.010222, 0.010635, 1.184521
  8:  max -> 0.016931, 0.010136, 0.010102, 0.010336, 0.010313, 0.010135, 0.010473, 0.010266, 0.010597, 0.624483
  16: max -> 0.010195, 0.010185, 0.010135, 0.010342, 0.010593, 0.010114, 0.010100, 0.010104, 0.010552, 0.329848

  1:  avg -> 0.000016, 0.000013, 0.000012, 0.000014, 0.000016, 0.000019, 0.000024, 0.000038, 0.000098, 0.020106
  2:  avg -> 0.000016, 0.000011, 0.000011, 0.000010, 0.000013, 0.000014, 0.000016, 0.000029, 0.000054, 0.011397
  4:  avg -> 0.000014, 0.000011, 0.000011, 0.000011, 0.000011, 0.000012, 0.000013, 0.000016, 0.000031, 0.007075
  8:  avg -> 0.000017, 0.000011, 0.000013, 0.000012, 0.000012, 0.000012, 0.000013, 0.000015, 0.000023, 0.004175
  16: avg -> 0.000017, 0.000012, 0.000012, 0.000013, 0.000012, 0.000011, 0.000012, 0.000013, 0.000018, 0.001493
  
Quadratic Probing:
  1:  max -> 0.012590, 0.010109, 0.010177, 0.010504, 0.010526, 0.010103, 0.010323, 0.010201, 0.010192, 3.464684
  2:  max -> 0.023267, 0.010106, 0.010117, 0.010507, 0.010104, 0.010361, 0.010439, 0.010139, 0.010584, 1.998877
  4:  max -> 0.010726, 0.010150, 0.010159, 0.010111, 0.010106, 0.010617, 0.010396, 0.010140, 0.010133, 0.725316
  8:  max -> 0.016951, 0.010167, 0.004710, 0.004628, 0.005023, 0.010105, 0.010118, 0.010234, 0.010134, 0.301852
  16: max -> 0.012082, 0.010282, 0.010176, 0.010207, 0.010593, 0.010587, 0.010114, 0.010224, 0.010119, 0.288709

  1:  avg -> 0.000017, 0.000011, 0.000011, 0.000013, 0.000014, 0.000015, 0.000019, 0.000024, 0.000035, 0.000535
  2:  avg -> 0.000017, 0.000010, 0.000012, 0.000012, 0.000012, 0.000013, 0.000015, 0.000017, 0.000026, 0.000331
  4:  avg -> 0.000016, 0.000012, 0.000012, 0.000011, 0.000013, 0.000012, 0.000015, 0.000015, 0.000021, 0.000190
  8:  avg -> 0.000017, 0.000013, 0.000012, 0.000013, 0.000013, 0.000013, 0.000013, 0.000014, 0.000018, 0.000150
  16: avg -> 0.000017, 0.000011, 0.000011, 0.000012, 0.000012, 0.000012, 0.000012, 0.000013, 0.000016, 0.000103
  
Double Hashing:
  1:  max -> 0.013908, 0.010116, 0.010514, 0.010242, 0.010168, 0.010113, 0.010182, 0.010210, 0.010275, 2.047390
  2:  max -> 0.016695, 0.010098, 0.010123, 0.010284, 0.010135, 0.010292, 0.010110, 0.010143, 0.010508, 0.685892
  4:  max -> 0.026722, 0.010101, 0.010231, 0.010151, 0.010586, 0.010598, 0.010308, 0.010320, 0.010205, 0.901556
  8:  max -> 0.012183, 0.010095, 0.010126, 0.010099, 0.010181, 0.010148, 0.010184, 0.010256, 0.010205, 0.381992
  16: max -> 0.020620, 0.010135, 0.010132, 0.010584, 0.010185, 0.010601, 0.010177, 0.010127, 0.010201, 0.285015

  1:  avg -> 0.000016, 0.000012, 0.000017, 0.000015, 0.000014, 0.000015, 0.000020, 0.000024, 0.000036, 0.000472
  2:  avg -> 0.000014, 0.000010, 0.000011, 0.000012, 0.000012, 0.000016, 0.000017, 0.000019, 0.000027, 0.000298
  4:  avg -> 0.000016, 0.000012, 0.000011, 0.000011, 0.000012, 0.000013, 0.000015, 0.000017, 0.000024, 0.000227
  8:  avg -> 0.000015, 0.000011, 0.000011, 0.000012, 0.000011, 0.000012, 0.000012, 0.000015, 0.000018, 0.000122
  16: avg -> 0.000017, 0.000012, 0.000012, 0.000012, 0.000012, 0.000012, 0.000012, 0.000012, 0.000018, 0.000098
```

Overall difference (savings in choosing higher numbers) in seconds between 1 record per bucket and 2, 4, 8, 16 respectively is
shown below.
The reason for Linear Probing having such high numbers is probably due to its characteristic of building local clusters, hence providing
more than one record per bucket gives good effect.

```
Separate Chaining:
1 -> 2:  1.605016
1 -> 4:  2.521402
1 -> 8:  1.445436
1 -> 16: 2.040356

Linear Probing:
1 -> 2:  921.009023
1 -> 4:  1378.599241
1 -> 8:  1682.944396
1 -> 16: 1965.012142

Quadratic Probing:
1 -> 2:  24.147140
1 -> 4:  39.721233
1 -> 8:  43.848376
1 -> 16: 49.842142

Double Hashing:
1 -> 2:  28.021918
1 -> 4:  33.052511
1 -> 8:  46.482506
1 -> 16: 45.679543
```

#### Note on Quadratic Probing
Quadratic probing uses a quadratic formula to ensure that probing jumps around and not continue to build on a local cluster, but this
also means that without choosing some specific parameters it could end up not finding specific empty records in the map file.

There are several techniques that can be used to solve this issue and the one chosen in this implementation is using a roundUp2(m) function
which can be read about in [Wikipedia](https://en.wikipedia.org/wiki/Quadratic_probing#Quadratic_function) the fifth bullet under examples.

#### Note on Double Hashing
Double Hashing uses two hash functions to solve collisions, the first gives a primary value pointing to a bucket and if that bucket is 
already occupied it uses a second hash function to get an offset from the first. As with Quadratic Probing it is important to choose 
parameters and a probing function that guarantees that all buckets are visited in a search for a free spot. There are several techniques 
that can be used to guarantee that, and the one chosen is the following:
  * The table size is rounded up to the nearest prime number
  * Hash function 1 uses a divisor of table size (`k % tableSize`) to return the primary value pointing to a bucket
  * Hash function 2 uses the following formula for the offset: `1 + ((k / tableSize) % (tableSize - 1))`
  * The probing function used to iterate until a free bucket is found is implemented as: `(hf1Value + iteration * hf2Value) % tableSize`

All this is implemented in the hash algorithm, which can be supplied as a custom hash algorithm if something better/other is of interest.
See section [Custom hash algorithm](https://github.com/gostonefire/filehashmap#custom-hash-algorithm) further down below.

### Creating a file hash map:
The NewFileHashMap function creates a new instance and file(s) are created according choice of collision resolution technique.

The calling parameters are:
  * name - The name of the file hash map that will eventually form the name (and path) of the physical files.
  * crtType - Choice of Collision Resolution Technique (crt.SeparateChaining, crt.LinearProbing, crt.QuadraticProbing or crt.DoubleHashing)
  * bucketsNeeded - The number of buckets to create space for in the map file.
  * recordsPerBucket - The number of records to hold in each bucket in the map file. Min value is 1 and any value given below 1 will result in 1 used effectively.
  * keyLength - Is the fixed key length that will later be accepted
  * valueLength - Is the fixed value length that will later be accepted
  * hashAlgorithm - Makes it possible to supply your own algorithm (will be discussed further down), set to nil to use the internal one.

```
fhm, info, err := filehashmap.NewFileHashMap("test", crt.SeparateChaining, 100, 1, 8, 12, nil)
if err != nil {
    // Do some logging or whatever
    ...
    return
}
defer fhm.CloseFiles()
```

In the example above we create a rather small hash map with 100 buckets needed and 1 record per bucket.
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
    * TotalRecords - The total number of records available in the hash map file (not including overflow). This value is recordsPerBucket * NumberOfBucketsAvailable.
    * FileSize - Size of the file created
  * err - which is a standard Go error

### Physical files created
The NewFileHashMap function creates one or two physical files (depending on choice of Collision Resolution Technique); a map file and potentially an overflow file.
File names are constructed using the name that was given in the call to NewFileHashMap.
  * Map file - \<name\>-map.bin
  * Overflow file - \<name\>-ovfl.bin

If name includes a path the files will end up in that path, otherwise they will end upp from within where the application
is executed.

The map file is fixed size with a header space of 1024 bytes. Each bucket has a number of records depending on the recordsPerBucket parameter,
and each record has a one byte header indicating whether the record is empty, deleted or occupied.
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
  * The nature of the key construction may gain on a customized hash algorithm to get better distribution over buckets, hence lower the amount of records in overflow.
  * The choice of collision resolution technique may have been wrong.

The ReorgFiles function does that reorganization in one go.

Under the hood it opens the existing files denoted by the parameter "name" and creates a new set of files which has the
names constructed by inserting a "-reorg" in them. The new files are created given configuration data provided to the ReorgFiles function. 
Records are pulled from original files, bucket by bucket, and set in the new files. After processing is finished the original 
files are left in the file system for the caller to decide what to do with them. Auto delete is not done.

Configuration:
```
// ReorgConf - Is a struct used in the call to ReorgFiles holding configuration for the new file structure.
//   - CollisionResolutionTechnique is the new CRT to use
//   - NumberOfBucketsNeeded is the new estimated number of buckets needed to store in the hash map files
//   - RecordsPerBucket is the new number of records per bucket
//   - KeyExtension is number of bytes to extend the key with
//   - PrependKeyExtension whether to prepend the extra space or append it
//   - ValueExtension is number of bytes to extend the value with
//   - PrependValueExtension whether to prepend the extra space or append it
//   - NewHashAlgorithm is the algorithm to use
//   - OldHashAlgorithm is the algorithm that was used in the original file hash map
type ReorgConf struct {
	CollisionResolutionTechnique int
	NumberOfBucketsNeeded        int
	RecordsPerBucket             int
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

// {CollisionResolutionTechnique:0 NumberOfBucketsNeeded:0 RecordsPerBucket:0 KeyExtension:0 PrependKeyExtension:false ValueExtension:10 PrependValueExtension:true NewHashAlgorithm:<nil> OldHashAlgorithm:<nil>}
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
	// Create a new FileHashMap with initialUniqueKeys=100, recordsPerBucket=1, keyLength=5 and valueLength=10
	fhm, _, _ := filehashmap.NewFileHashMap("test", crt.SeparateChaining, 100, 1, 5, 10, nil)

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

// {NumberOfBucketsNeeded:100 NumberOfBucketsAvailable:128 TotalRecords:128 FileSize:4096}
// {NumberOfBucketsNeeded:100 NumberOfBucketsAvailable:128 TotalRecords:128 FileSize:5376}
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
    For the Open Addressing resolution techniques (Linear/Quadratic Probing and Double Hashing) there is also a built-in failsafe that could (but should not) 
    throw an error of type crt.ProbingAlgorithm. This might happen if a custom hash algorithm is used, and it does not guarantee to not end up in looping through
    a subset of buckets.

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
    For the Open Addressing resolution techniques (Linear/Quadratic Probing and Double Hashing) there is also a built-in failsafe that could (but should not)
    throw an error of type crt.ProbingAlgorithm. This might happen if a custom hash algorithm is used, and it does not guarantee to not end up in looping through
    a subset of buckets.

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
    For the Open Addressing resolution techniques (Linear/Quadratic Probing and Double Hashing) there is also a built-in failsafe that could (but should not)
    throw an error of type crt.ProbingAlgorithm. This might happen if a custom hash algorithm is used, and it does not guarantee to not end up in looping through
    a subset of buckets.

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
awkward and could gain from having a better fitting algorithm to avoid too much data in overflow or in collision.

When using a custom hash algorithm the function SetTableSize will be called with the numberOfBuckets that was supplied 
when the new file hash map is/was created. Hence, any size that was given when the custom HashAlgorithm was instantiated 
will be overwritten (assuming the interface is implemented correctly).

```
// HashAlgorithm - Interface that permits an implementation using the FileHashMap to supply a custom bucket
// selection algorithm suited for its particular distribution of keys.
type HashAlgorithm interface {
	// SetTableSize - Sets the table size for the hash algorithm. 
	// It is called both when creating a new file hash map and when opening an existing one. Hence, if a custom
	// hash algorithm is supplied that implements this interface and the instance is already having a table size, it 
	// will be overwritten by the number of buckets that is/was supplied when creating the file hash map.
	//   - tableSize is the number of buckets the map file will address
	SetTableSize(tableSize int64)

	// HashFunc1 - Given key it generates an index (bucket) between 0 and table size - 1
	// Any number returned outside the table size (0 -> table size - 1) will result in an error down stream.
	HashFunc1(key []byte) int64

	// HashFunc2 - Given key it generates an offset probing value that will be used together with the value from HashFunc1 in
	// a call to DoubleHashFunc. The function is only used for the Double Hashing Collision Resolution Technique.
	HashFunc2(key []byte) int64

	// GetTableSize - Returns the table size the implemented hash functions are supporting
	// It is very important that this function return the actual table size and not just the table size given at instantiating time or
	// in a call to SetTableSize. Some algorithms are implemented by rounding up to nearest 2 to the power of x, or to the nearest prime, and
	// if such operations are built in the implementation of this interface it must be covered in the GetTableSize.
	GetTableSize() int64

	// ProbeIteration - Returns a combined hash value given values from HashFunc1 and HashFunc2 in iteration.
	// Since this function will be called repeatedly in a collision resolution situation, and the actual hash values
	// from the HashFunc1 and HashFunc2 are the same throughout iterations for one key, the function takes those values rather than
	// using the actual key as input.
	// For some probing algorithms it may be that they return a probing value outside the hash table bucket range, that is
	// alright, the internal loop will then just increment the iteration by one and call this function again.
	// The function is not used for Open Chaining Collision Resolution Technique.
	ProbeIteration(hf1Value, hf2Value, iteration int64) int64
}
```

The internal implementations should result in good enough keys for most situation though:

#### Separate Chaining algorithm
```
import (
	"github.com/gostonefire/filehashmap/internal/utils"
	"hash/crc32"
)

// SeparateChainingHashAlgorithm - The internally used bucket selection algorithm is implemented using crc32.ChecksumIEEE to
// create a hash value over the key and then applying bucket = hash & (actualTableSize - 1) to get the bucket number,
// where actualTableSize is the nearest bigger exponent of 2 of the requested table size.
type SeparateChainingHashAlgorithm struct {
	tableSize       int64
}

// NewSeparateChainingHashAlgorithm - Returns a pointer to a new SeparateChainingHashAlgorithm instance
func NewSeparateChainingHashAlgorithm(tableSize int64) *SeparateChainingHashAlgorithm {
	ha := &SeparateChainingHashAlgorithm{}
	ha.SetTableSize(tableSize)
	return ha
}

// SetTableSize - Sets the table size for the hash algorithm.
// In this implementation it updates the table size to the nearest bigger exponent of 2 of the requested table size.
//   - tableSize is the number of buckets the map file will address
func (O *SeparateChainingHashAlgorithm) SetTableSize(tableSize int64) {
	O.tableSize = utils.RoundUp2(tableSize)
}

// HashFunc1 - Given key it generates an index (bucket) between 0 and table size - 1
func (O *SeparateChainingHashAlgorithm) HashFunc1(key []byte) int64 {
	h := int64(crc32.ChecksumIEEE(key))
	return h & (O.tableSize - 1)
}

// HashFunc2 - Not used in open chaining probing collision resolution techniques, returns a dummy value
func (O *SeparateChainingHashAlgorithm) HashFunc2(key []byte) int64 {
	return 0
}

// GetTableSize - Returns the table size the implemented hash functions are supporting
func (O *SeparateChainingHashAlgorithm) GetTableSize() int64 {
	return O.tableSize
}

// ProbeIteration - Not used in open chaining probing collision resolution techniques, returns a dummy value
func (O *SeparateChainingHashAlgorithm) ProbeIteration(hf1Value, hf2Value, iteration int64) int64 {
	return 0
}
```

#### Linear Probing algorithm
```
import (
	"github.com/gostonefire/filehashmap/internal/utils"
	"hash/crc32"
)

// LinearProbingHashAlgorithm - The internally used bucket selection algorithm is implemented using crc32.ChecksumIEEE to
// create a hash value over the key and then applying bucket = hash & (actualTableSize - 1) to get the bucket number,
// where actualTableSize is the nearest bigger exponent of 2 of the requested table size.
type LinearProbingHashAlgorithm struct {
	tableSize       int64
}

// NewLinearProbingHashAlgorithm - Returns a pointer to a new LinearProbingHashAlgorithm instance
// It sets an initial value for the table size but that size may be updated to a new value depending on
// chosen Collision Probing Algorithm
func NewLinearProbingHashAlgorithm(tableSize int64) *LinearProbingHashAlgorithm {
	ha := &LinearProbingHashAlgorithm{}
	ha.SetTableSize(tableSize)
	return ha
}

// SetTableSize - Sets the table size for the hash algorithm.
// In this implementation it updates the table size to the nearest bigger exponent of 2 of the requested table size.
func (L *LinearProbingHashAlgorithm) SetTableSize(tableSize int64) {
	L.tableSize = utils.RoundUp2(tableSize)
}

// HashFunc1 - Given key it generates an index (bucket) between 0 and table size - 1
func (L *LinearProbingHashAlgorithm) HashFunc1(key []byte) int64 {
	h := int64(crc32.ChecksumIEEE(key))
	return h & (L.tableSize - 1)
}

// HashFunc2 - Not used in linear probing collision resolution techniques, returns a dummy value
func (L *LinearProbingHashAlgorithm) HashFunc2(key []byte) int64 {
	return 0
}

// GetTableSize - Returns the table size the implemented hash functions are supporting
func (L *LinearProbingHashAlgorithm) GetTableSize() int64 {
	return L.tableSize
}

// ProbeIteration - Implements Linear Probing
func (L *LinearProbingHashAlgorithm) ProbeIteration(hf1Value, hf2Value, iteration int64) int64 {
	probe := hf1Value + iteration
	if probe >= L.tableSize {
		probe -= L.tableSize
	}

	return probe
}
```

#### Quadratic Probing algorithm
```
import (
	"github.com/gostonefire/filehashmap/internal/utils"
	"hash/crc32"
)

// QuadraticProbingHashAlgorithm - The internally used bucket selection algorithm is implemented using crc32.ChecksumIEEE to
// create a hash value over the key and then applying bucket = hash & (actualTableSize - 1) to get the bucket number,
// where actualTableSize is the nearest bigger exponent of 2 of the requested table size.
type QuadraticProbingHashAlgorithm struct {
	tableSize       int64
	roundUp2        int64
}

// NewQuadraticProbingHashAlgorithm - Returns a pointer to a new QuadraticProbingHashAlgorithm instance
func NewQuadraticProbingHashAlgorithm(tableSize int64) *QuadraticProbingHashAlgorithm {
	ha := &QuadraticProbingHashAlgorithm{}
	ha.SetTableSize(tableSize)
	return ha
}

// SetTableSize - Sets the table size for the hash algorithm.
// In this implementation it updates the table size to the nearest bigger exponent of 2 of the requested table size.
// The extra RoundUp2 seems a little redundant, but the use of the two attributes makes it a little easier to
// remember where the algorithm comes from, should it be switched to a divisor type of hashing in the future.
func (Q *QuadraticProbingHashAlgorithm) SetTableSize(tableSize int64) {
	Q.tableSize = utils.RoundUp2(tableSize)
	Q.roundUp2 = utils.RoundUp2(Q.tableSize)
}

// HashFunc1 - Given key it generates an index (bucket) between 0 and table size - 1
func (Q *QuadraticProbingHashAlgorithm) HashFunc1(key []byte) int64 {
	h := int64(crc32.ChecksumIEEE(key))
	return h & (Q.tableSize - 1)
}

// HashFunc2 - Not used in quadratic probing collision resolution techniques, returns a dummy value
func (Q *QuadraticProbingHashAlgorithm) HashFunc2(key []byte) int64 {
	return 0
}

// GetTableSize - Returns the table size the implemented hash functions are supporting
func (Q *QuadraticProbingHashAlgorithm) GetTableSize() int64 {
	return Q.tableSize
}

// ProbeIteration - Implements Quadratic Probing
func (Q *QuadraticProbingHashAlgorithm) ProbeIteration(hf1Value, hf2Value, iteration int64) int64 {
	probe := (hf1Value + ((iteration*iteration + iteration) / 2)) % Q.roundUp2

	return probe
}
```

#### Double Hashing algorithm
```
import "hash/crc32"

// DoubleHashAlgorithm - The internally used bucket selection algorithm is implemented using crc32.ChecksumIEEE to
// create a hash value over the key and then applying HashFunc1 and HashFunc2 as primary respective probing functions.
type DoubleHashAlgorithm struct {
	tableSize int64
}

// NewDoubleHashAlgorithm - Returns a pointer to a new DoubleHashAlgorithm instance
func NewDoubleHashAlgorithm(tableSize int64) *DoubleHashAlgorithm {
	ha := &DoubleHashAlgorithm{}
	ha.SetTableSize(tableSize)
	return ha
}

// SetTableSize - Sets the table size for the hash algorithm.
// In this implementation it updates the table size to its nearest higher prime number, which allows the algorithm to
// iterate over the entirety of the tables buckets once and only once.
//   - tableSize is the number of buckets the map file will address
func (D *DoubleHashAlgorithm) SetTableSize(tableSize int64) {
	D.tableSize = tableSize
	D.updateToNearestPrime()
}

// HashFunc1 - Given key it generates an index (bucket) between 0 and table size - 1
func (D *DoubleHashAlgorithm) HashFunc1(key []byte) int64 {
	k := int64(crc32.ChecksumIEEE(key))
	return k % D.tableSize
}

// HashFunc2 - Given key it generates an offset probing value that will be used together with the value from HashFunc1 in
// a call to DoubleHashFunc.
func (D *DoubleHashAlgorithm) HashFunc2(key []byte) int64 {
	k := int64(crc32.ChecksumIEEE(key))

	return 1 + ((k / D.tableSize) % (D.tableSize - 1))
}

// GetTableSize - Returns the table size the implemented hash functions are supporting
func (D *DoubleHashAlgorithm) GetTableSize() int64 {
	return D.tableSize
}

// ProbeIteration - Returns a combined hash value given values from HashFunc1 and HashFunc2 in iteration.
// Since this function will be called repeatedly in a collision resolution situation, and the actual hash values
// from the HashFunc1 and HashFunc2 are the same throughout iterations for one key, the function takes those values rather than
// using the actual key as input.
func (D *DoubleHashAlgorithm) ProbeIteration(hf1Value, hf2Value, iteration int64) int64 {
	return (hf1Value + iteration*hf2Value) % D.tableSize
}

// updateToNearestPrime - To ensure that we don't end up in an infinite loop when probing, the easiest way is to
// ensure the table size is a prime number. This function updates the table size to nearest higher prime number.
func (D *DoubleHashAlgorithm) updateToNearestPrime() {
	n := D.tableSize

OUTER:
	for {
		if n == 2 || n == 3 {
			D.tableSize = n
			return
		}

		if n <= 1 || n%2 == 0 || n%3 == 0 {
			n++
			continue
		}

		for i := int64(5); i*i <= n; i += 6 {
			if n%i == 0 || n%(i+2) == 0 {
				n++
				continue OUTER
			}
		}

		D.tableSize = n
		return
	}
}
```

The Round2Up function that is used in some hash algorithms is implemented as:
```
// RoundUp2 - Rounds up to the nearest exponent of 2
func RoundUp2(a int64) int64 {
	r := uint64(a - 1)
	r |= r >> 1
	r |= r >> 2
	r |= r >> 4
	r |= r >> 8
	r |= r >> 16
	r |= r >> 32
	return int64(r + 1)
}
```
