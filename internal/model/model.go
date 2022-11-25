package model

import hashfunc "github.com/gostonefire/filehashmap/interfaces"

// RecordEmpty - State indicating a record that is or has never been in use
const RecordEmpty uint8 = 0

// RecordOccupied - State indicating a record that is in use
const RecordOccupied uint8 = 1

// RecordDeleted - State indicating a record that has been in use but was deleted
const RecordDeleted uint8 = 2

// Bucket - Represents all records in a bucket (both assigned and still not in use)
type Bucket struct {
	Record          Record
	BucketAddress   int64
	OverflowAddress int64
	HasOverflow     bool
}

// Record - Represents one record in a bucket
type Record struct {
	State         uint8
	IsOverflow    bool
	RecordAddress int64
	NextOverflow  int64
	Key           []byte
	Value         []byte
}

// StorageParameters - Represents parameters specific for any implementation of storage
type StorageParameters struct {
	CollisionResolutionTechnique int
	KeyLength                    int64
	ValueLength                  int64
	NumberOfBucketsNeeded        int64
	NumberOfBucketsAvailable     int64
	MapFileSize                  int64
	InternalAlgorithm            bool
}

// CRTConf - Is a struct to be passed in the call to NewXXFiles and contains configuration that affects
// file processing.
//   - Name is the name to base map and overflow file names on
//   - NumberOfBucketsNeeded is the number of buckets to calculate storage for
//   - KeyLength is the fixed length of keys to store
//   - ValueLength is the fixed length of values to store
//   - HashAlgorithm is the hash function(s) to use
type CRTConf struct {
	Name                  string
	NumberOfBucketsNeeded int64
	KeyLength             int64
	ValueLength           int64
	HashAlgorithm         hashfunc.HashAlgorithm
}
