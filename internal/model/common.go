package model

// Header - Represents the hash map file header data
type Header struct {
	InternalAlg       bool
	InitialUniqueKeys int64
	KeyLength         int64
	ValueLength       int64
	RecordsPerBucket  int64
	NumberOfBuckets   int64
	MinBucketNo       int64
	MaxBucketNo       int64
	FileSize          int64
}

// Bucket - Represents all records in a bucket (both assigned and still not in use)
type Bucket struct {
	Records         []Record
	BucketAddress   int64
	OverflowAddress int64
	HasOverflow     bool
}

// Record - Represents one record in a bucket
type Record struct {
	InUse         bool
	IsOverflow    bool
	RecordAddress int64
	NextOverflow  int64
	Key           []byte
	Value         []byte
}

// StorageParameters - Represents parameters specific for any implementation of storage
type StorageParameters struct {
	InitialUniqueKeys int64
	KeyLength         int64
	ValueLength       int64
	RecordsPerBucket  int64
	NumberOfBuckets   int64
	FillFactor        float64
	MapFileSize       int64
	InternalAlgorithm bool
}
