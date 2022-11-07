package conf

// InUseFlagBytes - Number of bytes used to indicate whether an index record is in use or not,
// it is added to each record when written to the index file
const InUseFlagBytes int64 = 1

// OvflFileHeaderLength - Length of overflow file header
const OvflFileHeaderLength int64 = 1024

// OverflowAddressLength - Length of address to next record in overflow file
const OverflowAddressLength int64 = 8

// MapFileHeaderLength - Length of hash map file header
const MapFileHeaderLength int64 = 1024

// BucketAlgorithmOffset - Header offset to whether using internal (1) or external (0) bucket algorithm - 1 byte
const BucketAlgorithmOffset int64 = 0

// InitialUniqueKeysOffset - Header offset to the initial unique keys setting when file was created - 8 bytes
const InitialUniqueKeysOffset int64 = 1

// KeyLengthOffset - Header offset to the key length in records used in buckets - 4 bytes
const KeyLengthOffset int64 = 9

// ValueLengthOffset - Header offset to the value length in records used in buckets - 4 bytes
const ValueLengthOffset int64 = 13

// RecordsPerBucketOffset - Header offset to number of records per bucket - 2 bytes
const RecordsPerBucketOffset int64 = 17

// NumberOfBucketsOffset - Header offset to number of buckets - 8 bytes
const NumberOfBucketsOffset int64 = 19

// MinBucketNoOffset - Header offset to min bucket number - 8 bytes
const MinBucketNoOffset int64 = 27

// MaxBucketNoOffset - Header offset to max (inclusive) bucket number - 8 bytes
const MaxBucketNoOffset int64 = 35

// FileSizeOffset - Header offset to the file size (should of course reflect true file size) - 8 bytes
const FileSizeOffset int64 = 43

// RecordInUse - Flag indicating a record that is in use
const RecordInUse uint8 = 1

// BucketHeaderLength - Length of header in each bucket
const BucketHeaderLength int64 = 8

// BucketOverflowAddressOffset - Bucket header offset to the overflow address - 8 bytes
const BucketOverflowAddressOffset int64 = 0
