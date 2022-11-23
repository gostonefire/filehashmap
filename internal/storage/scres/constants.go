package scres

// inUseFlagBytes - Number of bytes used to indicate whether an index record is in use or not,
// it is added to each record when written to the index file
const inUseFlagBytes int64 = 1

// ovflFileHeaderLength - Length of overflow file header
const ovflFileHeaderLength int64 = 1024

// overflowAddressLength - Length of address to next record in overflow file
const overflowAddressLength int64 = 8

// mapFileHeaderLength - Length of hash map file header
const mapFileHeaderLength int64 = 1024

// bucketAlgorithmOffset - Header offset to whether using internal (1) or external (0) bucket algorithm - 1 byte
const bucketAlgorithmOffset int64 = 0

// initialUniqueKeysOffset - Header offset to the initial unique keys setting when file was created - 8 bytes
const initialUniqueKeysOffset int64 = 1

// keyLengthOffset - Header offset to the key length in records used in buckets - 4 bytes
const keyLengthOffset int64 = 9

// valueLengthOffset - Header offset to the value length in records used in buckets - 4 bytes
const valueLengthOffset int64 = 13

// recordsPerBucketOffset - Header offset to number of records per bucket - 2 bytes
const recordsPerBucketOffset int64 = 17

// numberOfBucketsOffset - Header offset to number of buckets - 8 bytes
const numberOfBucketsOffset int64 = 19

// minBucketNoOffset - Header offset to min bucket number - 8 bytes
const minBucketNoOffset int64 = 27

// maxBucketNoOffset - Header offset to max (inclusive) bucket number - 8 bytes
const maxBucketNoOffset int64 = 35

// fileSizeOffset - Header offset to the file size (should of course reflect true file size) - 8 bytes
const fileSizeOffset int64 = 43

// recordInUse - Flag indicating a record that is in use
const recordInUse uint8 = 1

// bucketHeaderLength - Length of header in each bucket
const bucketHeaderLength int64 = 8

// bucketOverflowAddressOffset - Bucket header offset to the overflow address - 8 bytes
const bucketOverflowAddressOffset int64 = 0
