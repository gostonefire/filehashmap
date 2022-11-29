package scres

// ovflFileHeaderLength - Length of overflow file header
const ovflFileHeaderLength int64 = 1024

// overflowAddressLength - Length of address to next record in overflow file
const overflowAddressLength int64 = 8

// bucketHeaderLength - Length of header in each bucket
const bucketHeaderLength int64 = 8

// bucketOverflowAddressOffset - Bucket header offset to the overflow address - 8 bytes
const bucketOverflowAddressOffset int64 = 0
