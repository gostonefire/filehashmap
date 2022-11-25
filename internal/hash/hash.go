package hash

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
func NewSingleHashAlgorithm(tableSize int64) *SingleHashAlgorithm {
	exp := int64(math.Ceil(math.Log2(float64(tableSize)) / math.Log2(2)))
	return &SingleHashAlgorithm{tableSize: tableSize, exp: exp}
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

// RangeHashFunc1 - Returns the min and max (inclusive) that HashFunc1 will ever return.
func (B *SingleHashAlgorithm) RangeHashFunc1() (minValue, maxValue int64) {
	minValue = 0
	maxValue = 1<<B.exp - 1
	return
}

// RangeHashFunc2 - Returns the min and max (inclusive) that HashFunc2 will ever return.
// This function is only used in Double Hash algorithms, but implemented here to follow the interface.
func (B *SingleHashAlgorithm) RangeHashFunc2() (minValue, maxValue int64) {
	minValue = 0
	maxValue = B.tableSize - 1
	return
}

// CombinedHash - Returns a combined hash value given values from hash functions 1 and 2 with iteration.
// This function is only used in Double Hash algorithms, but implemented here to follow the interface.
func (B *SingleHashAlgorithm) CombinedHash(hashValue1, hashValue2, iteration int64) int64 {
	return (hashValue1 + iteration*hashValue2) % B.tableSize
}
