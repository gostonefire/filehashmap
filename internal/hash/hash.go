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
