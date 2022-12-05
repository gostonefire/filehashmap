package hash

import (
	"github.com/gostonefire/filehashmap/internal/utils"
	"hash/crc32"
)

// QuadraticProbingHashAlgorithm - The internally used bucket selection algorithm is implemented using crc32.ChecksumIEEE to
// create a hash value over the key and then applying bucket = hash & (actualTableSize - 1) to get the bucket number,
// where actualTableSize is the nearest bigger exponent of 2 of the requested table size.
type QuadraticProbingHashAlgorithm struct {
	tableSize int64
	roundUp2  int64
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
