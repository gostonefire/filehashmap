package hash

import (
	"github.com/gostonefire/filehashmap/internal/utils"
	"hash/crc32"
)

// LinearProbingHashAlgorithm - The internally used bucket selection algorithm is implemented using crc32.ChecksumIEEE to
// create a hash value over the key and then applying bucket = hash & (actualTableSize - 1) to get the bucket number,
// where actualTableSize is the nearest bigger exponent of 2 of the requested table size.
type LinearProbingHashAlgorithm struct {
	tableSize int64
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
