package hash

import (
	"github.com/gostonefire/filehashmap/internal/utils"
	"hash/crc32"
)

// SeparateChainingHashAlgorithm - The internally used bucket selection algorithm is implemented using crc32.ChecksumIEEE to
// create a hash value over the key and then applying bucket = hash & (actualTableSize - 1) to get the bucket number,
// where actualTableSize is the nearest bigger exponent of 2 of the requested table size.
type SeparateChainingHashAlgorithm struct {
	tableSize int64
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
