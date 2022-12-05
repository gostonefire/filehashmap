package hash

import "hash/crc32"

// DoubleHashAlgorithm - The internally used bucket selection algorithm is implemented using crc32.ChecksumIEEE to
// create a hash value over the key and then applying HashFunc1 and HashFunc2 as primary respective probing functions.
type DoubleHashAlgorithm struct {
	tableSize int64
}

// NewDoubleHashAlgorithm - Returns a pointer to a new DoubleHashAlgorithm instance
func NewDoubleHashAlgorithm(tableSize int64) *DoubleHashAlgorithm {
	ha := &DoubleHashAlgorithm{}
	ha.SetTableSize(tableSize)
	return ha
}

// SetTableSize - Sets the table size for the hash algorithm.
// In this implementation it updates the table size to its nearest higher prime number, which allows the algorithm to
// iterate over the entirety of the tables buckets once and only once.
//   - tableSize is the number of buckets the map file will address
func (D *DoubleHashAlgorithm) SetTableSize(tableSize int64) {
	D.tableSize = tableSize
	D.updateToNearestPrime()
}

// HashFunc1 - Given key it generates an index (bucket) between 0 and table size - 1
func (D *DoubleHashAlgorithm) HashFunc1(key []byte) int64 {
	k := int64(crc32.ChecksumIEEE(key))
	return k % D.tableSize
}

// HashFunc2 - Given key it generates an offset probing value that will be used together with the value from HashFunc1 in
// a call to DoubleHashFunc.
func (D *DoubleHashAlgorithm) HashFunc2(key []byte) int64 {
	k := int64(crc32.ChecksumIEEE(key))

	return 1 + ((k / D.tableSize) % (D.tableSize - 1))
}

// GetTableSize - Returns the table size the implemented hash functions are supporting
func (D *DoubleHashAlgorithm) GetTableSize() int64 {
	return D.tableSize
}

// ProbeIteration - Returns a combined hash value given values from HashFunc1 and HashFunc2 in iteration.
// Since this function will be called repeatedly in a collision resolution situation, and the actual hash values
// from the HashFunc1 and HashFunc2 are the same throughout iterations for one key, the function takes those values rather than
// using the actual key as input.
func (D *DoubleHashAlgorithm) ProbeIteration(hf1Value, hf2Value, iteration int64) int64 {
	return (hf1Value + iteration*hf2Value) % D.tableSize
}

// updateToNearestPrime - To ensure that we don't end up in an infinite loop when probing, the easiest way is to
// ensure the table size is a prime number. This function updates the table size to nearest higher prime number.
func (D *DoubleHashAlgorithm) updateToNearestPrime() {
	n := D.tableSize

OUTER:
	for {
		if n == 2 || n == 3 {
			D.tableSize = n
			return
		}

		if n <= 1 || n%2 == 0 || n%3 == 0 {
			n++
			continue
		}

		for i := int64(5); i*i <= n; i += 6 {
			if n%i == 0 || n%(i+2) == 0 {
				n++
				continue OUTER
			}
		}

		D.tableSize = n
		return
	}
}
