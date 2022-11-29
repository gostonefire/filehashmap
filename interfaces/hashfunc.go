package hashfunc

// HashAlgorithm - Interface that permits an implementation using the FileHashMap to supply a custom bucket
// selection algorithm suited for its particular distribution of keys.
type HashAlgorithm interface {
	// UpdateTableSize - Updates the table size for the hash algorithm.
	// This function will be used in for instance Quadratic Probing where we need one extra always empty bucket to
	// stop probing for finding existing records for a Get or for update in a Set
	//   - deltaSize is the number of buckets to extend (or decrease if a negative number is given) the table size with
	UpdateTableSize(deltaSize int64)
	// HashFunc1 - Given key it generates a bucket number between minValue and maxValue (inclusive)
	// Any number returned outside the minValue/maxValue (inclusive) range will result in an error down stream.
	HashFunc1(key []byte) int64
	// HashFunc2 - Given key it generates a bucket number between minValue and maxValue (inclusive)
	// Any number returned outside the minValue/maxValue (inclusive) range will result in an error down stream.
	HashFunc2(key []byte) int64
	// HashFunc1MaxValue - Returns the max value that HashFunc1 will ever return.
	HashFunc1MaxValue() int64
	// HashFunc2MaxValue - Returns the max value that HashFunc2 will ever return.
	HashFunc2MaxValue() int64
	// CombinedHash - Returns a combined hash value given values from hash functions 1 and 2 with iteration.
	CombinedHash(hashValue1, hashValue2, iteration int64) int64
}
