package hashfunc

// HashAlgorithm - Interface that permits an implementation using the FileHashMap to supply a custom bucket
// selection algorithm suited for its particular distribution of keys.
type HashAlgorithm interface {
	// HashFunc1 - Given key it generates a bucket number between minValue and maxValue (inclusive)
	// Any number returned outside the minValue/maxValue (inclusive) range will result in an error down stream.
	HashFunc1(key []byte) int64
	// HashFunc2 - Given key it generates a bucket number between minValue and maxValue (inclusive)
	// Any number returned outside the minValue/maxValue (inclusive) range will result in an error down stream.
	HashFunc2(key []byte) int64
	// RangeHashFunc1 - Returns the min and max (inclusive) that HashFunc1 will ever return.
	RangeHashFunc1() (minValue, maxValue int64)
	// RangeHashFunc2 - Returns the min and max (inclusive) that HashFunc2 will ever return.
	RangeHashFunc2() (minValue, maxValue int64)
	// CombinedHash - Returns a combined hash value given values from hash functions 1 and 2 with iteration.
	CombinedHash(hashValue1, hashValue2, iteration int64) int64
}
