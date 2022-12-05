package hashfunc

// HashAlgorithm - Interface that permits an implementation using the FileHashMap to supply a custom bucket
// selection algorithm suited for its particular distribution of keys.
type HashAlgorithm interface {
	// SetTableSize - Sets the table size for the hash algorithm.
	// It is called both when creating a new file hash map and when opening an existing one. Hence, if a custom
	// hash algorithm is supplied that implements this interface and the instance is already having a table size, it
	// will be overwritten by the number of buckets that is/was supplied when creating the file hash map.
	//   - tableSize is the number of buckets the map file will address
	SetTableSize(tableSize int64)

	// HashFunc1 - Given key it generates an index (bucket) between 0 and table size - 1
	// Any number returned outside the table size (0 -> table size - 1) will result in an error down stream.
	HashFunc1(key []byte) int64

	// HashFunc2 - Given key it generates an offset probing value that will be used together with the value from HashFunc1 in
	// a call to DoubleHashFunc. The function is only used for the Double Hashing Collision Resolution Technique.
	HashFunc2(key []byte) int64

	// GetTableSize - Returns the table size the implemented hash functions are supporting
	// It is very important that this function return the actual table size and not just the table size given at instantiating time or
	// in a call to SetTableSize. Some algorithms are implemented by rounding up to nearest 2 to the power of x, or to the nearest prime, and
	// if such operations are built in the implementation of this interface it must be covered in the GetTableSize.
	GetTableSize() int64

	// ProbeIteration - Returns a combined hash value given values from HashFunc1 and HashFunc2 in iteration.
	// Since this function will be called repeatedly in a collision resolution situation, and the actual hash values
	// from the HashFunc1 and HashFunc2 are the same throughout iterations for one key, the function takes those values rather than
	// using the actual key as input.
	// For some probing algorithms it may be that they return a probing value outside the hash table bucket range, that is
	// alright, the internal loop will then just increment the iteration by one and call this function again.
	// The function is not used for Open Chaining Collision Resolution Technique.
	ProbeIteration(hf1Value, hf2Value, iteration int64) int64
}
