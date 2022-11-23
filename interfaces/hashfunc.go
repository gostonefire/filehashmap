package hashfunc

// HashAlgorithm - Interface that permits an implementation using the FileHashMap to supply a custom bucket
// selection algorithm suited for its particular distribution of keys.
// The internally used algorithm is implemented using crc32.ChecksumIEEE to create a hash value over the key and
// then applying bucket = hash & (1<<exp - 1) to get the bucket number, where 1<<exp (2 to the power of exp)
// is the total number of buckets to distribute over.
type HashAlgorithm interface {
	// BucketNumber - Given key it generates a bucket number between minValue and maxValue (inclusive)
	// Any number returned outside the minValue/maxValue (inclusive) range will result in an error down stream.
	BucketNumber(key []byte) int64
	// BucketNumberRange - Returns the min and max (inclusive) that BucketNumber will ever return.
	BucketNumberRange() (minValue, maxValue int64)
}
