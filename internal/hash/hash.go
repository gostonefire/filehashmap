package hash

import (
	"hash/crc32"
	"math"
)

// BucketAlgorithm - The internally used bucket selection algorithm is implemented using crc32.ChecksumIEEE to
// create a hash value over the key and then applying bucket = hash & (1<<exp - 1) to get the bucket number,
// where 1<<exp (2 to the power of exp) is the total number of buckets to distribute over.
type BucketAlgorithm struct {
	exp int64
}

// NewBucketAlgorithm - Returns a pointer to a new BucketAlgorithm instance
func NewBucketAlgorithm(initialUniqueValues int64) *BucketAlgorithm {
	exp := int64(math.Floor(math.Log2(float64(initialUniqueValues)) / math.Log2(2)))
	return &BucketAlgorithm{exp: exp}
}

// BucketNumber - Given key it generates a bucket number between minValue and maxValue (inclusive)
func (B *BucketAlgorithm) BucketNumber(key []byte) int64 {
	h := int64(crc32.ChecksumIEEE(key))
	return h & (1<<B.exp - 1)
}

// BucketNumberRange - Returns the min and max (inclusive) that BucketNumber will ever return.
func (B *BucketAlgorithm) BucketNumberRange() (minValue, maxValue int64) {
	minValue = 0
	maxValue = 1<<B.exp - 1
	return
}
