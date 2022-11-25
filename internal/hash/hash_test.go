//go:build unit

package hash

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBucketNumberRange(t *testing.T) {
	t.Run("creates a valid bucket number", func(t *testing.T) {
		h := NewSingleHashAlgorithm(10)
		bucketMin, bucketMax := h.RangeHashFunc1()

		assert.Equal(t, int64(0), bucketMin, "correct min value")
		assert.Equal(t, int64(15), bucketMax, "correct max value")

	})
}

func TestBucketNumber(t *testing.T) {
	t.Run("creates a valid bucket number", func(t *testing.T) {
		a := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}

		h := NewSingleHashAlgorithm(10)
		bucketNo := h.HashFunc1(a)

		assert.Equal(t, int64(6), bucketNo, "create a valid bucket number")

	})
}
