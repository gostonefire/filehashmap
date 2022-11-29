//go:build unit

package hash

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSingleHashAlgorithm_HashFunc1MaxValue(t *testing.T) {
	t.Run("returns correct max bucket number", func(t *testing.T) {
		// Prepare
		h := NewSingleHashAlgorithm(10)

		// Execute
		bucketMax := h.HashFunc1MaxValue()

		// Check
		assert.Equal(t, int64(15), bucketMax, "correct max value")
	})
}

func TestSingleHashAlgorithm_HashFunc1(t *testing.T) {
	t.Run("creates a valid bucket number", func(t *testing.T) {
		// Prepare
		a := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}

		h := NewSingleHashAlgorithm(10)

		// Execute
		bucketNo := h.HashFunc1(a)

		// Check
		assert.Equal(t, int64(6), bucketNo, "create a valid bucket number")
	})
}

func TestSingleHashAlgorithm_UpdateTableSize(t *testing.T) {
	t.Run("updates table size", func(t *testing.T) {
		// Prepare
		h := NewSingleHashAlgorithm(10)
		bucketMax := h.HashFunc1MaxValue()
		assert.Equal(t, int64(15), bucketMax, "correct max value")

		// Execute
		h.UpdateTableSize(7)

		// Check
		bucketMax = h.HashFunc1MaxValue()
		assert.Equal(t, int64(31), bucketMax, "correct max value")

	})
}
