//go:build unit

package hash

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestLinearProbingHashAlgorithm_GetTableSize(t *testing.T) {
	t.Run("returns correct max bucket number", func(t *testing.T) {
		// Prepare
		h := NewLinearProbingHashAlgorithm(10)

		// Execute
		tableSize := h.GetTableSize()

		// Check
		assert.Equal(t, int64(16), tableSize, "correct tableSize value")
	})
}

func TestLinearProbingHashAlgorithm_HashFunc1(t *testing.T) {
	t.Run("creates a valid bucket number", func(t *testing.T) {
		// Prepare
		a := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}

		h := NewLinearProbingHashAlgorithm(10)

		// Execute
		bucketNo := h.HashFunc1(a)

		// Check
		assert.Equal(t, int64(6), bucketNo, "create a valid bucket number")
	})
}

func TestLinearProbingHashAlgorithm_SetTableSize(t *testing.T) {
	t.Run("sets table size", func(t *testing.T) {
		// Prepare
		h := NewLinearProbingHashAlgorithm(10)
		tableSize := h.GetTableSize()
		assert.Equal(t, int64(16), tableSize, "correct tableSize value")

		// Execute
		h.SetTableSize(16 + 7)

		// Check
		tableSize = h.GetTableSize()
		assert.Equal(t, int64(32), tableSize, "correct tableSize value")

	})
}

func TestLinearProbingHashAlgorithm_ProbeIteration(t *testing.T) {
	t.Run("iterates through table", func(t *testing.T) {
		// Prepare
		h := NewLinearProbingHashAlgorithm(10)
		tableSize := h.GetTableSize()
		assert.Equal(t, int64(16), tableSize, "correct tableSize value")

		a := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}

		bucketNo := h.HashFunc1(a)

		visit := make([]int, tableSize)

		// Execute
		for i := int64(0); i < tableSize; i++ {
			probe := h.ProbeIteration(bucketNo, 0, i)
			assert.GreaterOrEqualf(t, probe, int64(0), "probe not negative in iteration #%d", i)
			assert.Lessf(t, probe, tableSize, "probe less than table size in iteration #%d", i)
			visit[probe]++
		}

		// Check
		for i := int64(0); i < tableSize; i++ {
			assert.Equalf(t, 1, visit[i], "exactly one visit in bucket #%d", i)
		}
	})
}
