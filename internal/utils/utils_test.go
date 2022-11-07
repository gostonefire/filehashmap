//go:build unit

package utils

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestIsEqual(t *testing.T) {
	t.Run("two byte slices are equal in length and values", func(t *testing.T) {
		// Prepare
		a := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
		b := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}

		// Execute
		isEqual := IsEqual(a, b)

		// Check
		assert.True(t, isEqual, "slices equal in length and values")
	})

	t.Run("two byte slices are unequal in length", func(t *testing.T) {
		// Prepare
		a := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
		b := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}

		// Execute
		isEqual := IsEqual(a, b)

		// Check
		assert.False(t, isEqual, "slices unequal in length")
	})

	t.Run("two byte slices are unequal in values", func(t *testing.T) {
		// Prepare
		a := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
		b := []byte{0, 1, 5, 3, 4, 5, 6, 7, 8, 9}

		// Execute
		isEqual := IsEqual(a, b)

		// Check
		assert.False(t, isEqual, "slices unequal in length")
	})
}

func TestExtendByteSlice(t *testing.T) {
	t.Run("bytes are prepended to byte slice", func(t *testing.T) {
		// Prepare
		a := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}

		// Execute
		b := ExtendByteSlice(a, 10, true)

		// Check
		assert.Equal(t, 20, len(b), "slice has right length")
		for i, v := range b {
			if i < 10 {
				if v != 0 {
					assert.Fail(t, "zeros correctly prepended")
				}
			} else {
				if v != a[i-10] {
					assert.Fail(t, "data correctly at end of slice")
				}
			}
		}
	})

	t.Run("bytes are appended to byte slice", func(t *testing.T) {
		// Prepare
		a := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}

		// Execute
		b := ExtendByteSlice(a, 10, false)

		// Check
		assert.Equal(t, 20, len(b), "slice has right length")
		for i, v := range b {
			if i < 10 {
				if v != a[i] {
					assert.Fail(t, "data correctly in beginning of slice")
				}
			} else {
				if v != 0 {
					assert.Fail(t, "zeros correctly appended")
				}
			}
		}
	})
}
