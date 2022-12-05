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

func TestRoundUp2(t *testing.T) {
	t.Run("bytes are prepended to byte slice", func(t *testing.T) {
		// Prepare
		r2u := []int64{4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 262144, 16777216, 1073741824}
		input := []int64{3, 5, 9, 30, 50, 100, 129, 512, 1020, 1500, 3000, 7123, 9000, 200000, 16000000, 536870913}

		// Execute and Check
		for i := 0; i < len(input); i++ {
			r := RoundUp2(input[i])
			assert.Equal(t, r2u[i], r, "rounds upp correct")
		}
	})
}
