package utils

// IsEqual - Returns true if a and b are equal both in size and contents
func IsEqual(a, b []byte) bool {
	lenA := len(a)
	if lenA != len(b) {
		return false
	}

	for i := 0; i < lenA; i++ {
		if a[i] != b[i] {
			return false
		}
	}

	return true
}

// ExtendByteSlice - Extends a byte slice by prepending or appending a number of zero bytes
func ExtendByteSlice(a []byte, extension int64, prepend bool) (b []byte) {
	b = make([]byte, len(a))
	_ = copy(b, a)
	if extension > 0 {
		if prepend {
			b = append(make([]byte, extension), b...)
		} else {
			b = append(b, make([]byte, extension)...)
		}
	}

	return
}

// RoundUp2 - Rounds up to the nearest exponent of 2
func RoundUp2(a int64) int64 {
	r := uint64(a - 1)
	r |= r >> 1
	r |= r >> 2
	r |= r >> 4
	r |= r >> 8
	r |= r >> 16
	r |= r >> 32
	return int64(r + 1)
}
