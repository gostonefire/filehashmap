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
