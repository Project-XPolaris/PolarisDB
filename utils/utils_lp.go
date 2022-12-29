package utils

func FindLargestPrefix(a, b []byte) []byte {
	var i int
	for i = 0; i < len(a) && i < len(b); i++ {
		if a[i] != b[i] {
			break
		}
	}
	return a[:i]
}

func IsPrefix(a, b []byte) bool {
	for i := 0; i < len(a) && i < len(b); i++ {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
