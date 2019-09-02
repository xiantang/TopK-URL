package utils

func BKDRHash64(str string) uint64 {
	list := []byte(str)
	var seed uint64 = 131
	var hash uint64 = 0
	for i := 0; i < len(list); i++ {
		hash = hash*seed + uint64(list[i])
	}
	return hash & 0x7FFFFFFFFFFFFFFF
}
