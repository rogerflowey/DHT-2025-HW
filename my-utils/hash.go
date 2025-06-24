package myutils

import (
	"hash/fnv"
)

func HashStringToUint32(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
