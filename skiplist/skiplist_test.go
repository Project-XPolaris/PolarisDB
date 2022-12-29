package skiplist

import (
	"fmt"
	"math/rand"
	"testing"
)

func randomFloat64() float64 {
	return rand.Float64()
}
func TestSkipList_Insert(t *testing.T) {
	skipList := NewZset()
	for i := 0; i < 100; i++ {
		skipList.Add(randomFloat64(), fmt.Sprintf("member_%d", i), fmt.Sprintf("val_%d", i))
	}
}
