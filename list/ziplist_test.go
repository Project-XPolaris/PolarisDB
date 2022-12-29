package list

import (
	"fmt"
	"testing"
)

func TestZiplistEntry_Insert(t *testing.T) {
	list := NewZiplistEntry()
	for i := 0; i < 100; i++ {
		list.Insert(i, []byte(fmt.Sprintf("data_%d", i)))
	}
	iter := list.GetIterator()
	curEntry := iter.Next()
	for curEntry != nil {
		fmt.Println(string(curEntry.data))
		curEntry = iter.Next()
	}
	fmt.Println(list)

}

func TestZiplistEntry_Delete(t *testing.T) {
	list := NewZiplistEntry()
	for i := 0; i < 10; i++ {
		list.Insert(i, []byte(fmt.Sprintf("data_%d", i)))
	}
	for i := 0; i < 4; i++ {
		list.Delete(1)
	}
}
func TestZiplistEntry_Index(t *testing.T) {
	list := NewZiplistEntry()
	for i := 0; i < 10; i++ {
		list.Insert(i, []byte(fmt.Sprintf("data_%d", i)))
	}
	for i := 0; i < 10; i++ {
		ent := list.Index(i)
		if string(ent.data) != fmt.Sprintf("data_%d", i) {
			t.Errorf("invalid index %d, data %s", i, string(ent.data))
		}
	}
}
