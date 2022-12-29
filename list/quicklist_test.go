package list

import (
	"fmt"
	"testing"
)

func TestQuickList_Append(t *testing.T) {
	ql := NewQuickList()
	for i := 0; i < 10; i++ {
		ql.InsertAt(0, []byte(fmt.Sprintf("data_%d", i)))
	}
	iter := ql.GetIterator()
	curEntry := iter.Next()
	for curEntry != nil {
		fmt.Println(string(curEntry))
		curEntry = iter.Next()
	}
}
func TestQuickList_Append2(t *testing.T) {
	ql := NewQuickList()
	MaxZiplistSize = 5 * (len("data_00") + 16)
	for i := 0; i < 10; i++ {
		ql.InsertAt(0, []byte(fmt.Sprintf("data_%02d", i)))
	}
	iter := ql.GetIterator()
	curEntry := iter.Next()
	for curEntry != nil {
		fmt.Println(string(curEntry))
		curEntry = iter.Next()
	}
}
func TestQuickList_Append3(t *testing.T) {
	ql := NewQuickList()
	MaxZiplistSize = 5 * (len("data_00") + 16)
	for i := 0; i < 10; i++ {
		ql.InsertAt(0, []byte(fmt.Sprintf("data_%02d", i)))
	}
	ql.InsertAt(2, []byte("data_XX"))
	iter := ql.GetIterator()
	curEntry := iter.Next()
	for curEntry != nil {
		fmt.Println(string(curEntry))
		curEntry = iter.Next()
	}
}
func TestQuickList_Append4(t *testing.T) {
	ql := NewQuickList()
	MaxZiplistSize = 5 * (len("data_00") + 16)
	for i := 0; i < 10; i++ {
		ql.InsertAt(0, []byte(fmt.Sprintf("data_%02d", i)))
	}
	ql.InsertAt(10, []byte("data_XX"))
	iter := ql.GetIterator()
	curEntry := iter.Next()
	for curEntry != nil {
		fmt.Println(string(curEntry))
		curEntry = iter.Next()
	}
}
func TestQuickList_DeleteAt(t *testing.T) {
	ql := NewQuickList()
	for i := 0; i < 10; i++ {
		ql.InsertAt(0, []byte(fmt.Sprintf("data_%d", i)))
	}
	for i := 0; i < 4; i++ {
		ql.DeleteAt(0)
	}
	iter := ql.GetIterator()
	curEntry := iter.Next()
	for curEntry != nil {
		fmt.Println(string(curEntry))
		curEntry = iter.Next()
	}
}
func TestQuickList_Index(t *testing.T) {
	ql := NewQuickList()
	for i := 0; i < 100; i++ {
		ql.InsertAt(0, []byte(fmt.Sprintf("data_%d", i)))
	}
	for i := 0; i > 0; i-- {
		ent := ql.Index(i)
		if string(ent) != fmt.Sprintf("data_%d", i) {
			t.Errorf("invalid index %d, data %s", i, string(ent))
		}
	}
}
func TestQuickList_Range(t *testing.T) {
	ql := NewQuickList()
	for i := 0; i < 10; i++ {
		ql.InsertAt(0, []byte(fmt.Sprintf("data_%d", i)))
	}
	entries := ql.Range(2, 5)
	for _, entry := range entries {
		fmt.Println(string(entry))
	}
}
func TestZiplistEntry_Count(t *testing.T) {
	list := NewZiplistEntry()
	for i := 0; i < 10; i++ {
		list.Insert(i, []byte(fmt.Sprintf("data_%d", i)))
	}
	if list.Count() != 10 {
		t.Errorf("invalid count %d", list.Count())
	}
}
func TestQuickListIterator_Next(t *testing.T) {
	ql := NewQuickList()
	for i := 0; i < 10; i++ {
		ql.InsertAt(0, []byte(fmt.Sprintf("data_%d", i)))
	}
	iter := ql.GetIterator()
	curEntry := iter.Next()
	for curEntry != nil {
		fmt.Println(string(curEntry))
		curEntry = iter.Next()
	}
}
