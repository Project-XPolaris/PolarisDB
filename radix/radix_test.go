package radix

import (
	"bytes"
	"fmt"
	"testing"
)

func TestNewTree(t *testing.T) {
	tree := NewTree()
	tree.Set([]byte("key"), []byte("Value"))
	tree.Set([]byte("kex"), []byte("valuex"))
	tree.Set([]byte("kex24"), []byte("valuex24"))
	tree.Set([]byte("key2"), []byte("value2"))
	tree.Set([]byte("key1"), []byte("value1"))
	tree.Set([]byte("key3"), []byte("value3"))
	tree.Set([]byte("foo"), []byte("bar"))
	return
}

func TestRadixTree_Get(t *testing.T) {
	tree := NewTree()
	tree.Set([]byte("key"), []byte("Value"))
	tree.Set([]byte("key2"), []byte("value2"))
	tree.Set([]byte("key1"), []byte("value1"))
	tree.Set([]byte("key3"), []byte("value3"))
	tree.Set([]byte("key333"), []byte("value333"))
	val, err := tree.Get([]byte("key"))
	if err != nil {
		t.Fatal(err)
		return
	}
	fmt.Println(string(val), err)
}

func TestRadixTree_Delete(t *testing.T) {
	tree := NewTree()
	tree.Set([]byte("key"), []byte("Value"))
	tree.Set([]byte("kex"), []byte("valuex"))
	tree.Set([]byte("kex24"), []byte("valuex24"))
	tree.Set([]byte("key2"), []byte("value2"))
	tree.Set([]byte("key1"), []byte("value1"))
	tree.Set([]byte("key3"), []byte("value3"))
	tree.Set([]byte("foo"), []byte("bar"))
	tree.Delete([]byte("key"))
	val, err := tree.Get([]byte("key"))
	if err != nil {
		t.Fatal(err)
		return
	}
	fmt.Println(string(val), err)
	tree.Delete([]byte("key2"))
	val, err = tree.Get([]byte("key2"))
	if err != nil {
		t.Fatal(err)
		return
	}
	fmt.Println(string(val), err)
	tree.Delete([]byte("foo"))
	val, err = tree.Get([]byte("foo"))
	if err != nil {
		t.Fatal(err)
		return
	}
	fmt.Println(string(val), err)

}

func TestRadixTree_Walk(t *testing.T) {
	tree := NewTree()
	tree.Set([]byte("key"), []byte("Value"))
	tree.Set([]byte("kex"), []byte("valuex"))
	tree.Set([]byte("key2"), []byte("value2"))
	tree.Set([]byte("key1"), []byte("value1"))
	tree.Set([]byte("key3"), []byte("value3"))
	tree.Walk(func(key, value []byte) {
		fmt.Println(string(key), string(value))
	})
}

func TestRewrite(t *testing.T) {
	tree := NewTree()
	tree.Set([]byte("key"), []byte("Value"))
	tree.Set([]byte("key"), []byte("valuex"))
	val, err := tree.Get([]byte("key"))
	if err != nil {
		t.Fatal(err)
		return
	}
	if !bytes.Equal(val, []byte("valuex")) {
		t.Fatal("rewrite failed")
	}
}
