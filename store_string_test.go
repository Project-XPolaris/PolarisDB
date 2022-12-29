package polarisdb

import "testing"

func NewMockStringStore() *StringStore {
	return NewStore()
}

func TestRead(t *testing.T) {
	store := NewMockStringStore()
	store.write([]byte("foo"), []byte("bar"))
	data, err := store.read([]byte("foo"))
	if err != nil {
		t.Fatal(err)
		return
	}
	if string(data) != "bar" {
		t.Fatal("read data not equal")
		return
	}
}
func TestWrite(t *testing.T) {
	store := NewMockStringStore()
	store.write([]byte("foo"), []byte("bar"))
	data, err := store.read([]byte("foo"))
	if err != nil {
		t.Fatal(err)
		return
	}
	if string(data) != "bar" {
		t.Fatal("read data not equal")
		return
	}
}

func TestDelete(t *testing.T) {
	store := NewMockStringStore()
	store.write([]byte("foo"), []byte("bar"))
	err := store.delete([]byte("foo"))
	if err != nil {
		t.Fatal(err)
	}
	val, err := store.read([]byte("foo"))
	if val != nil {
		t.Fatal("deleteField failed")
		return
	}
}

func TestKeys(t *testing.T) {
	store := NewMockStringStore()
	store.write([]byte("foo"), []byte("bar"))
	store.write([]byte("foo1"), []byte("bar1"))
	store.write([]byte("foo2"), []byte("bar2"))
	keys, err := store.keys()
	if err != nil {
		t.Fatal(err)
		return
	}
	if len(keys) != 3 {
		t.Fatal("keys length not equal")
		return
	}
}
