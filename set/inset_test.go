package set

import "testing"

func TestIntSet_Add(t *testing.T) {
	intset := NewIntSet()
	for i := 100; i >= 0; i-- {
		err := intset.Add(i)
		if err != nil {
			t.Errorf("add error %v", err)
		}
	}

	setLen := intset.Len()
	if setLen != 101 {
		t.Errorf("invalid set len %d", setLen)
	}
}

func TestIntSet_Remove(t *testing.T) {
	intset := NewIntSet()
	for i := 100; i >= 0; i-- {
		err := intset.Add(i)
		if err != nil {
			t.Errorf("add error %v", err)
		}
	}
	for i := 100; i >= 0; i-- {
		err := intset.Remove(i)
		if err != nil {
			t.Errorf("remove error %v", err)
		}
	}
	setLen := intset.Len()
	if setLen != 0 {
		t.Errorf("invalid set len %d", setLen)
	}
}

func TestIntSet_CanInsert(t *testing.T) {
	intset := NewIntSet()
	for i := 100; i >= 0; i-- {
		err := intset.Add(i)
		if err != nil {
			t.Errorf("add error %v", err)
		}
	}
	if !intset.CanInsert("1") {
		t.Errorf("invalid CanInsert")
	}
}
