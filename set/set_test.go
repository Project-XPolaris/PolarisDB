package set

import (
	"fmt"
	"testing"
)

func TestSet_Add(t *testing.T) {
	intset := NewSet()
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
	for i := 100; i >= 0; i-- {
		contain, err := intset.Contains(i)
		if err != nil {
			t.Errorf("contains error %v", err)
		}
		if !contain {
			t.Errorf("contains error %v", err)
		}
	}
	strhash := NewSet()
	for i := 100; i >= 0; i-- {
		err := strhash.Add(fmt.Sprintf("data_%d", i))
		if err != nil {
			t.Errorf("remove error %v", err)
			return
		}
	}
	setLen = strhash.Len()
	if setLen != 101 {
		t.Errorf("invalid set len %d", setLen)
	}
	for i := 100; i >= 0; i-- {
		contain, err := strhash.Contains(fmt.Sprintf("data_%d", i))
		if err != nil {
			t.Errorf("contain error %v", err)
		}
		if !contain {
			t.Errorf("contain error %v", err)
		}
	}

	// str as int
	intset2 := NewSet()
	for i := 100; i >= 0; i-- {
		err := intset2.Add(fmt.Sprintf("%d", i))
		if err != nil {
			t.Errorf("remove error %v", err)
		}
	}
	setLen = intset2.Len()
	if setLen != 101 {
		t.Errorf("invalid set len %d", setLen)
	}
	for i := 100; i >= 0; i-- {
		contain, err := intset2.Contains(fmt.Sprintf("%d", i))
		if err != nil {
			t.Errorf("contain error %v", err)
		}
		if !contain {
			t.Errorf("contain error %v", err)
		}
	}

	// for intset reach max
	intset3 := NewSet()
	for i := 600; i >= 0; i-- {
		err := intset3.Add(i)
		if err != nil {
			t.Errorf("add error %v", err)
		}
	}
	setLen = intset3.Len()
	if setLen != 601 {
		t.Errorf("invalid set len %d", setLen)
	}
	if intset3.intSet != nil {
		t.Errorf("invalid set type")
	}

}

func TestSet_Diff(t *testing.T) {
	intset1 := NewSet()
	for i := 100; i >= 0; i-- {
		err := intset1.Add(i)
		if err != nil {
			t.Errorf("add error %v", err)
		}
	}
	intset2 := NewSet()
	for i := 50; i >= 0; i-- {
		err := intset2.Add(i)
		if err != nil {
			t.Errorf("add error %v", err)
		}
	}
	diffSetValues := Diff(intset1, intset2)
	if len(diffSetValues) != 50 {
		t.Errorf("invalid diff set len %d", len(diffSetValues))
	}
}
func TestSet_Intersection(t *testing.T) {
	intset1 := NewSet()
	for i := 100; i >= 0; i-- {
		err := intset1.Add(i)
		if err != nil {
			t.Errorf("add error %v", err)
		}
	}
	intset2 := NewSet()
	for i := 50; i >= 0; i-- {
		err := intset2.Add(i)
		if err != nil {
			t.Errorf("add error %v", err)
		}
	}
	intset3 := NewSet()
	for i := 20; i >= 0; i-- {
		err := intset3.Add(i)
		if err != nil {
			t.Errorf("add error %v", err)
		}
	}
	intersectionSetValues := Intersection(intset1, intset2, intset3)
	if len(intersectionSetValues) != 21 {
		t.Errorf("invalid intersection set len %d", len(intersectionSetValues))
	}

}
