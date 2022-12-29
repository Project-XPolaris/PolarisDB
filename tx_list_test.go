package polarisdb

import (
	"fmt"
	"testing"
)

func TestTX_LPush(t *testing.T) {
	db := NewDB(&DBConfig{Path: "./tmp"})
	err := db.Open()
	if err != nil {
		t.Fatal(err)
		return
	}
	defer cleanTestData()
	err = db.Update(func(tx *TX) error {
		for i := 0; i < 100; i++ {
			tx.LPush("foo", []byte(fmt.Sprintf("data_%d", i)))
		}
		return nil
	})
	err = db.View(func(tx *TX) error {
		for i := 0; i < 100; i++ {
			val, _ := tx.LIndex("foo", i)
			if string(val) != fmt.Sprintf("data_%d", i) {
				t.Fatal("read data not equal")
				return nil
			}
		}
		return nil
		return nil
	})

	db2 := NewDB(&DBConfig{Path: "./tmp"})
	err = db2.Open()

	err = db2.View(func(tx *TX) error {
		for i := 0; i < 100; i++ {
			val, _ := tx.LIndex("foo", i)
			if string(val) != fmt.Sprintf("data_%d", i) {
				t.Fatal("read data not equal")
				return nil
			}
		}
		return nil
	})

	if err != nil {
		t.Fatal(err)
		return
	}
}

func TestTX_LPop(t *testing.T) {
	db := NewDB(&DBConfig{Path: "./tmp"})
	err := db.Open()
	if err != nil {
		t.Fatal(err)
		return
	}
	defer cleanTestData()
	err = db.Update(func(tx *TX) error {
		for i := 0; i < 100; i++ {
			tx.LPush("foo", []byte(fmt.Sprintf("data_%d", i)))
		}
		return nil
	})
	err = db.Update(func(tx *TX) error {
		for i := 0; i < 100; i++ {
			vals, _ := tx.LPop("foo", 1)
			val := vals[0]
			if string(val) != fmt.Sprintf("data_%d", 99-i) {
				t.Fatal("read data not equal")
				return nil
			}
		}
		return nil
	})
	if err != nil {
		t.Error(err)
		return
	}
	err = db.View(func(tx *TX) error {

		listLen, err := tx.LLen("foo")
		if err != nil {
			t.Fatal(err)
			return nil
		}
		if listLen != 0 {
			t.Fatal("list len not equal")
			return nil
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
		return
	}
}
func TestTX_LInsert(t *testing.T) {
	db := NewDB(&DBConfig{Path: "./tmp"})
	err := db.Open()
	if err != nil {
		t.Fatal(err)
		return
	}
	defer cleanTestData()
	err = db.Update(func(tx *TX) error {
		len, _ := tx.LLen("foo")
		for i := 0; i < 100; i++ {
			tx.LInsert("foo", len, fmt.Sprintf("data_%d", i))
			len += 1
		}
		return nil
	})
	err = db.View(func(tx *TX) error {
		for i := 0; i < 10; i++ {
			val, _ := tx.LIndex("foo", i)
			if string(val) != fmt.Sprintf("data_%d", i) {
				t.Fatal("read data not equal")
				return nil
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
		return
	}
}
