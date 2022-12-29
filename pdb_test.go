package polarisdb

import (
	"crypto/rand"
	"fmt"
	"testing"
)

func TestNewDB(t *testing.T) {
	db := NewDB(&DBConfig{})
	if db == nil {
		t.Fatal("db is nil")
	}
}
func TestPolarisDB_Open(t *testing.T) {
	db := NewDB(&DBConfig{Path: "./tmp"})
	err := db.Open()
	if err != nil {
		t.Fatal(err)
		return
	}
	defer cleanTestData()

}

func TestPolarisDB_View(t *testing.T) {
	db := NewDB(&DBConfig{Path: "./tmp"})
	err := db.Open()
	if err != nil {
		t.Fatal(err)
		return
	}
	defer cleanTestData()

	err = db.Update(func(tx *TX) error {
		tx.SetString("foo", "bar", false)
		return nil
	})
	err = db.View(func(tx *TX) error {
		val, _ := tx.Get("foo")
		if val != "bar" {
			t.Fatal("read data not equal")
			return nil
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
		return
	}
}
func randomKeyAndValue(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	s := fmt.Sprintf("%X", b)
	return s
}
