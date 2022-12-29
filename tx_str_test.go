package polarisdb

import (
	"fmt"
	"os"
	"testing"
	"time"
)

func cleanTestData() {
	os.RemoveAll("./tmp")
}
func TestStringAct_Write(t *testing.T) {
	db := NewDB(&DBConfig{Path: "./tmp"})
	err := db.Open()
	if err != nil {
		t.Fatal(err)
		return
	}
	defer cleanTestData()
	err = db.Update(func(tx *TX) error {
		return tx.SetString("foo", "bar", false)
	})
	if err != nil {
		t.Fatal(err)
		return
	}
}
func TestTX_SetExpire(t *testing.T) {
	db := NewDB(&DBConfig{Path: "./tmp"})
	err := db.Open()
	if err != nil {
		t.Fatal(err)
		return
	}
	defer cleanTestData()

	err = db.Update(func(tx *TX) error {
		err = tx.SetString("foo", "bar", false)
		if err != nil {
			return err
		}
		err = tx.SetExpire("foo", 3)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
		return
	}
	db = NewDB(&DBConfig{Path: "./tmp"})
	err = db.Open()
	if err != nil {
		t.Fatal(err)
		return
	}
}

func TestTX_Append(t *testing.T) {
	db := NewDB(&DBConfig{Path: "./tmp"})
	err := db.Open()
	if err != nil {
		t.Fatal(err)
		return
	}
	defer cleanTestData()

	err = db.Update(func(tx *TX) error {
		err = tx.SetString("foo", "bar", false)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
		return
	}
	err = db.Update(func(tx *TX) error {
		err = tx.Append("foo", "bar")
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
		return
	}
	err = db.View(func(tx *TX) error {
		val, _ := tx.Get("foo")
		if val != "barbar" {
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

func TestTX_Decr(t *testing.T) {
	db := NewDB(&DBConfig{Path: "./tmp"})
	err := db.Open()
	if err != nil {
		t.Fatal(err)
		return
	}
	defer cleanTestData()

	err = db.Update(func(tx *TX) error {
		err = tx.SetString("foo", "3", false)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
		return
	}
	err = db.Update(func(tx *TX) error {
		err = tx.Decr("foo")
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
		return
	}

	err = db.View(func(tx *TX) error {
		val, _ := tx.Get("foo")
		if val != "2" {
			t.Fatal("read data not equal")
			return nil
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
		return
	}

	db2 := NewDB(&DBConfig{Path: "./tmp"})
	err = db2.Open()
	if err != nil {
		t.Fatal(err)
		return
	}
	err = db2.Update(func(tx *TX) error {
		err = tx.Decr("foo")
		if err != nil {
			return err
		}
		return nil

	})
	if err != nil {
		t.Fatal(err)
		return
	}
	err = db2.View(func(tx *TX) error {
		val, _ := tx.Get("foo")
		if val != "1" {
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

func TestTX_DecrBy(t *testing.T) {
	db := NewDB(&DBConfig{Path: "./tmp"})
	err := db.Open()
	if err != nil {
		t.Fatal(err)
		return
	}
	defer cleanTestData()

	err = db.Update(func(tx *TX) error {
		err = tx.SetString("foo", "3", false)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
		return
	}
	err = db.Update(func(tx *TX) error {
		err = tx.DecrBy("foo", 2)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
		return
	}

	err = db.View(func(tx *TX) error {
		val, err := tx.Get("foo")
		if err != nil {
			t.Fatal(err)
			return nil
		}
		if val != "1" {
			t.Fatal("read data not equal")
			return nil
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
		return
	}

	db2 := NewDB(&DBConfig{Path: "./tmp"})
	err = db2.Open()
	if err != nil {
		t.Fatal(err)
		return
	}
	err = db2.Update(func(tx *TX) error {
		return tx.DecrBy("foo", 2)

	})
	if err != nil {
		t.Fatal(err)
		return
	}
	err = db2.View(func(tx *TX) error {
		val, _ := tx.Get("foo")
		if val != "-1" {
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

func TestTX_Incr(t *testing.T) {
	db := NewDB(&DBConfig{Path: "./tmp"})
	err := db.Open()
	if err != nil {
		t.Fatal(err)
		return
	}
	defer cleanTestData()

	err = db.Update(func(tx *TX) error {
		err = tx.SetString("foo", "3", false)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
		return
	}
	err = db.Update(func(tx *TX) error {
		err = tx.Incr("foo")
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
		return
	}
	err = db.View(func(tx *TX) error {
		val, err := tx.Get("foo")
		if err != nil {
			return err
		}
		if val != "4" {
			t.Fatal("read data not equal")
			return nil
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
		return
	}

	db2 := NewDB(&DBConfig{Path: "./tmp"})
	err = db2.Open()
	if err != nil {
		t.Fatal(err)
		return
	}
	err = db2.Update(func(tx *TX) error {
		err = tx.Incr("foo")
		return err

	})
	if err != nil {
		t.Fatal(err)
		return
	}
	err = db2.View(func(tx *TX) error {
		val, _ := tx.Get("foo")
		if val != "5" {
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

func TestTX_IncrBy(t *testing.T) {
	db := NewDB(&DBConfig{Path: "./tmp"})
	err := db.Open()
	if err != nil {
		t.Fatal(err)
		return
	}
	defer cleanTestData()

	err = db.Update(func(tx *TX) error {
		err = tx.SetString("foo", "3", false)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
		return
	}
	err = db.Update(func(tx *TX) error {
		err = tx.IncrBy("foo", 2)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
		return
	}

	err = db.View(func(tx *TX) error {
		val, _ := tx.Get("foo")
		if val != "5" {
			t.Fatal("read data not equal")
			return nil
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
		return
	}

	db2 := NewDB(&DBConfig{Path: "./tmp"})
	err = db2.Open()
	if err != nil {
		t.Fatal(err)
		return
	}
	err = db2.Update(func(tx *TX) error {
		err = tx.IncrBy("foo", 2)
		if err != nil {
			return err
		}
		return nil

	})
	if err != nil {
		t.Fatal(err)
		return
	}
	err = db2.View(func(tx *TX) error {
		val, _ := tx.Get("foo")
		if val != "7" {
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

func TestTX_GetDel(t *testing.T) {
	db := NewDB(&DBConfig{Path: "./tmp"})
	err := db.Open()
	if err != nil {
		t.Fatal(err)
		return
	}
	defer cleanTestData()

	err = db.Update(func(tx *TX) error {
		err = tx.SetString("foo", "bar", false)
		if err != nil {
			return err
		}
		err = tx.SetString("foo2", "bar2", false)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
		return
	}

	err = db.Update(func(tx *TX) error {
		val, _ := tx.GetDel("foo")
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

	err = db.View(func(tx *TX) error {
		val, _ := tx.Get("foo")
		if val != "" {
			t.Fatal("read data not equal")
			return nil
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
		return
	}

	db2 := NewDB(&DBConfig{Path: "./tmp"})
	err = db2.Open()
	if err != nil {
		t.Fatal(err)
		return
	}
	err = db2.Update(func(tx *TX) error {
		_, err = tx.GetDel("foo2")
		if err != nil {
			return err
		}
		return nil

	})
	if err != nil {
		t.Fatal(err)
		return
	}
	err = db2.View(func(tx *TX) error {
		val, _ := tx.Get("foo2")
		if val != "" {
			t.Fatal("read data not equal")
			return nil
		}
		val1, _ := tx.Get("foo")
		if val1 != "" {
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

func TestTX_GetEx(t *testing.T) {
	db := NewDB(&DBConfig{Path: "./tmp"})
	err := db.Open()
	if err != nil {
		t.Fatal(err)
		return
	}
	defer cleanTestData()

	err = db.Update(func(tx *TX) error {
		err = tx.SetString("foo", "bar", false)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
		return
	}

	err = db.Update(func(tx *TX) error {
		val, _ := tx.GetEx("foo", 1)
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
	<-time.After(time.Second * 3)
	err = db.View(func(tx *TX) error {
		val, _ := tx.Get("foo")
		fmt.Print(val)
		if val != "" {
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

func TestTX_GetRange(t *testing.T) {
	db := NewDB(&DBConfig{Path: "./tmp"})
	err := db.Open()
	if err != nil {
		t.Fatal(err)
		return
	}
	defer cleanTestData()

	err = db.Update(func(tx *TX) error {
		err = tx.SetString("foo", "bar", false)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
		return
	}

	err = db.Update(func(tx *TX) error {
		val, _ := tx.GetRange("foo", 1, 3)
		if val != "ar" {
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

func TestTX_Lcs(t *testing.T) {
	db := NewDB(&DBConfig{Path: "./tmp"})
	err := db.Open()
	if err != nil {
		t.Fatal(err)
		return
	}
	defer cleanTestData()

	err = db.Update(func(tx *TX) error {
		err = tx.SetString("foo", "123bar23", false)
		if err != nil {
			return err
		}
		err = tx.SetString("foo2", "abar2gs", false)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
		return
	}

	err = db.Update(func(tx *TX) error {
		val, _ := tx.Lcs("foo", "foo2")
		if val != "bar2" {
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

func TestTX_MGet(t *testing.T) {
	db := NewDB(&DBConfig{Path: "./tmp"})
	err := db.Open()
	if err != nil {
		t.Fatal(err)
		return
	}
	defer cleanTestData()

	err = db.Update(func(tx *TX) error {
		err = tx.SetString("foo", "bar", false)
		if err != nil {
			return err
		}
		err = tx.SetString("foo2", "bar2", false)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
		return
	}

	err = db.Update(func(tx *TX) error {
		val, _ := tx.MGet("foo", "foo2", "fool3")
		if val[0] != "bar" || val[1] != "bar2" || val[2] != "" {
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

func TestTX_MSet(t *testing.T) {
	db := NewDB(&DBConfig{Path: "./tmp"})
	err := db.Open()
	if err != nil {
		t.Fatal(err)
		return
	}
	defer cleanTestData()

	err = db.Update(func(tx *TX) error {
		err = tx.MSet("foo", "bar", "foo2", "bar2")
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
		return
	}

	err = db.Update(func(tx *TX) error {
		val, _ := tx.Get("foo")
		if val != "bar" {
			t.Fatal("read data not equal")
			return nil
		}
		val, _ = tx.Get("foo2")
		if val != "bar2" {
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
