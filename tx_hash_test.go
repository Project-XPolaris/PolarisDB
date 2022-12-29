package polarisdb

import "testing"

func TestTX_HSet(t *testing.T) {
	db := NewDB(&DBConfig{Path: "./tmp"})
	err := db.Open()
	if err != nil {
		t.Fatal(err)
		return
	}
	defer cleanTestData()
	err = db.Update(func(tx *TX) error {
		return tx.HSet("foo", []Paris{
			{Field: []byte("bar1"), Value: []byte("bar1")},
			{Field: []byte("bar2"), Value: []byte("bar2")},
			{Field: []byte("bar3"), Value: []byte("bar3")},
		}...)
	})
	if err != nil {
		t.Fatal(err)
		return
	}
	err = db.View(func(tx *TX) error {
		v, err := tx.HGet("foo", "bar1")
		if err != nil {
			return err
		}
		if string(v) != "bar1" {
			t.Fatal("HGet value error")
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
	err = db2.View(func(tx *TX) error {
		v, err := tx.HGet("foo", "bar2")
		if err != nil {
			return err
		}
		if string(v) != "bar2" {
			t.Fatal("HGet value error")
			return nil
		}
		return nil
	})
}

func TestTX_HDel(t *testing.T) {
	db := NewDB(&DBConfig{Path: "./tmp"})
	err := db.Open()
	if err != nil {
		t.Fatal(err)
		return
	}
	defer cleanTestData()
	err = db.Update(func(tx *TX) error {
		return tx.HSet("foo", []Paris{
			{Field: []byte("bar1"), Value: []byte("bar1")},
			{Field: []byte("bar2"), Value: []byte("bar2")},
			{Field: []byte("bar3"), Value: []byte("bar3")},
		}...)
	})
	if err != nil {
		t.Fatal(err)
		return
	}
	err = db.Update(func(tx *TX) error {
		return tx.HDel("foo", "bar1")
	})
	if err != nil {
		t.Fatal(err)
		return
	}
	err = db.View(func(tx *TX) error {
		v, err := tx.HGet("foo", "bar1")
		if err != nil {
			return err
		}
		if v != "" {
			t.Fatal("HGet value error")
			return nil
		}
		return nil
	})
	if err == nil {
		t.Fatal("HGet value error")
		return
	}
	db2 := NewDB(&DBConfig{Path: "./tmp"})
	err = db2.Open()
	if err != nil {
		t.Fatal(err)
		return
	}
	err = db2.View(func(tx *TX) error {
		v, err := tx.HGet("foo", "bar2")
		if err != nil {
			return err
		}
		if string(v) != "bar2" {
			t.Fatal("HGet value error")
			return nil
		}
		return nil
	})
}

func TestTX_HGetAll(t *testing.T) {
	db := NewDB(&DBConfig{Path: "./tmp"})
	err := db.Open()
	if err != nil {
		t.Fatal(err)
		return
	}
	defer cleanTestData()
	err = db.Update(func(tx *TX) error {
		return tx.HSet("foo", []Paris{
			{Field: []byte("bar1"), Value: []byte("bar1")},
			{Field: []byte("bar2"), Value: []byte("bar2")},
			{Field: []byte("bar3"), Value: []byte("bar3")},
		}...)
	})
	if err != nil {
		t.Fatal(err)
		return
	}
	err = db.View(func(tx *TX) error {
		v, err := tx.HGetAll("foo")
		if err != nil {
			return err
		}
		if len(v) != 3 {
			t.Fatal("HGetAll value error")
			return nil
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
		return
	}
}

func TestTX_HExists(t *testing.T) {
	db := NewDB(&DBConfig{Path: "./tmp"})
	err := db.Open()
	if err != nil {
		t.Fatal(err)
		return
	}
	defer cleanTestData()
	err = db.Update(func(tx *TX) error {
		return tx.HSet("foo", []Paris{
			{Field: []byte("bar1"), Value: []byte("bar1")},
			{Field: []byte("bar2"), Value: []byte("bar2")},
			{Field: []byte("bar3"), Value: []byte("bar3")},
		}...)
	})
	if err != nil {
		t.Fatal(err)
		return
	}
	err = db.View(func(tx *TX) error {
		v, err := tx.HExists("foo", "bar1")
		if err != nil {
			return err
		}
		if !v {
			t.Fatal("HExists value error")
			return nil
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
		return
	}
}

func TestTX_HKeys(t *testing.T) {
	db := NewDB(&DBConfig{Path: "./tmp"})
	err := db.Open()
	if err != nil {
		t.Fatal(err)
		return
	}
	defer cleanTestData()
	err = db.Update(func(tx *TX) error {
		return tx.HSet("foo", []Paris{
			{Field: []byte("bar1"), Value: []byte("bar1")},
			{Field: []byte("bar2"), Value: []byte("bar2")},
			{Field: []byte("bar3"), Value: []byte("bar3")},
		}...)
	})
	if err != nil {
		t.Fatal(err)
		return
	}
	err = db.View(func(tx *TX) error {
		v, err := tx.HKeys("foo")
		if err != nil {
			return err
		}
		if len(v) != 3 {
			t.Fatal("HKeys value error")
			return nil
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
		return
	}
}

func TestTX_HVals(t *testing.T) {
	db := NewDB(&DBConfig{Path: "./tmp"})
	err := db.Open()
	if err != nil {
		t.Fatal(err)
		return
	}
	defer cleanTestData()
	err = db.Update(func(tx *TX) error {
		return tx.HSet("foo", []Paris{
			{Field: []byte("bar1"), Value: []byte("bar1")},
			{Field: []byte("bar2"), Value: []byte("bar2")},
			{Field: []byte("bar3"), Value: []byte("bar3")},
		}...)
	})
	if err != nil {
		t.Fatal(err)
		return
	}
	err = db.View(func(tx *TX) error {
		v, err := tx.HVals("foo")
		if err != nil {
			return err
		}
		if len(v) != 3 {
			t.Fatal("HVals value error")
			return nil
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
		return
	}
}

func TestTX_HLen(t *testing.T) {
	db := NewDB(&DBConfig{Path: "./tmp"})
	err := db.Open()
	if err != nil {
		t.Fatal(err)
		return
	}
	defer cleanTestData()
	err = db.Update(func(tx *TX) error {
		return tx.HSet("foo", []Paris{
			{Field: []byte("bar1"), Value: []byte("bar1")},
			{Field: []byte("bar2"), Value: []byte("bar2")},
			{Field: []byte("bar3"), Value: []byte("bar3")},
		}...)
	})
	if err != nil {
		t.Fatal(err)
		return
	}
	err = db.View(func(tx *TX) error {
		v, err := tx.HLen("foo")
		if err != nil {
			return err
		}
		if v != 3 {
			t.Fatal("HLen value error")
			return nil
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
		return
	}
}

func TestTX_HIncrBy(t *testing.T) {
	db := NewDB(&DBConfig{Path: "./tmp"})
	err := db.Open()
	if err != nil {
		t.Fatal(err)
		return
	}
	defer cleanTestData()
	err = db.Update(func(tx *TX) error {
		return tx.HSet("foo", []Paris{
			{Field: []byte("bar1"), Value: []byte("1")},
			{Field: []byte("bar2"), Value: []byte("2")},
			{Field: []byte("bar3"), Value: []byte("3")},
		}...)
	})
	if err != nil {
		t.Fatal(err)
		return
	}
	err = db.Update(func(tx *TX) error {
		err := tx.HIncrBy("foo", "bar1", 1)
		if err != nil {
			return err
		}
		err = tx.HIncrBy("foo", "bar2", 2)
		return err
	})
	if err != nil {
		t.Fatal(err)
		return
	}
	err = db.View(func(tx *TX) error {
		v, err := tx.HGet("foo", "bar1")
		if err != nil {
			return err
		}
		if string(v) != "2" {
			t.Fatal("HIncrBy value error")
			return nil
		}
		v, err = tx.HGet("foo", "bar2")
		if err != nil {
			return err
		}
		if string(v) != "4" {
			t.Fatal("HIncrBy value error")
			return nil
		}
		return nil
	})
}
