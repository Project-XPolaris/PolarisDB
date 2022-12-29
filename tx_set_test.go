package polarisdb

import (
	"fmt"
	"testing"
)

func TestTX_SAdd(t *testing.T) {
	db := NewDB(&DBConfig{Path: "./tmp"})
	err := db.Open()
	if err != nil {
		t.Fatal(err)
		return
	}
	defer cleanTestData()

	if err := db.Update(func(tx *TX) error {
		values := make([]interface{}, 0)
		for i := 0; i < 100; i++ {
			values = append(values, fmt.Sprintf("value_%d", i))
		}
		err := tx.SAdd("foo", values...)
		if err != nil {
			t.Fatal(err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.View(func(tx *TX) error {
		for i := 0; i < 100; i++ {
			exist, err := tx.SIsMember("foo", fmt.Sprintf("value_%d", i))
			if err != nil {
				t.Fatal(err)
			}
			if !exist {
				t.Fatal("not exist")
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestTX_SRem(t *testing.T) {
	db := NewDB(&DBConfig{Path: "./tmp"})
	err := db.Open()
	if err != nil {
		t.Fatal(err)
		return
	}
	defer cleanTestData()

	if err := db.Update(func(tx *TX) error {
		values := make([]interface{}, 0)
		for i := 0; i < 100; i++ {
			values = append(values, fmt.Sprintf("value_%d", i))
		}
		err := tx.SAdd("foo", values...)
		if err != nil {
			t.Fatal(err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *TX) error {
		for i := 0; i < 100; i++ {
			err := tx.SRem("foo", fmt.Sprintf("value_%d", i))
			if err != nil {
				t.Fatal(err)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.View(func(tx *TX) error {
		for i := 0; i < 100; i++ {
			exist, err := tx.SIsMember("foo", fmt.Sprintf("value_%d", i))
			if err != nil {
				t.Fatal(err)
			}
			if exist {
				t.Fatal("exist")
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestTX_SCard(t *testing.T) {
	db := NewDB(&DBConfig{Path: "./tmp"})
	err := db.Open()
	if err != nil {
		t.Fatal(err)
		return
	}
	defer cleanTestData()

	if err := db.Update(func(tx *TX) error {
		values := make([]interface{}, 0)
		for i := 0; i < 100; i++ {
			values = append(values, fmt.Sprintf("value_%d", i))
		}
		err := tx.SAdd("foo", values...)
		if err != nil {
			t.Fatal(err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.View(func(tx *TX) error {
		count, err := tx.SCard("foo")
		if err != nil {
			t.Fatal(err)
		}
		if count != 100 {
			t.Fatal("count not match")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestTX_SMembers(t *testing.T) {
	db := NewDB(&DBConfig{Path: "./tmp"})
	err := db.Open()
	if err != nil {
		t.Fatal(err)
		return
	}
	defer cleanTestData()

	if err := db.Update(func(tx *TX) error {
		values := make([]interface{}, 0)
		for i := 0; i < 100; i++ {
			values = append(values, fmt.Sprintf("value_%d", i))
		}
		err := tx.SAdd("foo", values...)
		if err != nil {
			t.Fatal(err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.View(func(tx *TX) error {
		values := make([]interface{}, 0)
		for i := 0; i < 100; i++ {
			values = append(values, fmt.Sprintf("value_%d", i))
		}
		members, err := tx.SMIsMembers("foo", values...)
		if err != nil {
			t.Fatal(err)
		}
		if len(members) != 100 {
			t.Fatal("count not match")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestTX_SDiff(t *testing.T) {
	db := NewDB(&DBConfig{Path: "./tmp"})
	err := db.Open()
	if err != nil {
		t.Fatal(err)
		return
	}
	defer cleanTestData()

	if err := db.Update(func(tx *TX) error {
		values := make([]interface{}, 0)
		for i := 0; i < 100; i++ {
			values = append(values, fmt.Sprintf("value_%d", i))
		}
		err := tx.SAdd("foo", values...)
		if err != nil {
			t.Fatal(err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *TX) error {
		values := make([]interface{}, 0)
		for i := 0; i < 50; i++ {
			values = append(values, fmt.Sprintf("value_%d", i))
		}
		err := tx.SAdd("bar", values...)
		if err != nil {
			t.Fatal(err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.View(func(tx *TX) error {
		values, err := tx.SDiff("foo", "bar")
		if err != nil {
			t.Fatal(err)
		}
		if len(values) != 50 {
			t.Fatal("count not match")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestTX_SInter(t *testing.T) {
	db := NewDB(&DBConfig{Path: "./tmp"})
	err := db.Open()
	if err != nil {
		t.Fatal(err)
		return
	}
	defer cleanTestData()

	if err := db.Update(func(tx *TX) error {
		values := make([]interface{}, 0)
		for i := 0; i < 100; i++ {
			values = append(values, fmt.Sprintf("value_%d", i))
		}
		err := tx.SAdd("foo", values...)
		if err != nil {
			t.Fatal(err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *TX) error {
		values := make([]interface{}, 0)
		for i := 0; i < 50; i++ {
			values = append(values, fmt.Sprintf("value_%d", i))
		}
		err := tx.SAdd("bar", values...)
		if err != nil {
			t.Fatal(err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.View(func(tx *TX) error {
		values, err := tx.SInter("foo", "bar")
		if err != nil {
			t.Fatal(err)
		}
		if len(values) != 50 {
			t.Fatal("count not match")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestTX_SUnion(t *testing.T) {
	db := NewDB(&DBConfig{Path: "./tmp"})
	err := db.Open()
	if err != nil {
		t.Fatal(err)
		return
	}
	defer cleanTestData()

	if err := db.Update(func(tx *TX) error {
		values := make([]interface{}, 0)
		for i := 0; i < 100; i++ {
			values = append(values, fmt.Sprintf("value_%d", i))
		}
		err := tx.SAdd("foo", values...)
		if err != nil {
			t.Fatal(err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *TX) error {
		values := make([]interface{}, 0)
		for i := 100; i < 150; i++ {
			values = append(values, fmt.Sprintf("value_%d", i))
		}
		err := tx.SAdd("bar", values...)
		if err != nil {
			t.Fatal(err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.View(func(tx *TX) error {
		values, err := tx.SUnion("foo", "bar")
		if err != nil {
			t.Fatal(err)
		}
		if len(values) != 150 {
			t.Fatal("count not match")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestTX_SPop(t *testing.T) {
	db := NewDB(&DBConfig{Path: "./tmp"})
	err := db.Open()
	if err != nil {
		t.Fatal(err)
		return
	}
	defer cleanTestData()

	if err := db.Update(func(tx *TX) error {
		for i := 0; i < 100; i++ {
			err := tx.SAdd("foo", fmt.Sprintf("value_%d", i))
			if err != nil {
				t.Fatal(err)
			}
		}
		for i := 0; i < 100; i++ {
			err = tx.SAdd("bar", i)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	outMembers := make([]interface{}, 0)
	if err := db.Update(func(tx *TX) error {
		value, err := tx.SPop("foo", 10)
		if err != nil {
			t.Fatal(err)
		}
		outMembers = append(outMembers, value...)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	db2 := NewDB(&DBConfig{Path: "./tmp"})
	err = db2.Open()
	if err != nil {
		t.Fatal(err)
		return
	}
	if err = db.Update(func(tx *TX) error {
		for _, member := range outMembers {
			isExist, err := tx.SIsMember("foo", member)
			if err != nil {
				return err
			}
			if isExist {
				t.Fatal(fmt.Sprintf("member %v should not exist", member))
			}
		}
		return nil

	}); err != nil {
		t.Fatal(err)
	}
}
