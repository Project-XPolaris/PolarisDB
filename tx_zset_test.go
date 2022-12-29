package polarisdb

import (
	"fmt"
	"testing"
)

func TestZsetAdd(t *testing.T) {
	db := NewDB(&DBConfig{Path: "./tmp"})
	err := db.Open()
	if err != nil {
		t.Fatal(err)
		return
	}
	defer cleanTestData()
	err = db.Update(func(tx *TX) error {
		for i := 0; i < 100; i++ {
			err := tx.ZAdd("foo", ZsetPair{
				Member: fmt.Sprintf("data_%d", i),
				Score:  float64(i),
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
	err = db.View(func(tx *TX) error {

		zsetLen, err := tx.ZCard("foo")
		if err != nil {
			t.Fatal(err)
			return nil
		}
		if zsetLen != 100 {
			t.Fatal("zset len not equal")
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
	err = db2.View(func(tx *TX) error {
		zsetLen, err := tx.ZCard("foo")
		if err != nil {
			t.Fatal(err)
			return nil
		}
		if zsetLen != 100 {
			t.Fatal("zset len not equal")
			return nil
		}
		members, err := tx.ZRange("foo", 0, 99)
		if err != nil {
			return err
		}
		for i := 0; i < 100; i++ {
			if members[i].(string) != fmt.Sprintf("data_%d", i) {
				t.Fatal("read data not equal")
				return nil
			}
		}
		return nil
	})

}

func TestTX_ZRem(t *testing.T) {
	db := NewDB(&DBConfig{Path: "./tmp"})
	err := db.Open()
	if err != nil {
		t.Fatal(err)
		return
	}
	defer cleanTestData()
	err = db.Update(func(tx *TX) error {
		for i := 0; i < 100; i++ {
			err := tx.ZAdd("foo",
				ZsetPair{
					Member: fmt.Sprintf("member_%d", i),
					Score:  float64(i),
				})
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
		return
	}
	err = db.Update(func(tx *TX) error {
		members := []string{"member_1", "member_2", "member_3"}
		err := tx.ZRem("foo", members...)
		if err != nil {
			return err
		}
		return nil
	})
	db2 := NewDB(&DBConfig{Path: "./tmp"})
	err = db2.Open()
	err = db2.View(func(tx *TX) error {
		zsetLen, err := tx.ZCard("foo")
		if err != nil {
			t.Fatal(err)
			return nil
		}
		if zsetLen != 97 {
			t.Fatal("zset len not equal")
			return nil
		}
		return nil
	})
}

func TestTX_ZDiff(t *testing.T) {
	db := NewDB(&DBConfig{Path: "./tmp"})
	err := db.Open()
	if err != nil {
		t.Fatal(err)
		return
	}
	defer cleanTestData()
	err = db.Update(func(tx *TX) error {
		for i := 0; i < 100; i++ {
			err := tx.ZAdd("foo",
				ZsetPair{
					Member: fmt.Sprintf("member_%d", i),
					Score:  float64(i),
				})
			if err != nil {
				return err
			}
		}
		for i := 0; i < 50; i++ {
			err := tx.ZAdd("bar",
				ZsetPair{
					Member: fmt.Sprintf("member_%d", i),
					Score:  float64(i),
				})
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
		return
	}
	err = db.View(func(tx *TX) error {
		members, err := tx.ZDiff("foo", "bar")
		if err != nil {
			return err
		}
		if len(members) != 100 {
			t.Fatal("zdiff result not equal")
			return nil
		}
		setCard, err := tx.ZDiffCard("foo", "bar")
		if err != nil {
			return err
		}
		if setCard != 50 {
			t.Fatal("zset CARD not equal")
			return nil
		}
		return nil
	})
}
func TestTX_ZInter(t *testing.T) {
	db := NewDB(&DBConfig{Path: "./tmp"})
	err := db.Open()
	if err != nil {
		t.Fatal(err)
		return
	}
	defer cleanTestData()
	err = db.Update(func(tx *TX) error {
		for i := 0; i < 100; i++ {
			err := tx.ZAdd("foo",
				ZsetPair{
					Member: fmt.Sprintf("member_%d", i),
					Score:  float64(i),
				})
			if err != nil {
				return err
			}
		}
		for i := 0; i < 50; i++ {
			err := tx.ZAdd("bar",
				ZsetPair{
					Member: fmt.Sprintf("member_%d", i),
					Score:  float64(i),
				})
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
		return
	}

	err = db.View(func(tx *TX) error {
		members, err := tx.ZInter("foo", "bar")
		if err != nil {
			return err
		}
		if len(members) != 100 {
			t.Fatal("zinter result not equal")
			return nil
		}
		setCard, err := tx.ZInterCard("foo", "bar")
		if err != nil {
			return err
		}
		if setCard != 50 {
			t.Fatal("zset CARD not equal")
			return nil
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
		return
	}

}

func TestTX_ZUnion(t *testing.T) {
	db := NewDB(&DBConfig{Path: "./tmp"})
	err := db.Open()
	if err != nil {
		t.Fatal(err)
		return
	}
	defer cleanTestData()
	err = db.Update(func(tx *TX) error {
		for i := 0; i < 100; i++ {
			err := tx.ZAdd("foo",
				ZsetPair{
					Member: fmt.Sprintf("member_%d", i),
					Score:  float64(i),
				})
			if err != nil {
				return err
			}
		}
		for i := 0; i < 50; i++ {
			err := tx.ZAdd("bar",
				ZsetPair{
					Member: fmt.Sprintf("member_%d", i),
					Score:  float64(i),
				})
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
		return
	}
	err = db.View(func(tx *TX) error {
		members, err := tx.ZUnion("foo", "bar")
		if err != nil {
			return err
		}
		if len(members) != 200 {
			t.Fatal("zunion result not equal")
			return nil
		}
		setCard, err := tx.ZUnionCard("foo", "bar")
		if err != nil {
			return err
		}
		if setCard != 100 {
			t.Fatal("zset CARD not equal")
			return nil
		}
		return nil
	})
}

func TestTX_ZDiffStore(t *testing.T) {
	db := NewDB(&DBConfig{Path: "./tmp"})
	err := db.Open()
	if err != nil {
		t.Fatal(err)
		return
	}
	defer cleanTestData()
	err = db.Update(func(tx *TX) error {
		for i := 0; i < 100; i++ {
			err := tx.ZAdd("foo",
				ZsetPair{
					Member: fmt.Sprintf("member_%d", i),
					Score:  float64(i),
				})
			if err != nil {
				return err
			}
		}
		for i := 0; i < 50; i++ {
			err := tx.ZAdd("bar",
				ZsetPair{
					Member: fmt.Sprintf("member_%d", i),
					Score:  float64(i),
				})
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
		return
	}
	err = db.View(func(tx *TX) error {
		newSetLen, err := tx.ZDiffStore("result", "foo", "bar")
		if err != nil {
			return err
		}
		if newSetLen != 50 {
			t.Fatal("zdiff result not equal")
			return nil
		}
		return nil
	})

	db2 := NewDB(&DBConfig{Path: "./tmp"})
	err = db2.Open()
	if err != nil {
		t.Fatal(err)
		return
	}
	err = db2.View(func(tx *TX) error {
		setLen, err := tx.ZCard("result")
		if err != nil {
			return err
		}
		if setLen != 50 {
			t.Fatal("zdiff result not equal")
			return nil
		}
		return nil
	})
}

func TestTX_ZInterStore(t *testing.T) {
	db := NewDB(&DBConfig{Path: "./tmp"})
	err := db.Open()
	if err != nil {
		t.Fatal(err)
		return
	}
	defer cleanTestData()
	err = db.Update(func(tx *TX) error {
		for i := 0; i < 100; i++ {
			err := tx.ZAdd("foo",
				ZsetPair{
					Member: fmt.Sprintf("member_%d", i),
					Score:  float64(i),
				})
			if err != nil {
				return err
			}
		}
		for i := 0; i < 50; i++ {
			err := tx.ZAdd("bar",
				ZsetPair{
					Member: fmt.Sprintf("member_%d", i),
					Score:  float64(i),
				})
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
		return
	}
	err = db.View(func(tx *TX) error {
		newSetLen, err := tx.ZInterStore("result", "foo", "bar")
		if err != nil {
			return err
		}
		if newSetLen != 50 {
			t.Fatal("zinter result not equal")
			return nil
		}
		return nil
	})

	db2 := NewDB(&DBConfig{Path: "./tmp"})
	err = db2.Open()
	if err != nil {
		t.Fatal(err)
		return
	}
	err = db2.View(func(tx *TX) error {
		setLen, err := tx.ZCard("result")
		if err != nil {
			return err
		}
		if setLen != 50 {
			t.Fatal("zinter result not equal")
			return nil
		}
		return nil
	})
}

func TestTX_ZUnionStore(t *testing.T) {
	db := NewDB(&DBConfig{Path: "./tmp"})
	err := db.Open()
	if err != nil {
		t.Fatal(err)
		return
	}
	defer cleanTestData()
	err = db.Update(func(tx *TX) error {
		for i := 0; i < 100; i++ {
			err := tx.ZAdd("foo",
				ZsetPair{
					Member: fmt.Sprintf("member_%d", i),
					Score:  float64(i),
				})
			if err != nil {
				return err
			}
		}
		for i := 0; i < 50; i++ {
			err := tx.ZAdd("bar",
				ZsetPair{
					Member: fmt.Sprintf("member_%d", i),
					Score:  float64(i),
				})
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
		return
	}
	err = db.View(func(tx *TX) error {
		newSetLen, err := tx.ZUnionStore("result", "foo", "bar")
		if err != nil {
			return err
		}
		if newSetLen != 100 {
			t.Fatal("zunion result not equal")
			return nil
		}
		return nil
	})

	db2 := NewDB(&DBConfig{Path: "./tmp"})
	err = db2.Open()
	if err != nil {
		t.Fatal(err)
		return
	}
	err = db2.View(func(tx *TX) error {
		setLen, err := tx.ZCard("result")
		if err != nil {
			return err
		}
		if setLen != 50 {
			t.Fatal("zunion result not equal")
			return nil
		}
		return nil
	})
}

func TestZIncrBy(t *testing.T) {
	db := NewDB(&DBConfig{Path: "./tmp"})
	err := db.Open()
	if err != nil {
		t.Fatal(err)
		return
	}
	defer cleanTestData()
	err = db.Update(func(tx *TX) error {
		err := tx.ZAdd("foo",
			ZsetPair{
				Member: "bar",
				Score:  1,
			})
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
		newScore, err := tx.ZIncrBy("foo", 2, "bar")
		if err != nil {
			return err
		}
		if newScore != 3 {
			t.Fatal("zincrby result not equal")
			return nil
		}
		return nil
	})
	db2 := NewDB(&DBConfig{Path: "./tmp"})
	err = db2.Open()
	if err != nil {
		t.Fatal(err)
	}
	err = db2.View(func(tx *TX) error {
		score, err := tx.ZScore("foo", "bar")
		if err != nil {
			return err
		}
		if score != 3 {
			t.Fatal("zincrby result not equal")
			return nil
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
		return
	}
}
