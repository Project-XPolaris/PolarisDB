package polarisdb

import (
	"math/rand"
	"testing"
	"time"
)

func TestSweeper(t *testing.T) {
	db := NewDB(&DBConfig{Path: "./tmp"})
	err := db.Open()
	if err != nil {
		t.Fatal(err)
		return
	}
	defer cleanTestData()
	err = db.Update(func(tx *TX) error {
		tx.SetString("foo", "bar", false)
		tx.SetExpire("foo", 1)
		return nil
	})
	if err != nil {
		t.Fatal(err)
		return
	}
	<-time.After(1500 * time.Millisecond)
}
func generateRandomString(textLen int) string {
	var text = make([]byte, textLen)
	var charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	for i := range text {
		text[i] = charset[rand.Intn(len(charset))]
	}
	return string(text)
}
func generateRandomNum(min, max int64) int64 {
	rand.Seed(time.Now().UnixNano())
	return rand.Int63n(max-min) + min
}
func TestRandomSweeper(t *testing.T) {
	db := NewDB(&DBConfig{Path: "./tmp"})
	err := db.Open()
	if err != nil {
		t.Fatal(err)
		return
	}
	defer cleanTestData()
	err = db.Update(func(tx *TX) error {
		for i := 0; i < 100; i++ {
			err := tx.LPush(generateRandomString(8), []byte(generateRandomString(48)))
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
	for i := 0; i < 100; i++ {
		RandomSweeper(db)
	}
}

func TestLruSweeper(t *testing.T) {
	time2Key := make(map[string]int64)
	for i := 0; i < 100; i++ {
		time2Key[generateRandomString(8)] = generateRandomNum(0, 50)
	}
	db := NewDB(&DBConfig{Path: "./tmp"})
	err := db.Open()
	if err != nil {
		t.Fatal(err)
		return
	}
	defer cleanTestData()
	err = db.Update(func(tx *TX) error {
		for key, _ := range time2Key {
			err := tx.SetString(key, generateRandomString(48), false)
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

	for key, review := range time2Key {
		for entKey, entity := range db.Dict.Data.Data {
			if entKey == key {
				entity.LRU = db.Clock.GetTime() - float64(review)
			}
		}
	}
	for i := 0; i < 100; i++ {
		LruSweeper(db)
	}
	if db.Dict.Data.Len() != 0 {
		t.Fatal("sweeper failed")
		return
	}
}
func TestLruTTLKeySweeper(t *testing.T) {
	db := NewDB(&DBConfig{Path: "./tmp"})
	err := db.Open()
	if err != nil {
		t.Fatal(err)
		return
	}
	// generate mock data
	keys := make([]string, 0)
	for i := 0; i < 100; i++ {
		keys = append(keys, generateRandomString(8))
	}
	defer cleanTestData()
	// init data
	err = db.Update(func(tx *TX) error {
		for _, key := range keys {
			err := tx.LPush(key, []byte(generateRandomString(48)))
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
	// set ttl
	err = db.Update(func(tx *TX) error {
		for idx, key := range keys {
			if idx%2 == 0 {
				continue
			}
			err := tx.SetExpire(key, generateRandomNum(30, 50))
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
	for _, entity := range db.Dict.Data.Data {
		entity.LRU = db.Clock.GetTime() - float64(generateRandomNum(0, 100))
	}
	for i := 0; i < 100; i++ {
		LruTTLKeySweeper(db)
	}
	if db.Dict.Data.Len() != 50 {
		t.Fatal("sweeper failed")
		return
	}
}

func TestRandomTTLListSweeper(t *testing.T) {
	db := NewDB(&DBConfig{Path: "./tmp"})
	err := db.Open()
	if err != nil {
		t.Fatal(err)
		return
	}
	// generate mock data
	keys := make([]string, 0)
	for i := 0; i < 100; i++ {
		keys = append(keys, generateRandomString(8))
	}
	defer cleanTestData()
	// init data
	err = db.Update(func(tx *TX) error {
		for _, key := range keys {
			err := tx.LPush(key, []byte(generateRandomString(48)))
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
	// set ttl
	err = db.Update(func(tx *TX) error {
		for idx, key := range keys {
			if idx%2 == 0 {
				continue
			}
			err := tx.SetExpire(key, generateRandomNum(0, 50))
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
	for _, entity := range db.Dict.Data.Data {
		entity.LRU = db.Clock.GetTime() - float64(generateRandomNum(0, 100))
	}
	for i := 0; i < 100; i++ {
		RandomExpireKeySweeper(db)
	}
}
