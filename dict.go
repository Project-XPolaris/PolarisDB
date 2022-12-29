package polarisdb

import (
	"github.com/projectxpolaris/polarisdb/dict"
	"sync"
)

type Object interface {
}
type KeyEntity struct {
	Ptr Object
	LRU float64
}

type KeyDict struct {
	sync.RWMutex
	Data *dict.Dict[*KeyEntity]
	db   *PolarisDB
}

func NewKeyDict() *KeyDict {
	return &KeyDict{
		Data: dict.NewDict[*KeyEntity](),
	}
}

func (d *KeyDict) Add(key string, value *KeyEntity) {
	d.Lock()
	defer d.Unlock()
	value.LRU = d.db.Clock.GetTime()
	d.db.Sweeper.TryRemoveExpire(key)
	d.Data.Add(key, value)
}
func (d *KeyDict) FindRaw(key string) (*KeyEntity, bool) {
	d.RLock()
	defer d.RUnlock()
	value, isExist := d.Data.Find(key)
	return value, isExist
}
func (d *KeyDict) Find(key string) (*KeyEntity, bool) {
	d.Lock()
	defer d.Unlock()
	// check if it expire
	isExpire := d.db.Sweeper.isExpire(key)
	if isExpire {
		d.db.Sweeper.TryRemoveExpire(key)
		return nil, false
	}
	value, isExist := d.Data.Find(key)
	if isExist {
		value.LRU = d.db.Clock.GetTime()
	}
	return value, isExist
}

func (d *KeyDict) Delete(keys ...string) {
	d.Lock()
	defer d.Unlock()
	for _, key := range keys {
		d.db.Sweeper.TryRemoveExpire(key)
		d.Data.Delete(key)
	}
}

func (d *KeyDict) RandomRemoveKey(count int) {
	d.Lock()
	defer d.Unlock()

	cur := 0
	for {
		if cur >= count {
			break
		}
		key := d.Data.RandomKey()
		if key == "" {
			break
		}
		d.Data.Delete(key)
		cur++
	}
}
func (d *KeyDict) Len() int {
	return d.Data.Len()
}
func (d *KeyDict) Keys() []string {
	return d.Data.Keys()
}

// random sample key
func (d *KeyDict) SampleKeys(count int) []string {
	d.Lock()
	defer d.Unlock()
	keys := make([]string, 0, count)
	for key := range d.Data.Data {
		keys = append(keys, key)
		if len(keys) >= count {
			break
		}
	}
	return keys
}
