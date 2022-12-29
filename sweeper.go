package polarisdb

import (
	"context"
	"math"
	"math/rand"
	"time"
)

var (
	minRandomInterval       = 500 * time.Millisecond
	maxRandomInterval       = 2 * minRandomInterval
	noExpire          int64 = -1
)
var (
	EvictAllKeyRandom = "allkeys-random"
	EvictAllKeyLRU    = "allkeys-lru"
	EvictVolRandom    = "volatile-random"
	EvictVolLRU       = "volatile-lru"
	EvictNoEviction   = "noeviction"
)

type Sweeper struct {
	Store *SweeperStore
	db    *PolarisDB
}

type Sweepable interface {
	delete(key []byte) error
	keys() ([]string, error)
}

func NewSweeper(db *PolarisDB) *Sweeper {
	return &Sweeper{
		Store: NewSweeperStore(),
		db:    db,
	}
}

func startupDelay() time.Duration {
	rand := rand.New(rand.NewSource(time.Now().UnixNano()))
	d, delta := minRandomInterval, maxRandomInterval-minRandomInterval
	if delta > 0 {
		d += time.Duration(rand.Int63n(int64(delta)))
	}
	return d
}

func (s *Sweeper) run(stop context.Context) {
	// get random interval
	<-time.After(startupDelay())
	ticker := time.NewTicker(time.Duration(s.db.Config.SweeperInterval) * time.Millisecond)
	evictTicker := time.NewTicker(time.Duration(s.db.Config.EvicterInterval) * time.Second)
	for {
		select {
		case <-ticker.C:
			s.sweep()
		case <-evictTicker.C:
			s.evict()
		case <-stop.Done():
			ticker.Stop()
			return
		}
	}
}
func (s *Sweeper) sweep() error {
	s.Store.Lock()
	defer s.Store.Unlock()
	for key, entity := range s.Store.TtlStore {
		if entity.TTL < time.Now().UnixMilli() {
			// evict the key
			obj, isExist := s.db.Dict.Find(key)
			if !isExist {
				delete(s.Store.TtlStore, key)
				continue
			}
			// is string obj
			if strStore, ok := obj.Ptr.(*StringStore); ok {
				strStore.delete([]byte(key))
			}
			s.db.Dict.Delete(key)
			// is hash obj
			delete(s.Store.TtlStore, key)

		}
	}
	return nil
}

func (s *Sweeper) evict() error {
	evictPolicy := s.db.Config.EvicterPolicy
	switch evictPolicy {
	case EvictAllKeyRandom:
		RandomSweeper(s.db)
	case EvictAllKeyLRU:
		LruSweeper(s.db)
	case EvictVolRandom:
		RandomExpireKeySweeper(s.db)
	case EvictVolLRU:
		LruSweeper(s.db)
	}
	return nil
}

func (s *Sweeper) SetKeyExpire(key string, ttl int64) {
	s.Store.Lock()
	defer s.Store.Unlock()
	// find the type of the key
	s.Store.TtlStore[key] = &ExpireEntity{
		TTL: ttl,
	}
}
func (s *Sweeper) GetExpire(key string) int64 {
	s.Store.Lock()
	defer s.Store.Unlock()
	if entity, ok := s.Store.TtlStore[key]; ok {
		return entity.TTL
	}
	return noExpire
}

func (s *Sweeper) isExpire(key string) bool {
	s.Store.Lock()
	defer s.Store.Unlock()
	if entity, ok := s.Store.TtlStore[key]; ok {
		return entity.TTL < time.Now().UnixMilli()
	}
	return false
}

func (s *Sweeper) TryRemoveExpire(key string) {
	s.Store.Lock()
	defer s.Store.Unlock()
	if _, ok := s.Store.TtlStore[key]; ok {
		delete(s.Store.TtlStore, key)
	}
}
func (s *Sweeper) RemoveExpire(key string) {
	s.Store.Lock()
	defer s.Store.Unlock()
	delete(s.Store.TtlStore, key)
}

// allkey=random
func RandomSweeper(db *PolarisDB) {
	removeCount := float64(db.Dict.Len()) * db.Config.RandomRemoveFactor
	count := math.Floor(removeCount)
	db.Dict.RandomRemoveKey(int(count))
}

// volatile-random
func RandomExpireKeySweeper(db *PolarisDB) {
	removeCount := float64(db.Dict.Len()) * db.Config.RandomRemoveFactor
	count := math.Floor(removeCount)
	keys := db.Sweeper.Store.SampleKeys(int(count))
	for _, key := range keys {
		db.Dict.Delete(key)
	}
}

func FindBestLRUKey(db *PolarisDB, keys []string) string {
	largeLRU := -1.0
	bestKey := ""
	for _, key := range keys {
		ent, isExist := db.Dict.FindRaw(key)
		if !isExist {
			continue
		}
		if db.Clock.GetLruNow(ent.LRU) > largeLRU {
			largeLRU = db.Clock.GetLruNow(ent.LRU)
			bestKey = key
		}
	}
	return bestKey
}

// allkey=lru
func LruSweeper(db *PolarisDB) {
	removeCount := float64(db.Dict.Len()) * db.Config.LruSampleFactor
	count := math.Floor(removeCount)
	keys := db.Dict.SampleKeys(int(count))
	bestKey := FindBestLRUKey(db, keys)
	if bestKey != "" {
		db.Dict.Delete(bestKey)
	}
}

// volatile-ttl
func LruTTLKeySweeper(db *PolarisDB) {
	removeCount := float64(db.Sweeper.Store.Len()) * db.Config.LruSampleFactor
	if (removeCount) < 1 {
		removeCount = 1
	}
	keys := db.Sweeper.Store.SampleKeys(int(removeCount))
	if len(keys) == 0 {
		return
	}
	bestKey := FindBestLRUKey(db, keys)
	if bestKey != "" {
		db.Dict.Delete(bestKey)
	}
}
