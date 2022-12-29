package polarisdb

import "sync"

type ExpireEntity struct {
	TTL int64
}

type SweeperStore struct {
	sync.RWMutex
	TtlStore map[string]*ExpireEntity
}

func NewSweeperStore() *SweeperStore {
	return &SweeperStore{
		TtlStore: make(map[string]*ExpireEntity),
	}
}

func (s *SweeperStore) Len() int {
	s.RLock()
	defer s.RUnlock()
	return len(s.TtlStore)
}

func (s *SweeperStore) SampleKeys(count int) []string {
	s.Lock()
	defer s.Unlock()
	useKeys := map[string]bool{}
	keys := make([]string, 0)
	if count > len(s.TtlStore) {
		count = len(s.TtlStore)
	}
	for key := range s.TtlStore {
		if len(keys) >= count {
			break
		}
		if useKeys[key] {
			continue
		}
		keys = append(keys, key)
		useKeys[key] = true
	}
	return keys
}
