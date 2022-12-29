package polarisdb

import (
	"github.com/projectxpolaris/polarisdb/radix"
	"sync"
)

type StringStore struct {
	Tree *radix.RadixTree
	sync.RWMutex
}

func NewStore() *StringStore {
	return &StringStore{
		Tree: radix.NewTree(),
	}
}

func (s *StringStore) write(key []byte, data []byte) {
	s.Lock()
	defer s.Unlock()
	s.Tree.Set(key, data)
}

func (s *StringStore) read(key []byte) ([]byte, error) {
	s.RLock()
	defer s.RUnlock()
	data, err := s.Tree.Get(key)
	if err != nil {
		return nil, err
	}
	return data, nil
}
func (s *StringStore) delete(key []byte) error {
	s.Lock()
	defer s.Unlock()
	return s.Tree.Delete(key)
}
func (s *StringStore) keys() ([]string, error) {
	s.RLock()
	defer s.RUnlock()
	keys := make([]string, 0)
	s.Tree.Walk(func(key []byte, _ []byte) {
		keys = append(keys, string(key))
	})
	return keys, nil
}
