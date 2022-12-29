package set

import (
	"errors"
	"math/rand"
	"strconv"
)

var (
	MaxContentLength = 512
)

type IntSet struct {
	contents []int64
}

func (s *IntSet) RandMember() interface{} {
	if s.Len() == 0 {
		return nil
	}
	randomIndex := rand.Intn(s.Len())
	return s.contents[randomIndex]
}

// random pop
func (s *IntSet) Pop() interface{} {
	if s.Len() == 0 {
		return nil
	}
	randomIndex := rand.Intn(s.Len())
	result := s.contents[randomIndex]
	s.contents = append(s.contents[:randomIndex], s.contents[randomIndex+1:]...)
	return result
}

func (s *IntSet) All() []interface{} {
	result := make([]interface{}, 0)
	for _, v := range s.contents {
		result = append(result, v)
	}
	return result
}

func (s *IntSet) Len() int {
	return len(s.contents)
}

func (s *IntSet) toInt64(data interface{}) (int64, error) {
	switch data.(type) {
	case int64:
		return data.(int64), nil
	case int:
		return int64(data.(int)), nil
	case int32:
		return int64(data.(int32)), nil
	case int16:
		return int64(data.(int16)), nil
	case int8:
		return int64(data.(int8)), nil
	case uint64:
		return int64(data.(uint64)), nil
	case uint32:
		return int64(data.(uint32)), nil
	case uint16:
		return int64(data.(uint16)), nil
	case uint8:
		return int64(data.(uint8)), nil
	case uint:
		return int64(data.(uint)), nil
	case string:
		value, err := strconv.ParseInt(data.(string), 10, 64)
		if err != nil {
			return 0, err
		}
		return value, nil
	default:
		return 0, errors.New("not support type")
	}
}
func (s *IntSet) CanInsert(data interface{}) bool {
	if !canUseIntSet(data) {
		return false
	}
	if s.Len() == MaxContentLength {
		return false
	}
	return true
}
func (s *IntSet) Add(i interface{}) error {
	value, err := s.toInt64(i)
	if err != nil {
		return err
	}
	if s.Len() == MaxContentLength {
		return nil
	}
	// find to insert
	for index, v := range s.contents {
		if v == i {
			return nil
		}
		if v > value {
			s.contents = append(s.contents[:index], append([]int64{value}, s.contents[index:]...)...)
			return nil
		}
	}
	s.contents = append(s.contents, value)
	return nil
}

func (s *IntSet) Remove(i interface{}) error {
	value, err := s.toInt64(i)
	if err != nil {
		return err
	}
	for index, v := range s.contents {
		if v == value {
			s.contents = append(s.contents[:index], s.contents[index+1:]...)
			break
		}
	}
	return nil
}

func (s *IntSet) Contains(i interface{}) (bool, error) {
	value, err := s.toInt64(i)
	if err != nil {
		return false, err
	}
	// binary search
	low := 0
	high := s.Len() - 1
	for low <= high {
		mid := (low + high) / 2
		if s.contents[mid] == value {
			return true, nil
		} else if s.contents[mid] > value {
			high = mid - 1
		} else {
			low = mid + 1
		}
	}
	return false, nil
}

func NewIntSet() *IntSet {
	return &IntSet{
		contents: make([]int64, 0),
	}
}
func (s *IntSet) toHashSet() *HashSet {
	h := NewHashSet()
	for _, v := range s.contents {
		h.Add(v)
	}
	return h
}

func canUseIntSet(data interface{}) bool {
	switch data.(type) {
	case int64:
		return true
	case int:
		return true
	case int32:
		return true
	case int16:
		return true
	case int8:
		return true
	case uint64:
		return true
	case uint32:
		return true
	case uint16:
		return true
	case uint8:
		return true
	case uint:
		return true
	case string:
		// can convert?
		_, err := strconv.ParseInt(data.(string), 10, 64)
		if err != nil {
			return false
		}
		return true
	default:
		return false
	}
}
