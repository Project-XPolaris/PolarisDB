package set

import (
	"sort"
)

type Store interface {
	Add(i interface{}) error
	Remove(i interface{}) error
	Contains(i interface{}) (bool, error)
	All() []interface{}
	Pop() interface{}
	RandMember() interface{}
	Len() int
}

type Set struct {
	intSet  *IntSet
	hashSet *HashSet
}

func NewSet() *Set {
	return &Set{
		intSet: NewIntSet(),
	}
}
func (s *Set) GetStore() Store {
	if s.intSet != nil {
		return s.intSet
	}
	return s.hashSet
}
func (s *Set) Add(i interface{}) error {
	store := s.GetStore()
	if store == nil {
		if canUseIntSet(i) {
			store = NewIntSet()
		} else {
			store = NewHashSet()
		}
	}

	if intSet, ok := store.(*IntSet); ok {
		if intSet.CanInsert(i) {
			s.intSet = intSet
			return intSet.Add(i)
		} else {
			store = intSet.toHashSet()
			s.intSet = nil
		}
	}
	store.Add(i)
	s.hashSet = store.(*HashSet)
	return nil
}

func (s *Set) Len() int {
	store := s.GetStore()
	return store.Len()
}

func (s *Set) Remove(i interface{}) error {
	store := s.GetStore()
	return store.Remove(i)
}

func (s *Set) Contains(i interface{}) (bool, error) {
	store := s.GetStore()
	return store.Contains(i)
}

func Diff(targetSet *Set, otherSets ...*Set) []interface{} {
	// select method
	selectMethod := 1
	totalOtherCount := 0
	for _, set := range otherSets {
		totalOtherCount += set.Len()
	}

	if targetSet.Len()*len(otherSets) > totalOtherCount+targetSet.Len() {
		selectMethod = 2
	}

	if selectMethod == 1 {
		// sort other sets by length
		sort.Slice(otherSets, func(i, j int) bool {
			return otherSets[i].Len() < otherSets[j].Len()
		})
		//O(N*M)
		result := make([]interface{}, 0)
		for _, data := range targetSet.Members() {
			existFlag := true
			for _, otherSet := range otherSets {
				if ok, _ := otherSet.Contains(data); !ok {
					existFlag = false
					break
				}
			}
			if !existFlag {
				result = append(result, data)
			}
		}
		return result
	} else {
		// O(N)
		targetSetList := targetSet.Members()
		for _, otherSet := range otherSets {
			for _, data := range otherSet.Members() {
				for i, targetData := range targetSetList {
					if targetData == data {
						targetSetList = append(targetSetList[:i], targetSetList[i+1:]...)
						break
					}
				}
			}
		}
		return targetSetList
	}
}

func DiffStore(targetSet *Set, otherSets ...*Set) (*Set, error) {
	diffList := Diff(targetSet, otherSets...)
	set := NewSet()
	for _, data := range diffList {
		err := set.Add(data)
		if err != nil {
			return nil, err
		}
	}
	return set, nil
}

func Intersection(sets ...*Set) []interface{} {
	// sort sets by length
	sort.Slice(sets, func(i, j int) bool {
		return sets[i].Len() < sets[j].Len()
	})
	//O(N*M)
	result := make([]interface{}, 0)
	for _, data := range sets[0].Members() {
		existFlag := true
		for _, otherSet := range sets[1:] {
			if ok, _ := otherSet.Contains(data); !ok {
				existFlag = false
				break
			}
		}
		if existFlag {
			result = append(result, data)
		}
	}
	return result
}

func IntersectionStore(sets ...*Set) (*Set, error) {
	intersectionList := Intersection(sets...)
	resultSet := NewSet()
	for _, data := range intersectionList {
		err := resultSet.Add(data)
		if err != nil {
			return nil, err
		}
	}
	return resultSet, nil
}

func Move(sourceSet, destSet *Set, data interface{}) (bool, error) {
	isContain, err := sourceSet.Contains(data)
	if err != nil {
		return false, err
	}
	if !isContain {
		return false, nil
	}
	err = destSet.Add(data)
	if err != nil {
		return false, err
	}
	err = sourceSet.Remove(data)
	if err != nil {
		return false, err
	}
	return true, nil
}

func (s *Set) Pop(count int) ([]interface{}, error) {
	store := s.GetStore()
	if s.Len() < count {
		count = s.Len()
	}
	result := make([]interface{}, 0)
	for i := 0; i < count; i++ {
		data := store.Pop()
		result = append(result, data)
	}
	return result, nil
}
func (s *Set) RandomMembers(count int) []interface{} {
	store := s.GetStore()
	if store.Len() < count {
		count = store.Len()
	}
	result := make([]interface{}, 0)
	cur := 0
	for cur < count {
		data := store.RandMember()
		// if is already in result, then rand again
		isExist := false
		for _, r := range result {
			if r == data {
				isExist = true
				break
			}
		}
		if !isExist {
			result = append(result, data)
			cur++
		}
	}
	return result
}

func Union(sets ...*Set) []interface{} {
	unionMap := make(map[interface{}]bool, 0)
	result := make([]interface{}, 0)
	for _, set := range sets {
		for _, data := range set.Members() {
			if _, ok := unionMap[data]; !ok {
				unionMap[data] = true
				result = append(result, data)
			}
		}
	}
	return result
}

func UnionStore(sets ...*Set) (*Set, error) {
	unionList := Union(sets...)
	resultSet := NewSet()
	for _, data := range unionList {
		err := resultSet.Add(data)
		if err != nil {
			return nil, err
		}
	}
	return resultSet, nil
}

func (s *Set) Members() []interface{} {
	store := s.GetStore()
	return store.All()
}
