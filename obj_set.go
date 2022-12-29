package polarisdb

import (
	"errors"
	"github.com/projectxpolaris/polarisdb/set"
)

type SetObject struct {
	Data *set.Set
}

func NewSetObject() *SetObject {
	return &SetObject{
		Data: set.NewSet(),
	}
}

func SetAdd(db *PolarisDB, key string, members ...interface{}) (*KeyEntity, error) {
	ent, isExist := db.Dict.Find(key)
	if !isExist {
		ent = &KeyEntity{
			Ptr: NewSetObject(),
		}
		db.Dict.Add(key, ent)
	}
	setObj := ent.Ptr.(*SetObject)
	for _, member := range members {
		setObj.Data.Add(member)
	}
	return ent, nil
}

func SetRemove(db *PolarisDB, key string, members ...interface{}) (*KeyEntity, error) {
	ent, isExist := db.Dict.Find(key)
	if !isExist {
		return nil, errors.New("key not exist")
	}
	setObj := ent.Ptr.(*SetObject)
	for _, member := range members {
		err := setObj.Data.Remove(member)
		if err != nil {
			return nil, err
		}
	}
	return ent, nil
}

func SetIsMember(db *PolarisDB, key string, member interface{}) (bool, error) {
	ent, isExist := db.Dict.Find(key)
	if !isExist {
		return false, errors.New("key not exist")
	}
	setObj := ent.Ptr.(*SetObject)
	return setObj.Data.Contains(member)
}

func SetSize(db *PolarisDB, key string) (int, error) {
	ent, isExist := db.Dict.Find(key)
	if !isExist {
		return 0, errors.New("key not exist")
	}
	setObj := ent.Ptr.(*SetObject)
	return setObj.Data.Len(), nil
}
func SetDiff(db *PolarisDB, key string, keys ...string) ([]interface{}, error) {
	ent, isExist := db.Dict.Find(key)
	if !isExist {
		return nil, errors.New("key not exist")
	}
	setObj := ent.Ptr.(*SetObject)
	otherSets := make([]*set.Set, 0)
	for _, key := range keys {
		ent, isExist := db.Dict.Find(key)
		if !isExist {
			return nil, errors.New("key not exist")
		}
		setObj := ent.Ptr.(*SetObject)
		otherSets = append(otherSets, setObj.Data)
	}
	return set.Diff(setObj.Data, otherSets...), nil
}

func SetInter(db *PolarisDB, keys ...string) ([]interface{}, error) {
	sets := make([]*set.Set, 0)
	for _, key := range keys {
		ent, isExist := db.Dict.Find(key)
		if !isExist {
			return nil, errors.New("key not exist")
		}
		setObj := ent.Ptr.(*SetObject)
		sets = append(sets, setObj.Data)
	}
	return set.Intersection(sets...), nil
}

// SetUnion returns the members of the set resulting from the union of all the given sets.
func SetUnion(db *PolarisDB, keys ...string) ([]interface{}, error) {
	sets := make([]*set.Set, 0)
	for _, key := range keys {
		ent, isExist := db.Dict.Find(key)
		if !isExist {
			return nil, errors.New("key not exist")
		}
		setObj := ent.Ptr.(*SetObject)
		sets = append(sets, setObj.Data)
	}
	return set.Union(sets...), nil
}

// SetMembers returns all members of the set value stored at key.
func SetMembers(db *PolarisDB, key string) ([]interface{}, error) {
	ent, isExist := db.Dict.Find(key)
	if !isExist {
		return nil, errors.New("key not exist")
	}
	setObj := ent.Ptr.(*SetObject)
	return setObj.Data.Members(), nil
}

// SetPop removes and returns one or more random elements from the set value stored at key.
func SetPop(db *PolarisDB, key string, count int) ([]interface{}, error) {
	ent, isExist := db.Dict.Find(key)
	if !isExist {
		return nil, errors.New("key not exist")
	}
	setObj := ent.Ptr.(*SetObject)
	return setObj.Data.Pop(count)
}

// SetRandomMember returns one or more random elements from the set value stored at key.
func SetRandomMember(db *PolarisDB, key string, count int) ([]interface{}, error) {
	ent, isExist := db.Dict.Find(key)
	if !isExist {
		return nil, errors.New("key not exist")
	}
	setObj := ent.Ptr.(*SetObject)
	return setObj.Data.RandomMembers(count), nil
}
