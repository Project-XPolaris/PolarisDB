package polarisdb

import (
	"errors"
	"github.com/projectxpolaris/polarisdb/list"
)

type ListObject struct {
	Data *list.QuickList
}

func NewListObject() *ListObject {
	return &ListObject{
		Data: list.NewQuickList(),
	}
}

func ListPush(db *PolarisDB, key string, data ...[]byte) (*KeyEntity, error) {
	ent, isExist := db.Dict.Find(key)
	if !isExist {
		ent = &KeyEntity{
			Ptr: NewListObject(),
		}
		db.Dict.Add(key, ent)
	}
	listObj := ent.Ptr.(*ListObject)
	for _, d := range data {
		listObj.Data.InsertAt(listObj.Data.Len(), d)
	}
	return ent, nil
}

func ListPop(db *PolarisDB, key string, count int) ([][]byte, error) {
	ent, isExist := db.Dict.Find(key)
	if !isExist {
		return nil, errors.New("key not exist")
	}
	listObj := ent.Ptr.(*ListObject)
	if listObj.Data.Len() == 0 {
		return nil, errors.New("list is empty")
	}
	out := make([][]byte, 0, count)
	for i := 0; i < count; i++ {
		out = append(out, listObj.Data.Index(listObj.Data.Len()-1))
		listObj.Data.DeleteAt(listObj.Data.Len() - 1)
	}
	return out, nil
}

func ListIndex(db *PolarisDB, key string, index int) ([]byte, error) {
	ent, isExist := db.Dict.Find(key)
	if !isExist {
		return nil, errors.New("key not exist")
	}
	listObj := ent.Ptr.(*ListObject)
	if listObj.Data.Len() == 0 {
		return nil, errors.New("list is empty")
	}
	return listObj.Data.Index(index), nil
}
func ListLen(db *PolarisDB, key string) (int, error) {
	ent, isExist := db.Dict.Find(key)
	if !isExist {
		return 0, errors.New("key not exist")
	}
	listObj := ent.Ptr.(*ListObject)
	return listObj.Data.Len(), nil
}

func ListRange(db *PolarisDB, key string, start, stop int) ([][]byte, error) {
	ent, isExist := db.Dict.Find(key)
	if !isExist {
		return nil, errors.New("key not exist")
	}
	listObj := ent.Ptr.(*ListObject)
	if listObj.Data.Len() == 0 {
		return nil, errors.New("list is empty")
	}
	return listObj.Data.Range(start, stop), nil
}
func ListInsert(db *PolarisDB, key string, index int, data []byte) error {
	ent, isExist := db.Dict.Find(key)
	if !isExist {
		// create new
		ent = &KeyEntity{
			Ptr: NewListObject(),
		}
		db.Dict.Add(key, ent)
	}
	listObj := ent.Ptr.(*ListObject)
	if listObj.Data.Len() < index || index < 0 {
		return errors.New("index out of range")
	}
	listObj.Data.InsertAt(index, data)

	return nil
}
