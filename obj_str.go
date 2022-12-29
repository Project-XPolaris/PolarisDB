package polarisdb

import (
	"errors"
	"fmt"
	"strconv"
)

func WriteStringToStore(db *PolarisDB, key []byte, value []byte, keepTTL bool) {
	obj, isExist := db.Dict.Find(string(key))
	if !isExist {
		obj = &KeyEntity{
			Ptr: db.StringStore,
		}
		db.Dict.Add(string(key), obj)
	}
	obj.Ptr.(*StringStore).write(key, value)
	if !keepTTL {
		db.Sweeper.TryRemoveExpire(string(key))
	}
}

func AppendStringToStore(db *PolarisDB, key []byte, value []byte) ([]byte, error) {
	obj, isExist := db.Dict.Find(string(key))
	if !isExist {
		obj = &KeyEntity{
			Ptr: db.StringStore,
		}
		db.Dict.Add(string(key), obj)
	}
	oldData, err := obj.Ptr.(*StringStore).read(key)
	if err != nil {
		return nil, err
	}
	newData := append(oldData, value...)
	obj.Ptr.(*StringStore).write(key, newData)
	return newData, nil
}

func StringCalculate(db *PolarisDB, key []byte, value int64) (string, error) {
	obj, isExist := db.Dict.Find(string(key))
	if !isExist {
		obj = &KeyEntity{
			Ptr: db.StringStore,
		}
		db.Dict.Add(string(key), obj)
	}
	oldData, err := obj.Ptr.(*StringStore).read(key)
	if err != nil {
		return "", err
	}
	// convert to int64
	oldValue, err := strconv.ParseInt(string(oldData), 10, 64)
	if err != nil {
		return "", err
	}
	newValue := oldValue + value
	strValue := fmt.Sprintf("%d", newValue)
	obj.Ptr.(*StringStore).write(key, []byte(strValue))
	return strValue, nil
}

func StringGetDel(db *PolarisDB, key []byte) (string, error) {
	obj, isExist := db.Dict.Find(string(key))
	if !isExist {
		return "", errors.New("key not exist")
	}
	val, err := obj.Ptr.(*StringStore).read(key)
	if err != nil {
		return "", err
	}
	err = obj.Ptr.(*StringStore).delete(key)
	if err != nil {
		return "", err
	}
	db.Dict.Delete(string(key))
	return string(val), nil
}
