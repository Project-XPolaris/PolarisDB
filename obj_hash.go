package polarisdb

import (
	"errors"
	"github.com/projectxpolaris/polarisdb/utils"
	"strconv"
)

type HashObject struct {
	Data map[string]interface{}
}
type Paris struct {
	Field []byte
	Value []byte
}

func NewHashObject() *HashObject {
	return &HashObject{
		Data: make(map[string]interface{}),
	}
}
func (h *HashObject) Set(field string, value interface{}) {
	h.Data[field] = value
}

func (h *HashObject) Get(field string) (interface{}, bool) {
	v, ok := h.Data[field]
	return v, ok
}
func (h *HashObject) GetAll() map[string]interface{} {
	return h.Data
}
func (h *HashObject) Delete(field string) {
	delete(h.Data, field)
}
func (h *HashObject) Keys() []string {
	var keys []string
	for k := range h.Data {
		keys = append(keys, k)
	}
	return keys
}
func (h *HashObject) Values() []interface{} {
	var values []interface{}
	for _, v := range h.Data {
		values = append(values, v)
	}
	return values
}
func (h *HashObject) Len() int {
	return len(h.Data)
}

func SetHashField(db *PolarisDB, key string, paris ...Paris) error {
	ent, isExist := db.Dict.Find(key)
	if !isExist {
		ent = &KeyEntity{
			Ptr: NewHashObject(),
		}
		db.Dict.Add(key, ent)
	}
	hashObj := ent.Ptr.(*HashObject)
	for _, pair := range paris {
		hashObj.Set(string(pair.Field), pair.Value)
	}
	return nil
}
func HashFieldCalculation(db *PolarisDB, key string, field string, addValue int64) (uint64, error) {
	ent, isExist := db.Dict.Find(key)
	if !isExist {
		return 0, errors.New("key not exist")
	}
	hashObj := ent.Ptr.(*HashObject)
	v, isFieldExist := hashObj.Get(field)
	if !isFieldExist {
		return 0, errors.New("field not exist")
	}
	valStr := utils.ToString(v)
	val, err := strconv.ParseInt(valStr, 10, 64)
	if err != nil {
		return 0, err
	}
	newVal := val + addValue
	hashObj.Set(field, newVal)
	return uint64(newVal), nil
}

func HashDeleteFields(db *PolarisDB, key string, fields ...string) error {
	ent, isExist := db.Dict.Find(key)
	if !isExist {
		return errors.New("key not exist")
	}
	hashObj := ent.Ptr.(*HashObject)
	for _, field := range fields {
		hashObj.Delete(field)
	}
	return nil
}
