package polarisdb

import (
	"errors"
	"github.com/projectxpolaris/polarisdb/skiplist"
)

type ZsetObject struct {
	Data *skiplist.Zset
}
type ZsetPair struct {
	Member string  `json:"member"`
	Score  float64 `json:"score"`
}

func NewZsetObject() *ZsetObject {
	return &ZsetObject{
		Data: skiplist.NewZset(),
	}
}

func ZsetAdd(db *PolarisDB, key string, pairs ...ZsetPair) (*KeyEntity, error) {
	ent, isExist := db.Dict.Find(key)
	if !isExist {
		ent = &KeyEntity{
			Ptr: NewZsetObject(),
		}
		db.Dict.Add(key, ent)
	}
	zsetObj := ent.Ptr.(*ZsetObject)

	for _, pair := range pairs {
		zsetObj.Data.Add(pair.Score, pair.Member, nil)
	}
	return ent, nil
}

func ZsetRemove(db *PolarisDB, key string, members ...string) (*KeyEntity, error) {
	ent, isExist := db.Dict.Find(key)
	if !isExist {
		return nil, errors.New("key not exist")
	}
	zsetObj := ent.Ptr.(*ZsetObject)
	for _, member := range members {
		zsetObj.Data.ZRem(member)
	}
	return ent, nil
}
func ZsetCard(db *PolarisDB, key string) (int, error) {
	ent, isExist := db.Dict.Find(key)
	if !isExist {
		return 0, errors.New("key not exist")
	}
	zsetObj := ent.Ptr.(*ZsetObject)
	return zsetObj.Data.ZCard(), nil
}
func ZsetScore(db *PolarisDB, key string, member string) (float64, error) {
	ent, isExist := db.Dict.Find(key)
	if !isExist {
		return 0, errors.New("key not exist")
	}
	zsetObj := ent.Ptr.(*ZsetObject)
	_, score := zsetObj.Data.ZScore(member)
	return score, nil
}

func ZsetRank(db *PolarisDB, key string, member string) (int64, error) {
	ent, isExist := db.Dict.Find(key)
	if !isExist {
		return 0, errors.New("key not exist")
	}
	zsetObj := ent.Ptr.(*ZsetObject)
	return zsetObj.Data.ZRank(member), nil
}

func ZsetRange(db *PolarisDB, key string, start int, stop int) ([]interface{}, error) {
	ent, isExist := db.Dict.Find(key)
	if !isExist {
		return nil, errors.New("key not exist")
	}
	zsetObj := ent.Ptr.(*ZsetObject)
	vals := zsetObj.Data.ZRange(start, stop)
	return vals, nil
}
func ZsetRangeWithScores(db *PolarisDB, key string, start int, stop int) ([]interface{}, error) {
	ent, isExist := db.Dict.Find(key)
	if !isExist {
		return nil, errors.New("key not exist")
	}
	zsetObj := ent.Ptr.(*ZsetObject)
	vals := zsetObj.Data.ZRangeWithScores(start, stop)
	return vals, nil
}
func Zdiff(db *PolarisDB, keys ...string) (*skiplist.Zset, error) {
	sets := make([]*skiplist.Zset, 0)
	for _, key := range keys {
		ent, isExist := db.Dict.Find(key)
		if !isExist {
			return nil, errors.New("key not exist")
		}
		zsetObj := ent.Ptr.(*ZsetObject)
		sets = append(sets, zsetObj.Data)
	}
	resultZset := skiplist.ZsetDiff(sets[0], sets[1:]...)
	return resultZset, nil
}
func ZdiffWithResult(db *PolarisDB, keys ...string) ([]interface{}, error) {
	resultSet, err := Zdiff(db, keys...)
	if err != nil {
		return nil, err
	}
	return resultSet.ZRangeWithScores(0, -1), nil
}
func ZInter(db *PolarisDB, keys ...string) (*skiplist.Zset, error) {
	sets := make([]*skiplist.Zset, 0)
	for _, key := range keys {
		ent, isExist := db.Dict.Find(key)
		if !isExist {
			return nil, errors.New("key not exist")
		}
		zsetObj := ent.Ptr.(*ZsetObject)
		sets = append(sets, zsetObj.Data)
	}
	resultZset := skiplist.ZsetInter(sets...)
	return resultZset, nil
}

func ZInterWithResult(db *PolarisDB, keys ...string) ([]interface{}, error) {
	resultSet, err := ZInter(db, keys...)
	if err != nil {
		return nil, err
	}
	return resultSet.ZRangeWithScores(0, -1), nil
}
func ZUnion(db *PolarisDB, keys ...string) (*skiplist.Zset, error) {
	sets := make([]*skiplist.Zset, 0)
	for _, key := range keys {
		ent, isExist := db.Dict.Find(key)
		if !isExist {
			return nil, errors.New("key not exist")
		}
		zsetObj := ent.Ptr.(*ZsetObject)
		sets = append(sets, zsetObj.Data)
	}
	resultZset := skiplist.ZsetUnion(sets...)
	return resultZset, nil
}
func ZUnionWithResult(db *PolarisDB, keys ...string) ([]interface{}, error) {
	resultSet, err := ZUnion(db, keys...)
	if err != nil {
		return nil, err
	}
	return resultSet.ZRangeWithScores(0, -1), nil
}

func ZIncrBy(db *PolarisDB, key string, increment float64, member string) (float64, error) {
	ent, isExist := db.Dict.Find(key)
	if !isExist {
		ent = &KeyEntity{
			Ptr: NewZsetObject(),
		}
		db.Dict.Add(key, ent)
	}
	zsetObj := ent.Ptr.(*ZsetObject)
	return zsetObj.Data.ZIncrBy(increment, member), nil
}

func ZScore(db *PolarisDB, key string, member string) (float64, error) {
	ent, isExist := db.Dict.Find(key)
	if !isExist {
		return 0, errors.New("key not exist")
	}
	zsetObj := ent.Ptr.(*ZsetObject)
	_, score := zsetObj.Data.ZScore(member)
	return score, nil
}

func ZRank(db *PolarisDB, key string, member string) (int64, error) {
	ent, isExist := db.Dict.Find(key)
	if !isExist {
		return 0, errors.New("key not exist")
	}
	zsetObj := ent.Ptr.(*ZsetObject)
	return zsetObj.Data.ZRank(member), nil
}
