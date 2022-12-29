package polarisdb

import (
	"errors"
	"fmt"
	"github.com/projectxpolaris/polarisdb/utils"
)

type TXData struct {
	data []byte
	key  []byte
}
type DataWriter interface {
	Write(db *PolarisDB) (err error)
	GetActionBlock() (*ActionBlock, error)
}

type TX struct {
	Writers []DataWriter
	db      *PolarisDB
}

func (t *TX) Exists(key string) (bool, error) {
	_, isExist := t.db.Dict.FindRaw(key)
	if isExist && !t.db.Sweeper.isExpire(key) {
		return true, nil
	}
	return false, nil
}

func (t *TX) SetString(key string, value string, keepTTL bool) error {
	WriteStringToStore(t.db, []byte(key), []byte(value), keepTTL)
	t.Writers = append(t.Writers, &StringAct{Data: value, Key: key, KeepTTL: keepTTL})
	return nil
}
func (t *TX) SetExpire(key string, duration int64) error {
	t.db.Sweeper.SetKeyExpire(key, utils.GetAbsExpireTime(duration))
	t.Writers = append(t.Writers, &ExpireAct{Key: key, TTL: utils.GetAbsExpireTime(duration)})
	return nil
}
func (t *TX) Append(key string, value string) error {
	newData, err := AppendStringToStore(t.db, []byte(key), []byte(value))
	if err != nil {
		return err
	}
	t.Writers = append(t.Writers, &StringAct{Key: key, Data: string(newData)})
	return nil
}
func (t *TX) Get(key string) (string, error) {
	obj, exist := t.db.Dict.Find(key)
	if !exist {
		return "", errors.New("key not exist")
	}
	fmt.Sprintf("obj type is %f", obj.LRU)
	val, err := obj.Ptr.(*StringStore).read([]byte(key))
	if err != nil {
		return "", err
	}
	return string(val), nil
}

func (t *TX) Decr(key string) error {
	newVal, err := StringCalculate(t.db, []byte(key), -1)
	if err != nil {
		return err
	}
	t.Writers = append(t.Writers, &StringAct{Key: key, Data: newVal})
	return nil
}

func (t *TX) DecrBy(key string, byValue int64) error {
	newVal, err := StringCalculate(t.db, []byte(key), -byValue)
	if err != nil {
		return err
	}
	t.Writers = append(t.Writers, &StringAct{Key: key, Data: newVal})
	return nil
}

func (t *TX) Incr(key string) error {
	newVal, err := StringCalculate(t.db, []byte(key), 1)
	if err != nil {
		return err
	}
	t.Writers = append(t.Writers, &StringAct{Key: key, Data: newVal})
	return nil
}

func (t *TX) IncrBy(key string, byValue int64) error {
	newVal, err := StringCalculate(t.db, []byte(key), byValue)
	if err != nil {
		return err
	}
	t.Writers = append(t.Writers, &StringAct{Key: key, Data: newVal})
	return nil
}

func (t *TX) GetDel(key string) (string, error) {
	value, err := StringGetDel(t.db, []byte(key))
	if err != nil {
		return "", err
	}
	t.Writers = append(t.Writers, &StringDelAction{Key: key})
	return value, nil
}

func (t *TX) GetEx(key string, ex int64) (string, error) {
	obj, exist := t.db.Dict.Find(key)
	if !exist {
		return "", errors.New("key not exist")
	}
	value, err := obj.Ptr.(*StringStore).read([]byte(key))
	if err != nil {
		return "", err
	}
	t.db.Sweeper.SetKeyExpire(key, utils.GetAbsExpireTime(ex))
	t.Writers = append(t.Writers, &SetExAction{Key: key, TTL: utils.GetAbsExpireTime(ex)})
	return string(value), nil
}

func (t *TX) GetRange(key string, start int64, end int64) (string, error) {
	obj, exist := t.db.Dict.Find(key)
	if !exist {
		return "", errors.New("key not exist")
	}
	value, err := obj.Ptr.(*StringStore).read([]byte(key))
	if err != nil {
		return "", err
	}
	if err != nil {
		return "", err
	}
	if start < 0 {
		start = int64(len(value)) + start
	}
	if end < 0 {
		end = int64(len(value)) + end
	}
	if start > int64(len(value)) {
		start = int64(len(value))
	}
	if end > int64(len(value)) {
		end = int64(len(value))
	}
	if start > end {
		start = end
	}
	return string(value[start:end]), nil
}

func (t *TX) Lcs(key1 string, key2 string) (string, error) {
	obj, exist := t.db.Dict.Find(key1)
	if !exist {
		return "", errors.New("key not exist")
	}
	value1, err := obj.Ptr.(*StringStore).read([]byte(key1))
	if err != nil {
		return "", err
	}
	obj, exist = t.db.Dict.Find(key2)
	if !exist {
		return "", errors.New("key not exist")
	}
	value2, err := obj.Ptr.(*StringStore).read([]byte(key2))
	// longest common subsequence
	lcs := utils.LongestCommonSubstring(value1, value2)
	return string(lcs), nil
}

func (t *TX) MGet(keys ...string) ([]string, error) {
	var values []string
	for _, key := range keys {
		obj, exist := t.db.Dict.Find(key)
		if !exist {
			values = append(values, "")
			continue
		}
		value, err := obj.Ptr.(*StringStore).read([]byte(key))
		if err != nil {
			return nil, err
		}
		values = append(values, string(value))
	}
	return values, nil
}

func (t *TX) MSet(keyValues ...string) error {
	if len(keyValues)%2 != 0 {
		return errors.New("key and Value must be paired")
	}
	for i := 0; i < len(keyValues); i += 2 {
		t.Writers = append(t.Writers, &StringAct{Data: keyValues[i+1], Key: keyValues[i]})
	}
	return nil
}

func (t *TX) HSet(key string, paris ...Paris) error {
	err := SetHashField(t.db, key, paris...)
	if err != nil {
		return err
	}
	t.Writers = append(t.Writers, &HashHSetAction{Key: []byte(key), Paris: paris})
	return nil
}
func (t *TX) HGet(key string, field string) (string, error) {
	ent, isExist := t.db.Dict.Find(key)
	if !isExist {
		return "", errors.New("key not exist")
	}
	value, isFieldExist := ent.Ptr.(*HashObject).Get(field)
	if !isFieldExist {
		return "", errors.New("field not exist")
	}
	return utils.ToString(value), nil
}

func (t *TX) HGetAll(key string) (map[string]string, error) {
	ent, isExist := t.db.Dict.Find(key)
	if !isExist {
		return nil, errors.New("key not exist")
	}
	value := ent.Ptr.(*HashObject).GetAll()
	result := make(map[string]string)
	for k, v := range value {
		result[utils.ToString(k)] = utils.ToString(v)
	}
	return result, nil
}

func (t *TX) HExists(key string, field string) (bool, error) {
	ent, isExist := t.db.Dict.Find(key)
	if !isExist {
		return false, errors.New("key not exist")
	}
	_, isFieldExist := ent.Ptr.(*HashObject).Get(field)
	return isFieldExist, nil
}

func (t *TX) HDel(key string, fields ...string) error {
	rawFields := make([][]byte, 0)
	for _, field := range fields {
		rawFields = append(rawFields, []byte(field))
	}
	err := HashDeleteFields(t.db, key, fields...)
	if err != nil {
		return err
	}
	t.Writers = append(t.Writers, &HashHDelAction{Key: []byte(key), Fields: rawFields})
	return nil
}

func (t *TX) HIncrBy(key string, field string, value int64) error {
	newVal, err := HashFieldCalculation(t.db, key, field, value)
	if err != nil {
		return err
	}
	t.Writers = append(t.Writers, &HashHSetAction{Key: []byte(key), Paris: []Paris{{
		Field: []byte(field), Value: []byte(fmt.Sprintf("%d", newVal)),
	}}})
	return nil
}

func (t *TX) HKeys(key string) ([]string, error) {
	ent, isExist := t.db.Dict.Find(key)
	if !isExist {
		return nil, errors.New("key not exist")
	}
	fields := ent.Ptr.(*HashObject).Keys()
	result := make([]string, 0)
	for _, field := range fields {
		result = append(result, utils.ToString(field))
	}
	return result, nil
}

func (t *TX) HLen(key string) (int64, error) {
	ent, isExist := t.db.Dict.Find(key)
	if !isExist {
		return 0, errors.New("key not exist")
	}
	return int64(ent.Ptr.(*HashObject).Len()), nil
}

func (t *TX) HVals(key string) ([]string, error) {
	ent, isExist := t.db.Dict.Find(key)
	if !isExist {
		return nil, errors.New("key not exist")
	}
	values := ent.Ptr.(*HashObject).Values()
	result := make([]string, 0)
	for _, value := range values {
		result = append(result, utils.ToString(value))
	}
	return result, nil
}

func (t *TX) LPush(key string, value ...[]byte) error {
	_, err := ListPush(t.db, key, value...)
	if err != nil {
		return err
	}
	t.Writers = append(t.Writers, &ListLPushAction{Key: []byte(key), Data: value})
	return nil
}

func (t *TX) LPop(key string, count int) ([][]byte, error) {
	value, err := ListPop(t.db, key, count)
	if err != nil {
		return nil, err
	}
	t.Writers = append(t.Writers, &ListLPopAction{Key: []byte(key), Count: count})
	return value, nil
}

func (t *TX) LIndex(key string, index int) ([]byte, error) {
	return ListIndex(t.db, key, index)
}

func (t *TX) LLen(key string) (int, error) {
	return ListLen(t.db, key)
}

func (t *TX) LRange(key string, start int, end int) ([][]byte, error) {
	return ListRange(t.db, key, start, end)
}

func (t *TX) LInsert(key string, position int, value string) error {
	err := ListInsert(t.db, key, position, []byte(value))
	if err != nil {
		return err
	}
	t.Writers = append(t.Writers, &ListInsertAction{Key: []byte(key), Index: position, Data: []byte(value)})
	return nil
}

func (t *TX) SAdd(key string, members ...interface{}) error {
	_, err := SetAdd(t.db, key, members...)
	if err != nil {
		return err
	}
	t.Writers = append(t.Writers, &SetAddAction{Key: key, Value: members})
	return nil
}

func (t *TX) SRem(key string, members ...interface{}) error {
	_, err := SetRemove(t.db, key, members...)
	if err != nil {
		return err
	}
	t.Writers = append(t.Writers, &SetRemAction{Key: key, Value: members})
	return nil
}

func (t *TX) SIsMember(key string, member interface{}) (bool, error) {
	return SetIsMember(t.db, key, member)
}

func (t *TX) SCard(key string) (int, error) {
	return SetSize(t.db, key)
}
func (t *TX) SMIsMembers(key string, members ...interface{}) ([]bool, error) {
	result := make([]bool, 0)
	for _, member := range members {
		isMember, err := SetIsMember(t.db, key, member)
		if err != nil {
			return nil, err
		}
		result = append(result, isMember)
	}
	return result, nil
}

func (t *TX) SDiff(key string, others ...string) ([]interface{}, error) {
	return SetDiff(t.db, key, others...)
}

func (t *TX) SInter(keys ...string) ([]interface{}, error) {
	return SetInter(t.db, keys...)
}

func (t *TX) SUnion(keys ...string) ([]interface{}, error) {
	return SetUnion(t.db, keys...)
}

func (t *TX) SMembers(key string) ([]interface{}, error) {
	return SetMembers(t.db, key)
}

func (t *TX) SPop(key string, count int) ([]interface{}, error) {
	vals, err := SetPop(t.db, key, count)
	if err != nil {
		return nil, err
	}
	t.Writers = append(t.Writers, &SetRemAction{Key: key, Value: vals})
	return vals, nil
}

func (t *TX) SRandMember(key string, count int) ([]interface{}, error) {
	return SetRandomMember(t.db, key, count)
}

func (t *TX) ZAdd(key string, pairs ...ZsetPair) error {
	_, err := ZsetAdd(t.db, key, pairs...)
	if err != nil {
		return err
	}
	t.Writers = append(t.Writers, &ZsetAddAction{Key: key, Pairs: pairs})
	return nil
}

func (t *TX) ZRem(key string, members ...string) error {
	_, err := ZsetRemove(t.db, key, members...)
	if err != nil {
		return err
	}
	t.Writers = append(t.Writers, &ZsetRemAction{Key: key, Members: members})
	return nil
}

func (t *TX) ZCard(key string) (int, error) {
	return ZsetCard(t.db, key)
}

func (t *TX) ZRange(key string, start int, end int) ([]interface{}, error) {
	return ZsetRange(t.db, key, start, end)
}

func (t *TX) ZRangeWithScores(key string, start int, end int) ([]interface{}, error) {
	return ZsetRangeWithScores(t.db, key, start, end)
}
func (t *TX) ZDiff(key string, others ...string) ([]interface{}, error) {
	return ZdiffWithResult(t.db, append([]string{key}, others...)...)
}

func (t *TX) ZDiffStore(saveKey string, targetKey string, others ...string) (int, error) {
	result, err := Zdiff(t.db, append([]string{targetKey}, others...)...)
	if err != nil {
		return 0, err
	}
	obj := NewZsetObject()
	obj.Data = result
	t.db.Dict.Add(saveKey, &KeyEntity{Ptr: obj})
	// add to aof
	resultVals := result.ZRangeWithScores(0, -1)
	pairs := valsToPairs(resultVals)
	t.Writers = append(t.Writers, &ZsetAddAction{Key: saveKey, Pairs: pairs})
	return len(pairs), nil
}

func (t *TX) ZDiffCard(key string, others ...string) (int, error) {
	result, err := Zdiff(t.db, append([]string{key}, others...)...)
	if err != nil {
		return 0, err
	}
	return result.ZCard(), nil
}

func (t *TX) ZInter(keys ...string) ([]interface{}, error) {
	return ZInterWithResult(t.db, keys...)
}

func (t *TX) ZInterStore(saveKey string, keys ...string) (int, error) {
	result, err := ZInter(t.db, keys...)
	if err != nil {
		return 0, err
	}
	obj := NewZsetObject()
	obj.Data = result
	t.db.Dict.Add(saveKey, &KeyEntity{Ptr: obj})
	// add to aof
	resultVals := result.ZRangeWithScores(0, -1)
	pairs := valsToPairs(resultVals)
	t.Writers = append(t.Writers, &ZsetAddAction{Key: saveKey, Pairs: pairs})
	return len(pairs), nil
}
func (t *TX) ZInterCard(keys ...string) (int, error) {
	result, err := ZInter(t.db, keys...)
	if err != nil {
		return 0, err
	}
	return result.ZCard(), nil
}

func (t *TX) ZUnion(keys ...string) ([]interface{}, error) {
	return ZUnionWithResult(t.db, keys...)
}

func (t *TX) ZUnionStore(saveKey string, keys ...string) (int, error) {
	result, err := ZUnion(t.db, keys...)
	if err != nil {
		return 0, err
	}
	obj := NewZsetObject()
	obj.Data = result
	t.db.Dict.Add(saveKey, &KeyEntity{Ptr: obj})
	// add to aof
	resultVals := result.ZRangeWithScores(0, -1)
	pairs := valsToPairs(resultVals)
	t.Writers = append(t.Writers, &ZsetAddAction{Key: saveKey, Pairs: pairs})
	return len(pairs), nil
}

func (t *TX) ZUnionCard(keys ...string) (int, error) {
	result, err := ZUnion(t.db, keys...)
	if err != nil {
		return 0, err
	}
	return result.ZCard(), nil
}
func (t *TX) ZIncrBy(key string, increment float64, member string) (float64, error) {
	result, err := ZIncrBy(t.db, key, increment, member)
	if err != nil {
		return 0, err
	}
	t.Writers = append(t.Writers, &ZsetAddAction{Key: key, Pairs: []ZsetPair{{Member: member, Score: result}}})
	return result, nil
}
func (t *TX) ZScore(key string, member string) (float64, error) {
	return ZScore(t.db, key, member)
}
func (t *TX) ZRank(key string, member string) (int64, error) {
	return ZRank(t.db, key, member)
}
func valsToPairs(vals []interface{}) []ZsetPair {
	pairs := make([]ZsetPair, 0)
	for i := 0; i < len(vals); i += 2 {
		pairs = append(pairs, ZsetPair{Member: vals[i].(string), Score: vals[i+1].(float64)})
	}
	return pairs
}
