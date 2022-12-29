package polarisdb

import (
	"bytes"
	"encoding/gob"
	"errors"
	"io"
	"time"
)

type ActionType int

// enum actions
const (
	SetStringAction ActionType = iota
	SetExpireAction
	GetDelAction
	GetExAction
	HSetAction
	HDelAction
	LPushAction
	LPopAction
	LInsertAction
	SAddAction
	SRemAction
	DelAction
	ZAddAction
	ZRemAction
)

type ActionBlock struct {
	Type ActionType
	Data []byte
}

func (b *ActionBlock) Serialize() ([]byte, error) {
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(b)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (b *ActionBlock) Deserialize(reader io.Reader) error {
	return gob.NewDecoder(reader).Decode(b)
}

func SerializeAction(action interface{}) ([]byte, error) {
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(action)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
func GenerateActionBlock(action interface{}, actionType ActionType) (*ActionBlock, error) {
	data, err := SerializeAction(action)
	if err != nil {
		return nil, err
	}
	return &ActionBlock{
		Type: actionType,
		Data: data,
	}, nil
}

type ExpireAct struct {
	Key string
	TTL int64
}

func (a *ExpireAct) Write(db *PolarisDB) (err error) {
	// check if key exists
	_, isExist := db.Dict.Find(a.Key)
	if !isExist {
		return errors.New("key not exist")
	}
	if a.TTL == -1 {
		return SetExpire(db, a.Key, -1)
	}
	a.TTL = time.Now().Unix() + a.TTL
	return SetExpire(db, a.Key, a.TTL)
}

func (a *ExpireAct) Deserialize(reader io.Reader) error {
	return gob.NewDecoder(reader).Decode(a)
}
func (a *ExpireAct) GetActionBlock() (*ActionBlock, error) {
	return GenerateActionBlock(a, SetExpireAction)
}

type HashHSetAction struct {
	Key   []byte
	Paris []Paris
}

func (a *HashHSetAction) Write(db *PolarisDB) (err error) {
	return SetHashField(db, string(a.Key), a.Paris...)
}

func (a *HashHSetAction) Deserialize(reader io.Reader) error {
	return gob.NewDecoder(reader).Decode(a)
}

func (a *HashHSetAction) GetActionBlock() (*ActionBlock, error) {
	return GenerateActionBlock(a, HSetAction)
}

type HashHDelAction struct {
	Key    []byte
	Fields [][]byte
}

func (a *HashHDelAction) Write(db *PolarisDB) (err error) {
	strs := make([]string, len(a.Fields))
	for i, field := range a.Fields {
		strs[i] = string(field)
	}
	return HashDeleteFields(db, string(a.Key), strs...)
}

func (a *HashHDelAction) GetActionBlock() (*ActionBlock, error) {
	return GenerateActionBlock(a, HDelAction)
}
func (a *HashHDelAction) Deserialize(reader io.Reader) error {
	return gob.NewDecoder(reader).Decode(a)
}

type ListLPushAction struct {
	Key  []byte
	Data [][]byte
}

func (a *ListLPushAction) Write(db *PolarisDB) (err error) {
	_, err = ListPush(db, string(a.Key), a.Data...)
	if err != nil {
		return err
	}
	return nil
}

func (a *ListLPushAction) GetActionBlock() (*ActionBlock, error) {
	return GenerateActionBlock(a, LPushAction)
}

func (a *ListLPushAction) Deserialize(reader io.Reader) error {
	return gob.NewDecoder(reader).Decode(a)
}

type ListLPopAction struct {
	Key   []byte
	Count int
}

func (a *ListLPopAction) Write(db *PolarisDB) (err error) {
	_, err = ListPop(db, string(a.Key), a.Count)
	return err
}

func (a *ListLPopAction) GetActionBlock() (*ActionBlock, error) {
	return GenerateActionBlock(a, LPopAction)
}

func (a *ListLPopAction) Deserialize(reader io.Reader) error {
	return gob.NewDecoder(reader).Decode(a)
}

type ListInsertAction struct {
	Key   []byte
	Data  []byte
	Index int
}

func (a *ListInsertAction) Write(db *PolarisDB) (err error) {
	return ListInsert(db, string(a.Key), a.Index, a.Data)
}

func (a *ListInsertAction) GetActionBlock() (*ActionBlock, error) {
	return GenerateActionBlock(a, LInsertAction)
}

func (a *ListInsertAction) Deserialize(reader io.Reader) error {
	return gob.NewDecoder(reader).Decode(a)
}

type SetAddAction struct {
	Key   string
	Value []interface{}
}

func (a *SetAddAction) Write(db *PolarisDB) (err error) {
	_, err = SetAdd(db, a.Key, a.Value...)
	if err != nil {
		return err
	}
	return nil
}

func (a *SetAddAction) GetActionBlock() (*ActionBlock, error) {
	return GenerateActionBlock(a, SAddAction)
}

func (a *SetAddAction) Deserialize(reader io.Reader) error {
	return gob.NewDecoder(reader).Decode(a)
}

type SetRemAction struct {
	Key   string
	Value []interface{}
}

func (a *SetRemAction) Write(db *PolarisDB) (err error) {
	_, err = SetRemove(db, a.Key, a.Value...)
	if err != nil {
		return err
	}
	return nil
}

func (a *SetRemAction) GetActionBlock() (*ActionBlock, error) {
	return GenerateActionBlock(a, SRemAction)
}

func (a *SetRemAction) Deserialize(reader io.Reader) error {
	return gob.NewDecoder(reader).Decode(a)
}

type SetInterStoreAction struct {
	DestKey   string
	TargetKey string
	OtherKeys []string
}
type StringAct struct {
	Data    string
	Key     string
	KeepTTL bool
}

func (a *StringAct) Write(db *PolarisDB) (err error) {
	WriteStringToStore(db, []byte(a.Key), []byte(a.Data), a.KeepTTL)
	return nil
}

func (a *StringAct) GetActionBlock() (*ActionBlock, error) {
	return GenerateActionBlock(a, SetStringAction)
}
func (a *StringAct) Deserialize(reader io.Reader) error {
	return gob.NewDecoder(reader).Decode(a)
}

type StringDelAction struct {
	Key string
}

func (a *StringDelAction) Write(db *PolarisDB) (err error) {
	err = db.StringStore.delete([]byte(a.Key))
	if err != nil {
		return err
	}
	return nil
}

func (a *StringDelAction) GetActionBlock() (*ActionBlock, error) {
	return GenerateActionBlock(a, GetDelAction)
}

func (a *StringDelAction) Deserialize(reader io.Reader) error {
	return gob.NewDecoder(reader).Decode(a)
}

type SetExAction struct {
	Key string
	TTL int64
}

func (a *SetExAction) Write(db *PolarisDB) (err error) {
	db.Sweeper.SetKeyExpire(a.Key, a.TTL)
	return nil
}

func (a *SetExAction) GetActionBlock() (*ActionBlock, error) {
	return GenerateActionBlock(a, GetExAction)
}

func (a *SetExAction) Deserialize(reader io.Reader) error {
	return gob.NewDecoder(reader).Decode(a)
}

type ZsetAddAction struct {
	Key   string
	Pairs []ZsetPair
}

func (a *ZsetAddAction) Write(db *PolarisDB) (err error) {
	_, err = ZsetAdd(db, a.Key, a.Pairs...)
	return err
}

func (a *ZsetAddAction) GetActionBlock() (*ActionBlock, error) {
	return GenerateActionBlock(a, ZAddAction)
}

func (a *ZsetAddAction) Deserialize(reader io.Reader) error {
	return gob.NewDecoder(reader).Decode(a)
}

type ZsetRemAction struct {
	Key     string
	Members []string
}

func (a *ZsetRemAction) Write(db *PolarisDB) (err error) {
	_, err = ZsetRemove(db, a.Key, a.Members...)
	return err
}

func (a *ZsetRemAction) GetActionBlock() (*ActionBlock, error) {
	return GenerateActionBlock(a, ZRemAction)
}

func (a *ZsetRemAction) Deserialize(reader io.Reader) error {
	return gob.NewDecoder(reader).Decode(a)
}
