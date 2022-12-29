package polarisdb

import (
	"bytes"
	"errors"
	"sync"
)

type PolarisDB struct {
	sync.RWMutex
	Log         *Log
	Dict        *KeyDict
	Sweeper     *Sweeper
	StringStore *StringStore
	Config      *DBConfig
	Clock       *LRUClock
	httpServer  *HttpServer
}

type DBConfig struct {
	Host               string  `json:"host"`
	Port               string  `json:"port"`
	LruClockResolution float64 `json:"lru_clock_resolution"`
	Path               string  `json:"aof_path"`
	SweeperInterval    int64   `json:"sweeper_interval"`
	RandomRemoveFactor float64 `json:"random_remove_factor"`
	LruSampleFactor    float64 `json:"lru_sample_factor"`
	EvicterInterval    int64   `json:"evicter_interval"`
	EvicterPolicy      string  `json:"evicter_policy"`
}

func NewDB(config *DBConfig) *PolarisDB {
	if config.Host == "" {
		config.Host = "localhost"
	}
	if config.Port == "" {
		config.Port = "8222"
	}
	if config.LruClockResolution == 0 {
		config.LruClockResolution = 0.01
	}
	if config.Path == "" {
		config.Path = "./data"
	}
	if config.SweeperInterval == 0 {
		config.SweeperInterval = 1000
	}
	if config.RandomRemoveFactor == 0 {
		config.RandomRemoveFactor = 0.001
	}
	if config.LruSampleFactor == 0 {
		config.LruSampleFactor = 0.001
	}
	if config.EvicterInterval == 0 {
		config.EvicterInterval = 1000
	}
	if config.EvicterPolicy == "" {
		config.EvicterPolicy = EvictNoEviction
	}
	return &PolarisDB{
		Config: config,
		Dict:   NewKeyDict(),
	}
}
func (db *PolarisDB) RunServer() {
	db.httpServer = NewHttpServer(db)
	addr := db.Config.Host + ":" + db.Config.Port
	go db.httpServer.run(addr)
}
func (db *PolarisDB) Open() error {
	db.Dict.db = db
	if db.Config == nil {
		return errors.New("no config")
	}
	db.Sweeper = NewSweeper(db)
	// init store
	stringStore := NewStore()
	db.StringStore = stringStore
	// init clock
	db.Clock = &LRUClock{db: db}
	db.Log = &Log{}
	err := db.Log.Open(db.Config.Path)
	if err != nil {
		return err
	}
	iter, err := db.Log.NewLogIterator()
	if err != nil {
		return err
	}
	for {
		block := iter.Next()
		if block == nil {
			break
		}
		actionBlock := ActionBlock{}
		err = actionBlock.Deserialize(bytes.NewBuffer(block.Data))
		if err != nil {
			return err
		}
		switch actionBlock.Type {
		case SetStringAction:
			stringAct := StringAct{}
			err = stringAct.Deserialize(bytes.NewBuffer(actionBlock.Data))
			err = stringAct.Write(db)
			if err != nil {
				return err
			}
		case SetExpireAction:
			expireAct := ExpireAct{}
			err = expireAct.Deserialize(bytes.NewBuffer(actionBlock.Data))
			err = expireAct.Write(db)
		case GetDelAction:
			getDelAct := StringDelAction{}
			err = getDelAct.Deserialize(bytes.NewBuffer(actionBlock.Data))
			err = getDelAct.Write(db)
		case GetExAction:
			getExAct := SetExAction{}
			err = getExAct.Deserialize(bytes.NewBuffer(actionBlock.Data))
			err = getExAct.Write(db)
		case HSetAction:
			hsetAct := HashHSetAction{}
			err = hsetAct.Deserialize(bytes.NewBuffer(actionBlock.Data))
			err = hsetAct.Write(db)
		case HDelAction:
			hdelAct := HashHDelAction{}
			err = hdelAct.Deserialize(bytes.NewBuffer(actionBlock.Data))
			err = hdelAct.Write(db)
		case LPushAction:
			lpushAct := ListLPushAction{}
			err = lpushAct.Deserialize(bytes.NewBuffer(actionBlock.Data))
			err = lpushAct.Write(db)
		case LPopAction:
			lpopAct := ListLPopAction{}
			err = lpopAct.Deserialize(bytes.NewBuffer(actionBlock.Data))
			err = lpopAct.Write(db)
		case LInsertAction:
			linsertAct := ListInsertAction{}
			err = linsertAct.Deserialize(bytes.NewBuffer(actionBlock.Data))
			err = linsertAct.Write(db)
		case SAddAction:
			saddAct := SetAddAction{}
			err = saddAct.Deserialize(bytes.NewBuffer(actionBlock.Data))
			err = saddAct.Write(db)
		case SRemAction:
			sremAct := SetRemAction{}
			err = sremAct.Deserialize(bytes.NewBuffer(actionBlock.Data))
			err = sremAct.Write(db)
		case ZAddAction:
			zaddAct := ZsetAddAction{}
			err = zaddAct.Deserialize(bytes.NewBuffer(actionBlock.Data))
			err = zaddAct.Write(db)
		case ZRemAction:
			zremAct := ZsetRemAction{}
			err = zremAct.Deserialize(bytes.NewBuffer(actionBlock.Data))
			err = zremAct.Write(db)
		}
	}
	//go db.Sweeper.run(context.Background())
	return nil
}

func (db *PolarisDB) Update(trf func(tx *TX) error) error {
	db.Lock()
	defer db.Unlock()
	tx := &TX{Writers: []DataWriter{}, db: db}
	err := trf(tx)
	if err != nil {
		return err
	}
	blocks := make([]*ActionBlock, 0)
	for _, dataWriter := range tx.Writers {
		err = dataWriter.Write(db)
		if err != nil {
			return err
		}
		block, err := dataWriter.GetActionBlock()
		if err != nil {
			return err
		}
		blocks = append(blocks, block)
	}
	for _, block := range blocks {
		data, err := block.Serialize()
		if err != nil {
			return err
		}
		err = db.Log.Append(&Block{data})
	}
	return nil
}

func (db *PolarisDB) View(trf func(tx *TX) error) error {
	db.RLock()
	defer db.RUnlock()
	tx := &TX{Writers: []DataWriter{}, db: db}
	err := trf(tx)
	if err != nil {
		return err
	}
	if len(tx.Writers) != 0 {
		return errors.New("view transaction is read only")
	}
	return nil
}
