package polarisdb

import (
	"errors"
	"github.com/allentom/haruka"
	"github.com/projectxpolaris/polarisdb/utils"
	"time"
)

type HttpServer struct {
	Database *PolarisDB
	Api      *haruka.Engine
}

func NewHttpServer(Database *PolarisDB) *HttpServer {
	api := haruka.NewEngine()

	return &HttpServer{
		Database: Database,
		Api:      api,
	}
}

type StringRequestBody struct {
	Key     string   `json:"key"`
	Keys    []string `json:"keys"`
	Value   string   `json:"value"`
	NumVal  int64    `json:"numVal"`
	Expire  int64    `json:"expire"`
	Start   int64    `json:"start"`
	End     int64    `json:"end"`
	NX      bool     `json:"nx"`
	XX      bool     `json:"xx"`
	AT      int64    `json:"at"`
	KeepTTL bool     `json:"keepTTL"`
}
type HashRequestBody struct {
	Key   string            `json:"key"`
	Field string            `json:"field"`
	Pairs map[string]string `json:"pairs"`
	Num   int64             `json:"num"`
}
type ListRequestBody struct {
	Key    string   `json:"key"`
	Values []string `json:"values"`
	Value  string   `json:"value"`
	Count  int      `json:"count"`
	Index  int      `json:"index"`
	Pivot  string   `json:"pivot"`
	Start  int      `json:"start"`
	End    int      `json:"end"`
}

type SetRequestBody struct {
	Key    string   `json:"key"`
	Values []string `json:"values"`
	Value  string   `json:"value"`
	Count  int      `json:"count"`
}

type ZSetRequestBody struct {
	Key       string     `json:"key"`
	StoreKey  string     `json:"storeKey"`
	Others    []string   `json:"others"`
	Pairs     []ZsetPair `json:"pairs"`
	Members   []string   `json:"members"`
	Member    string     `json:"member"`
	Start     int        `json:"start"`
	Stop      int        `json:"stop"`
	WithScore bool       `json:"withScore"`
}
type RequestBody struct {
	Key    string `json:"key"`
	Expire int64  `json:"expire"`
}

func (server *HttpServer) InitHandler() {
	server.Api.Router.POST("/action/get", func(context *haruka.Context) {
		var err error
		var requestBody StringRequestBody
		if !ParseJSONOrErrorResponse(context, &requestBody) {
			return
		}
		var value string
		err = server.Database.View(func(tx *TX) error {
			value, err = tx.Get(requestBody.Key)
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			RaiseErrorResponse(err, context)
			return
		}
		MakeSuccessResponse(context, value)
	})
	server.Api.Router.POST("/action/set", func(context *haruka.Context) {
		var err error
		var requestBody StringRequestBody
		if !ParseJSONOrErrorResponse(context, &requestBody) {
			return
		}
		err = server.Database.Update(func(tx *TX) error {
			if requestBody.NX {
				isExist, err := tx.Exists(requestBody.Key)
				if err != nil {
					return err
				}
				if isExist {
					return errors.New("key already exist")
				}
			}
			if requestBody.XX {
				isExist, err := tx.Exists(requestBody.Key)
				if err != nil {
					return err
				}
				if !isExist {
					return errors.New("key not exist")
				}
			}
			err = tx.SetString(requestBody.Key, requestBody.Value, requestBody.KeepTTL)
			if err != nil {
				return err
			}
			var expire int64
			if requestBody.Expire > 0 {
				expire = requestBody.Expire
				if err != nil {
					return err
				}
			}
			if requestBody.AT > 0 {
				expire = requestBody.AT - time.Now().UnixMilli()
				if err != nil {
					return err
				}
			}
			if expire > 0 {
				err = tx.SetExpire(requestBody.Key, expire)
				if err != nil {
					return err
				}
			}

			return nil
		})
		if err != nil {
			RaiseErrorResponse(err, context)
			return
		}
		MakeSuccessResponse(context, nil)
	})
	server.Api.Router.POST("/action/expire", func(context *haruka.Context) {
		var err error
		var requestBody RequestBody
		if !ParseJSONOrErrorResponse(context, &requestBody) {
			return
		}
		err = server.Database.Update(func(tx *TX) error {
			err = tx.SetExpire(requestBody.Key, requestBody.Expire)
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			RaiseErrorResponse(err, context)
			return
		}
		MakeSuccessResponse(context, nil)
	})
	server.Api.Router.POST("/action/append", func(context *haruka.Context) {
		var err error
		var requestBody StringRequestBody
		if !ParseJSONOrErrorResponse(context, &requestBody) {
			return
		}
		err = server.Database.Update(func(tx *TX) error {

			err = tx.Append(requestBody.Key, requestBody.Value)
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			RaiseErrorResponse(err, context)
			return
		}
		MakeSuccessResponse(context, nil)
	})
	server.Api.Router.POST("/action/decr", func(context *haruka.Context) {
		var err error
		var requestBody StringRequestBody
		if !ParseJSONOrErrorResponse(context, &requestBody) {
			return
		}
		var value int64
		err = server.Database.Update(func(tx *TX) error {
			err = tx.Decr(requestBody.Key)
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			RaiseErrorResponse(err, context)
			return
		}
		MakeSuccessResponse(context, value)
	})
	server.Api.Router.POST("/action/decrby", func(context *haruka.Context) {
		var err error
		var requestBody StringRequestBody
		if !ParseJSONOrErrorResponse(context, &requestBody) {
			return
		}
		var value int64
		err = server.Database.Update(func(tx *TX) error {
			err = tx.DecrBy(requestBody.Key, requestBody.NumVal)
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			RaiseErrorResponse(err, context)
			return
		}
		MakeSuccessResponse(context, value)
	})
	server.Api.Router.POST("/action/incr", func(context *haruka.Context) {
		var err error
		var requestBody StringRequestBody
		if !ParseJSONOrErrorResponse(context, &requestBody) {
			return
		}
		var value int64
		err = server.Database.Update(func(tx *TX) error {
			err = tx.Incr(requestBody.Key)
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			RaiseErrorResponse(err, context)
			return
		}
		MakeSuccessResponse(context, value)
	})
	server.Api.Router.POST("/action/incrby", func(context *haruka.Context) {
		var err error
		var requestBody StringRequestBody
		if !ParseJSONOrErrorResponse(context, &requestBody) {
			return
		}
		var value int64
		err = server.Database.Update(func(tx *TX) error {
			err = tx.IncrBy(requestBody.Key, requestBody.NumVal)
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			RaiseErrorResponse(err, context)
			return
		}
		MakeSuccessResponse(context, value)
	})
	server.Api.Router.POST("/action/getdel", func(context *haruka.Context) {
		var err error
		var requestBody StringRequestBody
		if !ParseJSONOrErrorResponse(context, &requestBody) {
			return
		}
		var value string
		err = server.Database.Update(func(tx *TX) error {
			value, err = tx.GetDel(requestBody.Key)
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			RaiseErrorResponse(err, context)
			return
		}
		MakeSuccessResponse(context, value)
	})
	server.Api.Router.POST("/action/getex", func(context *haruka.Context) {
		var err error
		var requestBody StringRequestBody
		if !ParseJSONOrErrorResponse(context, &requestBody) {
			return
		}
		var value string
		err = server.Database.Update(func(tx *TX) error {
			value, err = tx.GetEx(requestBody.Key, requestBody.Expire)
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			RaiseErrorResponse(err, context)
			return
		}
		MakeSuccessResponse(context, value)
	})
	server.Api.Router.POST("/action/getrange", func(context *haruka.Context) {
		var err error
		var requestBody StringRequestBody
		if !ParseJSONOrErrorResponse(context, &requestBody) {
			return
		}
		var value string
		err = server.Database.Update(func(tx *TX) error {
			value, err = tx.GetRange(requestBody.Key, requestBody.Start, requestBody.End)
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			RaiseErrorResponse(err, context)
			return
		}
		MakeSuccessResponse(context, value)
	})
	server.Api.Router.POST("/action/mget", func(context *haruka.Context) {
		var err error
		var requestBody StringRequestBody
		if !ParseJSONOrErrorResponse(context, &requestBody) {
			return
		}
		var values []string
		err = server.Database.Update(func(tx *TX) error {
			values, err = tx.MGet(requestBody.Keys...)
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			RaiseErrorResponse(err, context)
			return
		}
		MakeSuccessResponse(context, values)
	})
	server.Api.Router.POST("/action/hget", func(context *haruka.Context) {
		var err error
		var requestBody HashRequestBody
		if !ParseJSONOrErrorResponse(context, &requestBody) {
			return
		}
		var value string
		err = server.Database.Update(func(tx *TX) error {
			value, err = tx.HGet(requestBody.Key, requestBody.Field)
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			RaiseErrorResponse(err, context)
			return
		}
		MakeSuccessResponse(context, value)
	})
	server.Api.Router.POST("/action/hset", func(context *haruka.Context) {
		var err error
		var requestBody HashRequestBody
		if !ParseJSONOrErrorResponse(context, &requestBody) {
			return
		}
		var value string
		err = server.Database.Update(func(tx *TX) error {
			pairs := make([]Paris, 0)
			for k, s := range requestBody.Pairs {
				pairs = append(pairs, Paris{Field: []byte(k), Value: []byte(s)})
			}
			err = tx.HSet(requestBody.Key, pairs...)
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			RaiseErrorResponse(err, context)
			return
		}
		MakeSuccessResponse(context, value)
	})
	server.Api.Router.POST("/action/hdel", func(context *haruka.Context) {
		var err error
		var requestBody HashRequestBody
		if !ParseJSONOrErrorResponse(context, &requestBody) {
			return
		}
		var value string
		err = server.Database.Update(func(tx *TX) error {
			err = tx.HDel(requestBody.Key, requestBody.Field)
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			RaiseErrorResponse(err, context)
			return
		}
		MakeSuccessResponse(context, value)
	})
	server.Api.Router.POST("/action/hgetall", func(context *haruka.Context) {
		var err error
		var requestBody HashRequestBody
		if !ParseJSONOrErrorResponse(context, &requestBody) {
			return
		}
		var value map[string]string
		err = server.Database.Update(func(tx *TX) error {
			value, err = tx.HGetAll(requestBody.Key)
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			RaiseErrorResponse(err, context)
			return
		}
		MakeSuccessResponse(context, value)
	})
	server.Api.Router.POST("/action/hkeys", func(context *haruka.Context) {
		var err error
		var requestBody HashRequestBody
		if !ParseJSONOrErrorResponse(context, &requestBody) {
			return
		}
		var value []string
		err = server.Database.Update(func(tx *TX) error {
			value, err = tx.HKeys(requestBody.Key)
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			RaiseErrorResponse(err, context)
			return
		}
		MakeSuccessResponse(context, value)
	})
	server.Api.Router.POST("/action/hvals", func(context *haruka.Context) {
		var err error
		var requestBody HashRequestBody
		if !ParseJSONOrErrorResponse(context, &requestBody) {
			return
		}
		var value []string
		err = server.Database.Update(func(tx *TX) error {
			value, err = tx.HVals(requestBody.Key)
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			RaiseErrorResponse(err, context)
			return
		}
		MakeSuccessResponse(context, value)
	})
	server.Api.Router.POST("/action/hexists", func(context *haruka.Context) {
		var err error
		var requestBody HashRequestBody
		if !ParseJSONOrErrorResponse(context, &requestBody) {
			return
		}
		var value bool
		err = server.Database.Update(func(tx *TX) error {
			value, err = tx.HExists(requestBody.Key, requestBody.Field)
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			RaiseErrorResponse(err, context)
			return
		}
		MakeSuccessResponse(context, value)
	})
	server.Api.Router.POST("/action/hlen", func(context *haruka.Context) {
		var err error
		var requestBody HashRequestBody
		if !ParseJSONOrErrorResponse(context, &requestBody) {
			return
		}
		var value int64
		err = server.Database.Update(func(tx *TX) error {
			value, err = tx.HLen(requestBody.Key)
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			RaiseErrorResponse(err, context)
			return
		}
		MakeSuccessResponse(context, value)
	})
	server.Api.Router.POST("/action/hincrby", func(context *haruka.Context) {
		var err error
		var requestBody HashRequestBody
		if !ParseJSONOrErrorResponse(context, &requestBody) {
			return
		}
		var value int
		err = server.Database.Update(func(tx *TX) error {
			err = tx.HIncrBy(requestBody.Key, requestBody.Field, requestBody.Num)
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			RaiseErrorResponse(err, context)
			return
		}
		MakeSuccessResponse(context, value)
	})
	server.Api.Router.POST("/action/lpush", func(context *haruka.Context) {
		var err error
		var requestBody ListRequestBody
		if !ParseJSONOrErrorResponse(context, &requestBody) {
			return
		}
		var value int
		err = server.Database.Update(func(tx *TX) error {
			rawVal := make([][]byte, len(requestBody.Values))
			for i, v := range requestBody.Values {
				rawVal[i] = []byte(v)
			}
			err = tx.LPush(requestBody.Key, rawVal...)
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			RaiseErrorResponse(err, context)
			return
		}
		MakeSuccessResponse(context, value)
	})
	server.Api.Router.POST("/action/lpop", func(context *haruka.Context) {
		var err error
		var requestBody ListRequestBody
		if !ParseJSONOrErrorResponse(context, &requestBody) {
			return
		}
		var value [][]byte
		err = server.Database.Update(func(tx *TX) error {
			value, err = tx.LPop(requestBody.Key, requestBody.Count)
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			RaiseErrorResponse(err, context)
			return
		}
		strs := make([]string, 0)
		for _, v := range value {
			strs = append(strs, string(v))
		}
		MakeSuccessResponse(context, strs)
	})
	server.Api.Router.POST("/action/lindex", func(context *haruka.Context) {
		var err error
		var requestBody ListRequestBody
		if !ParseJSONOrErrorResponse(context, &requestBody) {
			return
		}
		var value []byte
		err = server.Database.Update(func(tx *TX) error {
			value, err = tx.LIndex(requestBody.Key, requestBody.Index)
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			RaiseErrorResponse(err, context)
			return
		}
		MakeSuccessResponse(context, string(value))
	})
	server.Api.Router.POST("/action/llen", func(context *haruka.Context) {
		var err error
		var requestBody ListRequestBody
		if !ParseJSONOrErrorResponse(context, &requestBody) {
			return
		}
		var value int
		err = server.Database.Update(func(tx *TX) error {
			value, err = tx.LLen(requestBody.Key)
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			RaiseErrorResponse(err, context)
			return
		}
		MakeSuccessResponse(context, value)
	})
	server.Api.Router.POST("/action/linsert", func(context *haruka.Context) {
		var err error
		var requestBody ListRequestBody
		if !ParseJSONOrErrorResponse(context, &requestBody) {
			return
		}
		err = server.Database.Update(func(tx *TX) error {
			if requestBody.Pivot == "AFTER" {
				requestBody.Index += 1
			}
			err = tx.LInsert(requestBody.Key, requestBody.Index, requestBody.Value)
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			RaiseErrorResponse(err, context)
			return
		}
		MakeSuccessResponse(context, 1)
	})
	server.Api.Router.POST("/action/lrange", func(context *haruka.Context) {
		var err error
		var requestBody ListRequestBody
		if !ParseJSONOrErrorResponse(context, &requestBody) {
			return
		}
		var value [][]byte
		err = server.Database.Update(func(tx *TX) error {
			value, err = tx.LRange(requestBody.Key, requestBody.Start, requestBody.End)
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			RaiseErrorResponse(err, context)
			return
		}
		strs := make([]string, 0)
		for _, v := range value {
			strs = append(strs, string(v))
		}
		MakeSuccessResponse(context, strs)
	})
	server.Api.Router.POST("/action/sadd", func(context *haruka.Context) {
		var err error
		var requestBody SetRequestBody
		if !ParseJSONOrErrorResponse(context, &requestBody) {
			return
		}
		var value int
		err = server.Database.Update(func(tx *TX) error {
			rawValues := make([]interface{}, 0)
			for _, v := range requestBody.Values {
				rawValues = append(rawValues, v)
			}
			err = tx.SAdd(requestBody.Key, rawValues...)
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			RaiseErrorResponse(err, context)
			return
		}
		MakeSuccessResponse(context, value)
	})
	server.Api.Router.POST("/action/srem", func(context *haruka.Context) {
		var err error
		var requestBody SetRequestBody
		if !ParseJSONOrErrorResponse(context, &requestBody) {
			return
		}
		var value int
		err = server.Database.Update(func(tx *TX) error {
			rawValues := make([]interface{}, 0)
			for _, v := range requestBody.Values {
				rawValues = append(rawValues, v)
			}
			err = tx.SRem(requestBody.Key, rawValues...)
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			RaiseErrorResponse(err, context)
			return
		}
		MakeSuccessResponse(context, value)
	})
	server.Api.Router.POST("/action/sdiff", func(context *haruka.Context) {
		var err error
		var requestBody SetRequestBody
		if !ParseJSONOrErrorResponse(context, &requestBody) {
			return
		}
		var value []interface{}
		err = server.Database.Update(func(tx *TX) error {
			value, err = tx.SDiff(requestBody.Key, requestBody.Values...)
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			RaiseErrorResponse(err, context)
			return
		}
		strs := make([]string, 0)
		for _, v := range value {
			strs = append(strs, utils.ToString(v))
		}
		MakeSuccessResponse(context, strs)
	})
	server.Api.Router.POST("/action/sinter", func(context *haruka.Context) {
		var err error
		var requestBody SetRequestBody
		if !ParseJSONOrErrorResponse(context, &requestBody) {
			return
		}
		var value []interface{}
		err = server.Database.Update(func(tx *TX) error {
			keys := append([]string{requestBody.Key}, requestBody.Values...)
			value, err = tx.SInter(keys...)
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			RaiseErrorResponse(err, context)
			return
		}
		strs := make([]string, 0)
		for _, v := range value {
			strs = append(strs, utils.ToString(v))
		}
		MakeSuccessResponse(context, strs)
	})
	server.Api.Router.POST("/action/sunion", func(context *haruka.Context) {
		var err error
		var requestBody SetRequestBody
		if !ParseJSONOrErrorResponse(context, &requestBody) {
			return
		}
		var value []interface{}
		err = server.Database.Update(func(tx *TX) error {
			keys := append([]string{requestBody.Key}, requestBody.Values...)
			value, err = tx.SUnion(keys...)
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			RaiseErrorResponse(err, context)
			return
		}
		strs := make([]string, 0)
		for _, v := range value {
			strs = append(strs, utils.ToString(v))
		}
		MakeSuccessResponse(context, strs)
	})
	server.Api.Router.POST("/action/sismember", func(context *haruka.Context) {
		var err error
		var requestBody SetRequestBody
		if !ParseJSONOrErrorResponse(context, &requestBody) {
			return
		}
		var value bool
		err = server.Database.Update(func(tx *TX) error {
			value, err = tx.SIsMember(requestBody.Key, requestBody.Value)
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			RaiseErrorResponse(err, context)
			return
		}
		MakeSuccessResponse(context, value)
	})
	server.Api.Router.POST("/action/smismember", func(context *haruka.Context) {
		var err error
		var requestBody SetRequestBody
		if !ParseJSONOrErrorResponse(context, &requestBody) {
			return
		}
		var value []bool
		err = server.Database.Update(func(tx *TX) error {
			rawValues := make([]interface{}, 0)
			for _, v := range requestBody.Values {
				rawValues = append(rawValues, v)
			}
			value, err = tx.SMIsMembers(requestBody.Key, rawValues...)
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			RaiseErrorResponse(err, context)
			return
		}
		MakeSuccessResponse(context, value)
	})
	server.Api.Router.POST("/action/scard", func(context *haruka.Context) {
		var err error
		var requestBody SetRequestBody
		if !ParseJSONOrErrorResponse(context, &requestBody) {
			return
		}
		var value int
		err = server.Database.Update(func(tx *TX) error {
			value, err = tx.SCard(requestBody.Key)
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			RaiseErrorResponse(err, context)
			return
		}
		MakeSuccessResponse(context, value)
	})
	server.Api.Router.POST("/action/smembers", func(context *haruka.Context) {
		var err error
		var requestBody SetRequestBody
		if !ParseJSONOrErrorResponse(context, &requestBody) {
			return
		}
		var value []interface{}
		err = server.Database.Update(func(tx *TX) error {
			value, err = tx.SMembers(requestBody.Key)
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			RaiseErrorResponse(err, context)
			return
		}
		strs := make([]string, 0)
		for _, v := range value {
			strs = append(strs, utils.ToString(v))
		}
		MakeSuccessResponse(context, strs)
	})
	server.Api.Router.POST("/action/spop", func(context *haruka.Context) {
		var err error
		var requestBody SetRequestBody
		if !ParseJSONOrErrorResponse(context, &requestBody) {
			return
		}
		var value []interface{}
		err = server.Database.Update(func(tx *TX) error {
			value, err = tx.SPop(requestBody.Key, requestBody.Count)
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			RaiseErrorResponse(err, context)
			return
		}
		resultStr := make([]string, 0)
		for _, v := range value {
			resultStr = append(resultStr, utils.ToString(v))
		}

		MakeSuccessResponse(context, resultStr)
	})
	server.Api.Router.POST("/action/srandmember", func(context *haruka.Context) {
		var err error
		var requestBody SetRequestBody
		if !ParseJSONOrErrorResponse(context, &requestBody) {
			return
		}
		var value []interface{}
		err = server.Database.Update(func(tx *TX) error {
			value, err = tx.SRandMember(requestBody.Key, requestBody.Count)
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			RaiseErrorResponse(err, context)
			return
		}
		resultStr := make([]string, 0)
		for _, v := range value {
			resultStr = append(resultStr, utils.ToString(v))
		}

		MakeSuccessResponse(context, resultStr)
	})
	server.Api.Router.POST("/action/zadd", func(context *haruka.Context) {
		var err error
		var requestBody ZSetRequestBody
		if !ParseJSONOrErrorResponse(context, &requestBody) {
			return
		}
		err = server.Database.Update(func(tx *TX) error {
			err = tx.ZAdd(requestBody.Key, requestBody.Pairs...)
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			RaiseErrorResponse(err, context)
			return
		}
		MakeSuccessResponse(context, len(requestBody.Pairs))
	})
	server.Api.Router.POST("/action/zrem", func(context *haruka.Context) {
		var err error
		var requestBody ZSetRequestBody
		if !ParseJSONOrErrorResponse(context, &requestBody) {
			return
		}
		err = server.Database.Update(func(tx *TX) error {
			err = tx.ZRem(requestBody.Key, requestBody.Members...)
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			RaiseErrorResponse(err, context)
			return
		}
		MakeSuccessResponse(context, len(requestBody.Members))
	})
	server.Api.Router.POST("/action/zcard", func(context *haruka.Context) {
		var err error
		var requestBody ZSetRequestBody
		if !ParseJSONOrErrorResponse(context, &requestBody) {
			return
		}
		var value int
		err = server.Database.Update(func(tx *TX) error {
			value, err = tx.ZCard(requestBody.Key)
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			RaiseErrorResponse(err, context)
			return
		}
		MakeSuccessResponse(context, value)
	})
	server.Api.Router.POST("/action/zscore", func(context *haruka.Context) {
		var err error
		var requestBody ZSetRequestBody
		if !ParseJSONOrErrorResponse(context, &requestBody) {
			return
		}
		var value float64
		err = server.Database.View(func(tx *TX) error {
			value, err = tx.ZScore(requestBody.Key, requestBody.Member)
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			RaiseErrorResponse(err, context)
			return
		}
		MakeSuccessResponse(context, value)
	})
	server.Api.Router.POST("/action/zmscore", func(context *haruka.Context) {
		var err error
		var requestBody ZSetRequestBody
		if !ParseJSONOrErrorResponse(context, &requestBody) {
			return
		}
		var value []float64
		err = server.Database.View(func(tx *TX) error {
			for _, member := range requestBody.Members {
				score, err := tx.ZScore(requestBody.Key, member)
				if err != nil {
					return err
				}
				value = append(value, score)
			}
			return nil
		})
		if err != nil {
			RaiseErrorResponse(err, context)
			return
		}
		MakeSuccessResponse(context, value)
	})
	server.Api.Router.POST("/action/zdiff", func(context *haruka.Context) {
		var err error
		var requestBody ZSetRequestBody
		if !ParseJSONOrErrorResponse(context, &requestBody) {
			return
		}
		var value []interface{}
		err = server.Database.View(func(tx *TX) error {
			value, err = tx.ZDiff(requestBody.Key, requestBody.Others...)
			if err != nil {
				return err
			}
			return nil
		})
		data := make([]string, 0)
		for _, v := range value {
			data = append(data, utils.ToString(v))
		}
		if err != nil {
			RaiseErrorResponse(err, context)
			return
		}
		MakeSuccessResponse(context, data)
	})
	server.Api.Router.POST("/action/zdiffstore", func(context *haruka.Context) {
		var err error
		var requestBody ZSetRequestBody
		if !ParseJSONOrErrorResponse(context, &requestBody) {
			return
		}
		var value int
		err = server.Database.Update(func(tx *TX) error {
			value, err = tx.ZDiffStore(requestBody.StoreKey, requestBody.Key, requestBody.Others...)
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			RaiseErrorResponse(err, context)
			return
		}
		MakeSuccessResponse(context, value)
	})
	server.Api.Router.POST("/action/zdiffcard", func(context *haruka.Context) {
		var err error
		var requestBody ZSetRequestBody
		if !ParseJSONOrErrorResponse(context, &requestBody) {
			return
		}
		var value int
		err = server.Database.View(func(tx *TX) error {
			value, err = tx.ZDiffCard(requestBody.Key, requestBody.Others...)
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			RaiseErrorResponse(err, context)
			return
		}
		MakeSuccessResponse(context, value)
	})
	server.Api.Router.POST("/action/zinter", func(context *haruka.Context) {
		var err error
		var requestBody ZSetRequestBody
		if !ParseJSONOrErrorResponse(context, &requestBody) {
			return
		}
		var value []interface{}
		err = server.Database.View(func(tx *TX) error {
			value, err = tx.ZInter(requestBody.Others...)
			if err != nil {
				return err
			}
			return nil
		})
		data := make([]string, 0)
		for _, v := range value {
			data = append(data, utils.ToString(v))
		}
		if err != nil {
			RaiseErrorResponse(err, context)
			return
		}
		MakeSuccessResponse(context, data)
	})
	server.Api.Router.POST("/action/zinterstore", func(context *haruka.Context) {
		var err error
		var requestBody ZSetRequestBody
		if !ParseJSONOrErrorResponse(context, &requestBody) {
			return
		}
		var value int
		err = server.Database.Update(func(tx *TX) error {
			value, err = tx.ZInterStore(requestBody.StoreKey, requestBody.Others...)
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			RaiseErrorResponse(err, context)
			return
		}
		MakeSuccessResponse(context, value)
	})
	server.Api.Router.POST("/action/zintercard", func(context *haruka.Context) {
		var err error
		var requestBody ZSetRequestBody
		if !ParseJSONOrErrorResponse(context, &requestBody) {
			return
		}
		var value int
		err = server.Database.View(func(tx *TX) error {
			value, err = tx.ZInterCard(requestBody.Others...)
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			RaiseErrorResponse(err, context)
			return
		}
		MakeSuccessResponse(context, value)
	})
	server.Api.Router.POST("/action/zunion", func(context *haruka.Context) {
		var err error
		var requestBody ZSetRequestBody
		if !ParseJSONOrErrorResponse(context, &requestBody) {
			return
		}
		var value []interface{}
		err = server.Database.View(func(tx *TX) error {
			value, err = tx.ZUnion(requestBody.Others...)
			if err != nil {
				return err
			}
			return nil
		})
		data := make([]string, 0)
		for _, v := range value {
			data = append(data, utils.ToString(v))
		}
		if err != nil {
			RaiseErrorResponse(err, context)
			return
		}
		MakeSuccessResponse(context, data)
	})
	server.Api.Router.POST("/action/zunionstore", func(context *haruka.Context) {
		var err error
		var requestBody ZSetRequestBody
		if !ParseJSONOrErrorResponse(context, &requestBody) {
			return
		}
		var value int
		err = server.Database.Update(func(tx *TX) error {
			value, err = tx.ZUnionStore(requestBody.StoreKey, requestBody.Others...)
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			RaiseErrorResponse(err, context)
			return
		}
		MakeSuccessResponse(context, value)
	})
	server.Api.Router.POST("/action/zunioncard", func(context *haruka.Context) {
		var err error
		var requestBody ZSetRequestBody
		if !ParseJSONOrErrorResponse(context, &requestBody) {
			return
		}
		var value int
		err = server.Database.View(func(tx *TX) error {
			value, err = tx.ZUnionCard(requestBody.Others...)
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			RaiseErrorResponse(err, context)
			return
		}
		MakeSuccessResponse(context, value)
	})
	server.Api.Router.POST("/action/zrange", func(context *haruka.Context) {
		var err error
		var requestBody ZSetRequestBody
		if !ParseJSONOrErrorResponse(context, &requestBody) {
			return
		}
		var value []interface{}
		err = server.Database.View(func(tx *TX) error {
			if requestBody.WithScore {
				value, err = tx.ZRangeWithScores(requestBody.Key, requestBody.Start, requestBody.Stop)
			} else {
				value, err = tx.ZRange(requestBody.Key, requestBody.Start, requestBody.Stop)
			}
			if err != nil {
				return err
			}
			return nil
		})
		data := make([]string, 0)
		for _, v := range value {
			data = append(data, utils.ToString(v))
		}
		if err != nil {
			RaiseErrorResponse(err, context)
			return
		}
		MakeSuccessResponse(context, data)
	})
	server.Api.Router.POST("/action/ping", func(context *haruka.Context) {
		MakeSuccessResponse(context, nil)
	})

}
func (a *HttpServer) run(addr string) error {
	err := a.Database.Open()
	if err != nil {
		return err
	}
	go a.Api.RunAndListen(addr)
	if err != nil {
		return err
	}
	return nil
}
func RaiseErrorResponse(err error, ctx *haruka.Context) {
	ctx.JSONWithStatus(haruka.JSON{
		"success": false,
		"error":   err.Error(),
	}, 200)
}
func MakeSuccessResponse(ctx *haruka.Context, data interface{}) {
	ctx.JSON(haruka.JSON{
		"success": true,
		"data":    data,
	})
}
func ParseJSONOrErrorResponse(ctx *haruka.Context, data interface{}) bool {
	err := ctx.ParseJson(data)
	if err != nil {
		RaiseErrorResponse(err, ctx)
		return false
	}
	return true
}
