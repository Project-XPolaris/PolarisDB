# PolarisDB

参考了Redis的主要设计思路，使用golang进行编写的Key-Value数据库

## 主要的数据结构
  * String (radix)
  * Hash (hashmap)
  * List (ziplist)
  * Set  (intset\hashmap)
  * Sorted Set (skiplist)
## 使用的一些特性
* Key TTL
* AOF 持久化
* Http方式访问
* 数据淘汰策略

## 支持的一些命令
* String
    * SET
    * GET
    * APPEND
    * DECR
    * DECRBY
    * INCR
    * INCRBY
    * GETDEL
    * GETEX
    * GETRANGE
    * MGET
* Hash
    * HGET
    * HSET
    * HDEL
    * HGETALL
    * HKEYS
    * HVALS
    * HEXTSTS
    * HLEN
    * HINCRBY
* List
    * LPUSH
    * LPOP
    * Lindex
    * LLEN
    * LINSERT
    * LRANGE
* Set
    * SADD
    * SREM
    * SDIFF
    * SINTER
    * SUNION
    * SISMEMBER
    * SMISMEMBER
    * SCARD
    * SMEMBERS
    * SPOP
    * SRANDMEMBER
* Sorted Set
    * ZADD
    * ZREM
    * ZCARD
    * ZSCORE
    * ZMSCORE
    * ZDIFF
    * ZDIFFSTORE
    * ZDIFFCARD
    * ZINTER
    * ZINTERSTORE
    * ZINTERCARD
    * ZUNION
    * ZUNIONSTORE
    * ZUNIONCARD
    * ZRANGE