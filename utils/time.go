package utils

import "time"

func GetAbsExpireTime(expireTime int64) int64 {
	if expireTime < 0 {
		return -1
	}
	return expireTime + time.Now().UnixMilli() //ms
}
