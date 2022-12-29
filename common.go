package polarisdb

func SetExpire(db *PolarisDB, key string, ttl int64) error {
	db.Sweeper.SetKeyExpire(key, ttl)
	return nil
}
