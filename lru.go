package polarisdb

import "time"

var (
	LruClockMax float64 = (1 << 24) - 1
)

type LRUClock struct {
	db *PolarisDB
}

func (c *LRUClock) GetTime() float64 {
	t := float64(time.Now().Unix()) / c.db.Config.LruClockResolution
	if t > LruClockMax {
		t = t - LruClockMax
	}
	return t
}

func (c *LRUClock) GetLruNow(idletime float64) float64 {
	nowClock := c.GetTime()
	if nowClock >= idletime {
		return (nowClock - idletime) * c.db.Config.LruClockResolution
	} else {
		return (LruClockMax - idletime + nowClock) * c.db.Config.LruClockResolution
	}
}
