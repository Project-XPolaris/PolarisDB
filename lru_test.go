package polarisdb

//func TestLRUClock(t *testing.T) {
//	LruClockMax = float64(time.Now().Unix() + 5)
//	for i := 0; i < 100; i++ {
//		<-time.After(1 * time.Second)
//		val2 := LRUClock()
//		fmt.Printf("%f\n", val2)
//	}
//}
//func TestEstimateObjectIdleTime(t *testing.T) {
//	LruClockMax = float64(time.Now().Unix() + 4)
//	objectLRU := LRUClock()
//	for i := 0; i < 100; i++ {
//		<-time.After(1 * time.Second)
//		val := GetLruNow(objectLRU)
//		fmt.Printf("%f\n", val)
//	}
//}
