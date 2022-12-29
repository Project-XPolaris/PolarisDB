package main

import (
	"context"
	"github.com/projectxpolaris/polarisdb"
)

func main() {
	dbInst := polarisdb.NewDB(&polarisdb.DBConfig{})
	dbInst.Open()
	dbInst.RunServer()
	<-context.Background().Done()
}
