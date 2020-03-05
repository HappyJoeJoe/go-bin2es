package bin2es

import (
	"context"

	es7 "github.com/olivere/elastic/v7"
	"github.com/siddontang/go-log/log"
)

type MyES struct {
	Client 		*es7.Client
	BulkService *es7.BulkService
	Ctx         context.Context
}

func (e *MyES) Close() {
	defer log.Info("----- ES closed -----")

	e.Client.Stop()
}