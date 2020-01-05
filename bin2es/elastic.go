package bin2es

import (
	"context"

	"github.com/siddontang/go-log/log"
	es7 "github.com/olivere/elastic/v7"
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