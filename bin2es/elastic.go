package bin2es

import (
	//系统
	"context"
	"net/http"
	"syscall"
	"time"
	//第三方
	"github.com/juju/errors"
	es7 "github.com/olivere/elastic/v7"
	"github.com/siddontang/go-log/log"
)

type MyES struct {
	Client      *es7.Client
	BulkService *es7.BulkService
	Ctx         context.Context
}

type MyRetrier struct {
	backoff es7.Backoff
}

func NewMyRetrier() *MyRetrier {
	return &MyRetrier{
		backoff: es7.NewConstantBackoff(time.Second),
	}
}

func (r *MyRetrier) Retry(ctx context.Context, retry int, req *http.Request, resp *http.Response, err error) (time.Duration, bool, error) {
	// Fail hard on a specific error
	if err == syscall.ECONNREFUSED {
		return 0, false, errors.New("Elasticsearch or network down")
	}

	// Let the backoff strategy decide how long to wait and whether to stop
	wait, stop := r.backoff.Next(retry)

	log.Infof("request es failed, retrying wait:%d  stop:%t", wait, stop)

	return wait, stop, nil
}

func (e *MyES) Close() {
	defer log.Info("----- ES closed -----")

	e.Client.Stop()
}
