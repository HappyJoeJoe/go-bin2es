package main

import (
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"flag"

	"github.com/juju/errors"
	"go-bin2es/bin2es"
	"github.com/siddontang/go-log/log"
)

var conf_path = flag.String("config", "./config/config.toml", "config file path")

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	flag.Parse()
	
	// 创建信号频道 sc
	sc := make(chan os.Signal, 1) 
	signal.Notify(
		sc,
		os.Kill,
		os.Interrupt,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	// 加载配置文件
	cfg, err := bin2es.NewConfigWithFile(*conf_path)
	if err != nil {
		log.Error(errors.ErrorStack(err))
		return
	}


	var isHA bool
	var h bin2es.HA
	if cfg.Etcd.Enable {
		h, err = bin2es.NewEtcd(cfg)
		isHA = true
	} else if cfg.Zk.Enable {
		h, err = bin2es.NewZK(cfg)
		isHA = true
	}
	if isHA {
		if err != nil {
			log.Error(errors.ErrorStack(err))
			return
		}

		log.Infof("----- acquiring lock for master-node-%d -----", cfg.Mysql.ServerID)

		if err = h.Lock(); err != nil {
			log.Error(errors.ErrorStack(err))
			return
		}
		defer func() {
			defer log.Info("----- close ha -----")
			h.UnLock()
			h.Close()
		}()
	}

	// 初始化bin2es实例
	b, err := bin2es.NewBin2es(cfg)
	if err != nil {
		log.Error(errors.ErrorStack(err))
		return
	}

	finish := make(chan bool)

	// 运行bin2es实例
	go func() {
		b.Run()
		finish <- true
	}()

	select{
	case n := <-sc:
		log.Infof("receive signal:%v, quiting", n)		
	case <-b.Ctx().Done():
		log.Infof("context done, err:%v", b.Ctx().Err())
	}

	b.Close()

	<-finish
}