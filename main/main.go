package main

import (
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"flag"

	"github.com/juju/errors"
	"github.com/siddontang/go-log/log"
	"go-bin2es/bin2es"
)

var conf_path = flag.String("config", "./config/config.toml", "config file path")
var logLevel = flag.String("log_level", "info", "log level")

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	flag.Parse()
	
	log.SetLevelByName(*logLevel)

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