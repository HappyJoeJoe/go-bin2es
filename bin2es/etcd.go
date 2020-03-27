package bin2es

import (
	//系统
	"context"
	"fmt"
	"time"
	//第三方
	"crypto/tls"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/juju/errors"
	"go.etcd.io/etcd/pkg/transport"
	"github.com/siddontang/go-log/log"
)

type EtcdCli struct {
	ServerId    uint32
	LockPath    string
	EndPoints   []string
	Client      *clientv3.Client
	Session     *concurrency.Session
	Mutex       *concurrency.Mutex
	CertPath    string
	KeyPath     string
	CaPath      string
	DialTimeout int
}

func NewEtcd(c *Config) (*EtcdCli, error) {
	e := new(EtcdCli)
	e.ServerId = c.Mysql.ServerID
	e.LockPath = fmt.Sprintf("%s-%d", c.Etcd.LockPath, e.ServerId)
	e.EndPoints = c.Etcd.Endpoints
	e.CertPath = c.Etcd.CertPath
	e.KeyPath = c.Etcd.KeyPath
	e.CaPath = c.Etcd.CaPath
	e.DialTimeout = c.Etcd.DialTimeout

	var err error
	var tlsConfig *tls.Config
	if c.Etcd.EnableTLS {
		tlsInfo := transport.TLSInfo{
			CertFile:      e.CertPath,
			KeyFile:       e.KeyPath,
			TrustedCAFile: e.CaPath,
		}

		tlsConfig, err = tlsInfo.ClientConfig()
		if err != nil {
			log.Errorf("etcd init tls config failed, err:%s", errors.Trace(err))
			return nil, errors.Trace(err)
		}
	}

	if e.Client, err = clientv3.New(clientv3.Config{
		Endpoints:   e.EndPoints,
		DialTimeout: time.Duration(e.DialTimeout) * time.Second,
		TLS:         tlsConfig,
	}); err != nil {
		log.Errorf("etcd create client failed, err:%s", errors.Trace(err))
		return nil, errors.Trace(err)
	}

	if e.Session, err = concurrency.NewSession(e.Client); err != nil {
		log.Errorf("etcd create session failed, err:%s", errors.Trace(err))
		return nil, errors.Trace(err)
	}

	e.Mutex = concurrency.NewMutex(e.Session, e.LockPath)

	return e, nil
}

func (e *EtcdCli) Lock() error {
	if err := e.Mutex.Lock(context.TODO()); err != nil {
		log.Errorf("etcd mutex failed, err:%s", errors.Trace(err))
		return errors.Trace(err)
	}

	return nil
}

func (e *EtcdCli) UnLock() error {
	if err := e.Mutex.Unlock(context.TODO()); err != nil {
		log.Errorf("etcd unlock failed, err:%s", errors.Trace(err))
		return errors.Trace(err)
	}

	return nil
}

func (e *EtcdCli) Close() {
	e.Session.Close()
	e.Client.Close()
}
