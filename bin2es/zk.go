package bin2es

import (
	//系统
	"fmt"
	"time"
	//第三方
	"github.com/juju/errors"
	"github.com/samuel/go-zookeeper/zk"
	"github.com/siddontang/go-log/log"
)

type ZK struct {
	serverId       uint32
	lockPath       string
	hosts          []string
	conn           *zk.Conn
	lock           *zk.Lock
	acls           []zk.ACL
	sessionTimeout int
}

func NewZK(c *Config) (*ZK, error) {
	z := new(ZK)
	z.serverId = c.Mysql.ServerID
	z.lockPath = fmt.Sprintf("%s-%d", c.Zk.LockPath, z.serverId)
	z.hosts = c.Zk.Hosts
	z.acls = zk.WorldACL(zk.PermAll)
	z.sessionTimeout = c.Zk.SessionTimeout

	var err error
	if z.conn, _, err = zk.Connect(z.hosts, time.Second*time.Duration(z.sessionTimeout)); err != nil {
		log.Errorf("zookeeper connect failed, err:%s", errors.Trace(err))
		return nil, errors.Trace(err)
	}
	z.lock = zk.NewLock(z.conn, z.lockPath, z.acls)

	return z, nil
}

func (z *ZK) Lock() error {
	if err := z.lock.Lock(); err != nil {
		log.Errorf("zookeeper lock failed, err:%s", errors.Trace(err))
		return errors.Trace(err)
	}

	return nil
}

func (z *ZK) UnLock() error {
	if err := z.lock.Unlock(); err != nil {
		log.Errorf("zookeeper unlock failed, err:%s", errors.Trace(err))
		return errors.Trace(err)
	}

	return nil
}

func (z *ZK) Close() {
	z.conn.Close()
}
