package bin2es

/* 高可用接口
 * 可以支持 zookeeper, etcd 等
 */
type HA interface {
	Lock() error
	UnLock() error
	Close()
}
