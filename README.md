# go-bin2es

go-bin2es is a service syncing binlog to es

采用了[go-mysql库](https://github.com/siddontang/go-mysql)可以过滤指定的db的table, 从而把binlog数据通过[配置的方法](./config/binlog2es.json)过滤后, 刷新到`elasticsearch7`上

## 优点:
+ 支持高版本的elasticsearch7
+ 原生支持es的嵌套对象、嵌套数组类型
+ 实时性高, 低时延
+ 性能还可以, 每秒处理1k左右的数据

### editing your config.toml, and configure it like following:

```
data_dir = "./var"

[es]
nodes = [
	"http://127.0.0.1:9200" #es集群
]
bulk_size = 1024  #批量刷新个数
flush_duration = 500  #批量刷新时间间隔, 单位:ms


[mysql]
addr = "127.0.0.1:3306"
user = "root"
pwd = "root"
charset = "utf8"
server_id = 1


[[source]]
schema = "my_db"  #过滤my_db下的test_tbl表, 可以配置多个
tables = [
	"test_tbl",
	"test_tbl2"
]
[[source]]
schema = "my_db"
tables = [
	"test_tbl2"
]

```