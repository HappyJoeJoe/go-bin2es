# go-bin2es

go-bin2es is a service syncing binlog to es

采用了[go-mysql](https://github.com/siddontang/go-mysql)可以过滤指定的db的table, 从而把binlog数据通过[配置的方法](./config/binlog2es.json)过滤后, 刷新到`elasticsearch7`上

## 优点:
+ 支持高版本的elasticsearch7
+ 原生支持es的嵌套对象、嵌套数组类型
+ 实时性高, 低时延
+ 支持自定义函数[UserDefinedFunc](https://github.com/HappyJoeJoe/go-bin2es/blob/f2b8b741eb9faea93eb6c92781d90dc7baf89425/bin2es/row_handler.go#L163)去处理es数据, 可扩展更强


# Example
```sql

create database my_test;

create table Parent (
	id int not null auto_increment primary key,
	name varchar(64) not null,
	sex char(1) not null
)comment = '父';

create table Child (
	id int not null auto_increment primary key,
	name varchar(64) not null,
	sex char(1) not null, 
	parent_id int not null
)comment = '子';

Parent:
insert into Parent (name, sex) values ('Tom', "m"); #id为1
Child:
insert into Child (name, sex, parent_id) values ('Tom1', 'm', 1); #Tom的孩子1
insert into Child (name, sex, parent_id) values ('Tom2', 'm', 1); #Tom的孩子2
insert into Child (name, sex, parent_id) values ('Tom3', 'm', 1); #Tom的孩子3

Parent:
insert into Parent (name, sex) values ('Jerry', "m"); #id为2
Child:
insert into Child (name, sex, parent_id) values ('Jerry1', 'm', 2); #Jerry的孩子1
insert into Child (name, sex, parent_id) values ('Jerry2', 'f', 2); #Jerry的孩子2
```



### editing your config.toml, and configure it like following:

```toml

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
schema = "my_test"  #过滤my_test下的`Parent`和`Child`表, 可以配置多个
tables = [
	"Parent",
	"Child"
]

```

### editing bin2es.json

```json

[
    {
        "schema":"my_test",
        "tables": [
            "Parent",
            "Child"
        ],
        "actions":["insert", "update", "delete"],
        "pipeline":{
            "PkDoSQL":{
                "sql":"SELECT Parent.id, Parent.name, Parent.sex, group_concat(concat_ws('_', Child.name, Child.sex) separator ',') as Childs FROM Parent join Child on Parent.id = Child.parent_id WHERE (?) GROUP BY Parent.id",
                "replaces": [ #若遇到Parent或Child, 自动将`?`替换为`$key.$value = {该表对应的$value对应的字段的值}`
                    {"Parent":"id"},  #eg: 若遇到Parent, 则`?`被替换为`Parent.id = 行数据对应的`id`字段的值`
                    {"Child":"parent_id"} #eg: Child, 则`?`被替换为`Child.parent_id = 行数据对应的`parent_id`字段的值`
                ]
            },
            "NestedObj":{
                "common":"profile", 
                "fields":[
                    {"name":"es_name"}, 将查询到的结果的`name`字段放进`profile`的`es_name`下
                    {"sex":"es_sex"}    同理
                ]
            },
            "NestedArray":{
                "sql_field":"Childs",   将查询到的结果的`Childs`字段解析放入到common指定的`childs`下
                "common":"childs",
                "pos2fields":[          其中解析结果的第一个位置放入到es的`es_name`下, 第二个位置放入到`es_sex`下
                    {"es_name":1},
                    {"es_sex":2}
                ],
                "fields_seprator": "_",
                "group_seprator": ","
            }
        },
        "dest":{
            "index":"test_es"           es的索引名
        }
    }
]

```

### execute ./bin/go-bin2es
+ then, you will get results in es by using following API: `GET /test_es/_search`

```json

{
    "took": 0,
    "timed_out": false,
    "_shards": {
        "total": 1,
        "successful": 1,
        "skipped": 0,
        "failed": 0
    },
    "hits": {
        "total": {
            "value": 2,
            "relation": "eq"
        },
        "max_score": 1.0,
        "hits": [
            {
                "_index": "test_es",
                "_type": "_doc",
                "_id": "1",
                "_score": 1.0,
                "_source": {
                    "childs": [  #嵌套数组
                        {
                            "chl_name": "Tom1",
                            "chl_sex": "f"
                        },
                        {
                            "chl_name": "Tom2",
                            "chl_sex": "f"
                        },
                        {
                            "chl_name": "Tom3",
                            "chl_sex": "m"
                        }
                    ],
                    "id": "1",
                    "profile": { #嵌套对象
                        "es_name": "Tom",
                        "es_sex": "m"
                    }
                }
            },
            {
                "_index": "test_es",
                "_type": "_doc",
                "_id": "2",
                "_score": 1.0,
                "_source": {
                    "childs": [  #嵌套数组
                        {
                            "chl_name": "Jerry1",
                            "chl_sex": "m"
                        },
                        {
                            "chl_name": "Jerry2",
                            "chl_sex": "f"
                        }
                    ],
                    "id": "2",
                    "profile": { #嵌套对象
                        "es_name": "Jerry",
                        "es_sex": "m"
                    }
                }
            }
        ]
    }
}

```

### 如果你觉得对你有用, 可以给我打赏一瓶Colo
![Alt text](https://github.com/HappyJoeJoe/go-bin2es/blob/master/9db0fe58b386110503eb2a08703752bc.jpg)
