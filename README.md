# go-bin2es

go-bin2es is a service syncing binlog to es

+ editing your config.toml, and configure it like following:

```
data_dir = "./var"

[es]
nodes = [
	"http://127.0.0.1:9200"
]
bulk_size = 1024
flush_duration = 500


[mysql]
addr = "127.0.0.1:3306"
user = "root"
pwd = "root"
charset = "utf8"
server_id = 1


[[source]]
schema = "dos_test"
tables = [
	"jzg_car_models",
	"auction_cars",
	"auction_car_infos",
	"auction_car_details"
]
[[source]]
schema = "dos_test"
tables = [
	"used_car_tickets",
	"jzg_car_models"
]

```
+ execute  ./bin/go-bin2es