#!/usr/bin/python3
# -*- coding: UTF-8 -*-

import pymysql
import requests
import json
import time
import sys

tests = [
	{
		'sql': 'update Child set name = "---" where id = 1',
		'name': '---'
	},
	{
		'sql': 'update Child set name = "123" where id = 1',
		'name': '123'
	},
]

sql = 'insert into Child (id, name, sex, parent_id) values ({}, "j", "m", {}, "cs")'

# 打开数据库连接
db = pymysql.connect("127.0.0.1", "root", "root", "my_test", charset='utf8mb4' )
# 使用cursor()方法获取操作游标 
cursor = db.cursor()
# 记录SQL执行序号
seq = 1
# 记录多少行数据未按执行结果期望
rows = 0

for seq in range(5000):
	# cursor.execute(sql.format(seq+1,seq+1))
	# print("executing", i, "SQL:", sql.format(seq,seq))
	# db.commit()
	for test in tests:
		# 使用execute方法执行SQL语句
		cursor.execute(test['sql'])
		db.commit()
		print("executing", seq, "SQL:", test['sql'])
		time.sleep(0.5)
		response = requests.get('http://localhost:9200/test_es/_doc/1')
		dict = json.loads(str(response.content, encoding = "utf8"))
		if test['name'] != dict['_source']['name']:
			print("db: ", test['name'])
			print("es: ", dict['_source']['name'])
			rows = rows + 1

print(rows, "rows(s) unexpected!")
# 关闭数据库连接
db.close()
