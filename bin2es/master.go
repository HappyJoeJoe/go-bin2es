package bin2es

import (
	//系统
	"fmt"
	"sync"
	//第三方
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/juju/errors"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-log/log"
)

type dbInfo struct {
	sync.RWMutex

	db       *sql.DB
	serverId uint32
	schema   string
	table    string
	name     string
	pos      uint32
}

func loadMasterInfo(serverId uint32, masterInfo MasterInfo) (*dbInfo, error) {
	var m dbInfo

	user := masterInfo.User
	pwd := masterInfo.Pwd
	addr := masterInfo.Addr
	port := masterInfo.Port
	schema := masterInfo.Schema
	table := masterInfo.Table
	charset := masterInfo.Charset
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=%s", user, pwd, addr, port, charset)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Errorf("sql Open error: %s", err)
		return nil, errors.Trace(err)
	}
	if err = db.Ping(); err != nil {
		log.Errorf("db ping error: %s", err)
		return nil, errors.Trace(err)
	}

	//若没有schema:bin2es, 则创建
	dbSQL := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", schema)
	if _, err := db.Exec(dbSQL); err != nil {
		log.Errorf("create database %s failed, err:%s", schema, err)
		return nil, errors.Trace(err)
	}

	//若没有table:master_info, 则创建
	tblSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s (
			server_id int NOT NULL,
			bin_name  varchar(256) NOT NULL DEFAULT '',
			bin_pos   bigint NOT NULL  DEFAULT 0,
			PRIMARY KEY (server_id)
		)ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='binlog位置表'
	`, schema, table)
	if _, err := db.Exec(tblSQL); err != nil {
		log.Errorf("create table %s failed, err:%s", table, err)
		return nil, errors.Trace(err)
	}

	//读取master_info的`bin_name` `bin_pos`
	var name string
	var pos uint32
	querySQL := fmt.Sprintf("SELECT bin_name, bin_pos FROM %s.%s WHERE server_id = %d", schema, table, serverId)
	rows, err := db.Query(querySQL)
	if err != nil {
		log.Errorf("query %s.%s failed, err:%s", schema, table, err)
		return nil, errors.Trace(err)
	}
	defer rows.Close()

	var num = 0
	for rows.Next() {
		num += 1
		err = rows.Scan(&name, &pos)
		if err != nil {
			log.Errorf("iteration %s.%s failed, err:%s", schema, table, err)
			return nil, errors.Trace(err)
		}
		log.Infof("bin_name:%s bin_pos:%d", name, pos)
	}
	err = rows.Err()
	if err != nil {
		log.Errorf("rows.Err:%s", err)
		return nil, errors.Trace(err)
	}

	if num == 0 {
		insertSQL := fmt.Sprintf("INSERT INTO %s.%s (server_id, bin_name, bin_pos) VALUES (%d, %s, %d)", schema, table, serverId, "''", 0)
		log.Info(insertSQL)
		if _, err := db.Exec(insertSQL); err != nil {
			log.Infof("insert %s.%s failed, err:%s", schema, table, err)
			return nil, errors.Trace(err)
		}
	}

	m.db = db
	m.name = name
	m.pos = pos
	m.schema = schema
	m.table = table
	m.serverId = serverId

	return &m, errors.Trace(err)
}

func (m *dbInfo) Save(pos mysql.Position) error {
	log.Infof("save position %s", pos)

	m.Lock()
	defer m.Unlock()

	m.name = pos.Name
	m.pos = pos.Pos

	//写db
	var err error
	updateSQL := fmt.Sprintf("UPDATE %s.%s SET bin_name = '%s', bin_pos = %d WHERE server_id = %d", m.schema, m.table, m.name, m.pos, m.serverId)
	if _, err = m.db.Exec(updateSQL); err != nil {
		log.Errorf("update %s.%s failed, err:%s", m.schema, m.table, err)
	}

	return errors.Trace(err)
}

func (m *dbInfo) Position() mysql.Position {
	m.RLock()
	defer m.RUnlock()

	return mysql.Position{
		Name: m.name,
		Pos:  m.pos,
	}
}

func (m *dbInfo) SetPosition(p mysql.Position) {
	m.RLock()
	defer m.RUnlock()

	m.name = p.Name
	m.pos = p.Pos
}

func (m *dbInfo) Close() error {
	log.Info("----- closing master -----")

	pos := m.Position()

	return m.Save(pos)
}
