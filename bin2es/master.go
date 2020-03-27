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

type masterInfo struct {
	sync.RWMutex

	DB       *sql.DB
	ServerID uint32
	Schema   string
	Table    string
	Name     string
	Pos      uint32
}

func loadMasterInfo(server_id uint32, master_info MasterInfo) (*masterInfo, error) {
	var m masterInfo

	user := master_info.User
	pwd := master_info.Pwd
	addr := master_info.Addr
	port := toString(master_info.Port)
	schema := master_info.Schema
	table := master_info.Table
	charset := master_info.Charset
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/?charset=%s", user, pwd, addr, port, charset)

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
	var Name string
	var Pos uint32
	querySQL := fmt.Sprintf("SELECT bin_name, bin_pos FROM %s.%s WHERE server_id = %d", schema, table, server_id)
	rows, err := db.Query(querySQL)
	if err != nil {
		log.Errorf("query %s.%s failed, err:%s", schema, table, err)
		return nil, errors.Trace(err)
	}
	defer rows.Close()

	var Num = 0
	for rows.Next() {
		Num += 1
		err = rows.Scan(&Name, &Pos)
		if err != nil {
			log.Errorf("iteration %s.%s failed, err:%s", schema, table, err)
			return nil, errors.Trace(err)
		}
		log.Infof("bin_name:%s bin_pos:%d", Name, Pos)
	}
	err = rows.Err()
	if err != nil {
		log.Errorf("rows.Err:%s", err)
		return nil, errors.Trace(err)
	}

	if Num == 0 {
		insertSQL := fmt.Sprintf("INSERT INTO %s.%s (server_id, bin_name, bin_pos) VALUES (%d, %s, %d)", schema, table, server_id, "''", 0)
		log.Info(insertSQL)
		if _, err := db.Exec(insertSQL); err != nil {
			log.Infof("insert %s.%s failed, err:%s", schema, table, err)
			return nil, errors.Trace(err)
		}
	}

	m.DB = db
	m.Name = Name
	m.Pos = Pos
	m.Schema = schema
	m.Table = table
	m.ServerID = server_id

	return &m, errors.Trace(err)
}

func (m *masterInfo) Save(pos mysql.Position) error {
	log.Infof("save position %s", pos)

	m.Lock()
	defer m.Unlock()

	m.Name = pos.Name
	m.Pos = pos.Pos

	//写db
	var err error
	updateSQL := fmt.Sprintf("UPDATE %s.%s SET bin_name = '%s', bin_pos = %d WHERE server_id = %d", m.Schema, m.Table, m.Name, m.Pos, m.ServerID)
	if _, err = m.DB.Exec(updateSQL); err != nil {
		log.Errorf("update %s.%s failed, err:%s", m.Schema, m.Table, err)
	}

	return errors.Trace(err)
}

func (m *masterInfo) Position() mysql.Position {
	m.RLock()
	defer m.RUnlock()

	return mysql.Position{
		Name: m.Name,
		Pos:  m.Pos,
	}
}

func (m *masterInfo) SetPosition(p mysql.Position) {
	m.RLock()
	defer m.RUnlock()

	m.Name = p.Name
	m.Pos = p.Pos
}

func (m *masterInfo) Close() error {
	log.Info("----- closing master -----")

	pos := m.Position()

	return m.Save(pos)
}
