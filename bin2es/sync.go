package bin2es

import (
	//系统
	"context"
	"fmt"
	"reflect"
	"strconv"
	"time"
	//第三方
	"github.com/juju/errors"
	es7 "github.com/olivere/elastic/v7"
	"github.com/siddontang/go-mysql/canal"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"github.com/siddontang/go-log/log"
)

type posSaver struct {
	pos   mysql.Position
	force bool
}

type ReqJson struct {
	data interface{}
}

type eventHandler struct {
	b *Bin2es
}

//flush logs触发
func (h *eventHandler) OnRotate(e *replication.RotateEvent) error {
	pos := mysql.Position{
		Name: string(e.NextLogName),
		Pos:  uint32(e.Position),
	}

	h.b.syncCh <- posSaver{pos, true}

	return h.b.ctx.Err()
}

//表结构变动触发
func (h *eventHandler) OnTableChanged(schema, table string) error {
	return nil
}

//DDL语句触发
func (h *eventHandler) OnDDL(nextPos mysql.Position, _ *replication.QueryEvent) error {
	h.b.syncCh <- posSaver{nextPos, true}
	return h.b.ctx.Err()
}

//DML语句触发
func (h *eventHandler) OnXID(nextPos mysql.Position) error {
	h.b.syncCh <- posSaver{nextPos, false}
	return h.b.ctx.Err()
}

//DML语句触发
func (h *eventHandler) OnRow(e *canal.RowsEvent) error {
	schema := e.Table.Schema
	table := e.Table.Name
	columns := e.Table.Columns
	action := e.Action
	message := make(map[string]interface{})

	if action == "delete" || h.b.isInTblFilter(schema+"."+table) != true {
		return nil
	}

	var values []interface{}
	switch action {
	case "insert":
		values = e.Rows[0]
	case "update":
		values = e.Rows[1]
	}

	body := make(map[string]string)
	for i := 0; i < len(columns); i++ {
		body[columns[i].Name] = toString(values[i])
	}
	message["schema"] = schema
	message["table"] = table
	message["action"] = action
	message["body"] = body

	h.b.syncCh <- ReqJson{message}

	return nil
}

//DDL, DML语句触发
func (h *eventHandler) OnGTID(gtid mysql.GTIDSet) error {
	return nil
}

//DDL, DML语句触发
func (h *eventHandler) OnPosSynced(pos mysql.Position, set mysql.GTIDSet, force bool) error {
	return nil
}

func (h *eventHandler) String() string {
	return "Bin2esEventHandler"
}

func (b *Bin2es) syncES() {
	defer log.Info("----- syncES quit -----")
	defer func() { b.finish <- true }()
	defer b.wg.Done()

	log.Infof("begin to sync binlog to es")

	ticker := time.NewTicker(time.Duration(b.c.Es.FlushDuration) * time.Millisecond)
	defer ticker.Stop()

	lastSavedTime := time.Now()
	row := make(map[string]interface{})
	var pos mysql.Position
	var err error

	for {
		needPipe := false
		needFlush := false
		needSavePos := false
		select {
		case v := <-b.syncCh:
			switch v := v.(type) {
			case posSaver:
				now := time.Now()
				if v.force || now.Sub(lastSavedTime) > time.Duration(b.c.MasterInfo.FlushDuration)*time.Second {
					lastSavedTime = now
					needFlush = true
					needSavePos = true
					pos = v.pos
				}
			case ReqJson:
				row = v.data.(map[string]interface{})
				needPipe = true
			default:
				log.Errorf("unrecognized type:%s", reflect.TypeOf(v))
				b.cancel()
				return
			}
		case <-ticker.C:
			needFlush = true
		case <-b.ctx.Done():
			return
		}

		if needPipe {
			if err = b.Pipeline(row); err != nil {
				log.Errorf("pipeline exc failed, err:%+v", err)
				b.cancel()
				return
			}

			if b.esCli.bulkService.NumberOfActions() >= b.c.Es.BulkSize {
				needFlush = true
			}
		}

		if needFlush && b.esCli.bulkService.NumberOfActions() > 0 {
			bulkResponse, err := b.esCli.bulkService.Do(context.TODO())
			if err != nil {
				log.Errorf("bulkService Do failed, err:%+v", err)
				b.cancel()
				return
			}

			if bulkResponse == nil {
				log.Error("bulkResponse should not be nil; got nil")
				b.cancel()
				return
			}

			failedResults := bulkResponse.Failed()
			if failedResults != nil && len(failedResults) > 0 {
				for _, failedResult := range failedResults {
					log.Errorf("Failed bulk response: %+v", failedResult)
				}
				b.cancel()
				return
			}
		}

		if needSavePos {
			if err = b.master.Save(pos); err != nil {
				log.Errorf("save sync position:%s err:%+v, close sync", pos, err)
				b.cancel()
				return
			}
		}
	}

	return
}

func (b *Bin2es) Pipeline(binRow map[string]interface{}) error {

	schema := binRow["schema"].(string)
	table := binRow["table"].(string)
	action := binRow["action"].(string)

	confs := b.event2Pipe[fmt.Sprintf("%s_%s_%s", schema, table, action)]
	for _, conf := range confs {

		rows := []map[string]interface{}{binRow}

		for _, pipeline := range conf.Pipelines {
			for funcName, funcArgs := range pipeline {

				tmpRows := make([]map[string]interface{}, 0)

				for _, row := range rows {

					args := []reflect.Value{reflect.ValueOf(row), reflect.ValueOf(funcArgs.(map[string]interface{}))}

					retValues := b.refFuncMap[funcName].Call(args)

					if !retValues[1].IsNil() {
						if err := retValues[1].Interface().(error); err != nil {
							return errors.Trace(err)
						}
					}

					if retValues[0].IsNil() || !retValues[0].CanInterface() {
						return errors.Errorf("pipeline:%s retValues:%+v exception, row:%+v funcArgs:%+v", funcName, retValues[0], row, funcArgs)
					}
					newRows := retValues[0].Interface().(ROWS)
					if len(newRows) == 0 {
						// log.Warnf("pipeline:%s get null result, row:%+v funcArgs:%+v", funcName, row, funcArgs)
						return nil
					}
					tmpRows = append(tmpRows, newRows...)
				}

				rows = tmpRows
			}
		}

		var request es7.BulkableRequest
		for _, esRow := range rows {
			docId := esRow["_id"].(string)
			delete(esRow, "_id")

			switch action {
			case "insert":
				request = es7.NewBulkIndexRequest().Index(conf.Dest.Index).Id(docId).Doc(esRow)
			case "update":
				request = es7.NewBulkUpdateRequest().Index(conf.Dest.Index).Id(docId).Doc(esRow).DocAsUpsert(true)
			}
			b.esCli.bulkService.Add(request).Refresh("true")
		}
	}

	return nil
}

func toString(i interface{}) string {
	switch i := i.(type) {
	case int:
		return strconv.FormatInt(int64(i), 10)
	case int8:
		return strconv.FormatInt(int64(i), 10)
	case int16:
		return strconv.FormatInt(int64(i), 10)
	case int32:
		return strconv.FormatInt(int64(i), 10)
	case int64:
		return strconv.FormatInt(i, 10)
	case uint:
		return strconv.FormatUint(uint64(i), 10)
	case uint8:
		return strconv.FormatUint(uint64(i), 10)
	case uint16:
		return strconv.FormatUint(uint64(i), 10)
	case uint32:
		return strconv.FormatUint(uint64(i), 10)
	case uint64:
		return strconv.FormatUint(i, 10)
	case float32:
		return strconv.FormatFloat(float64(i), 'f', -1, 64)
	case float64:
		return strconv.FormatFloat(i, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(i)
	case string:
		return i
	}

	return ""
}
