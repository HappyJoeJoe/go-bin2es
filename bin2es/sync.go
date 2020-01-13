package bin2es

import (
	"reflect"
	"encoding/json"
	"strings"
	"context"
	"time"
	"strconv"

	"github.com/juju/errors"
	"github.com/siddontang/go-log/log"
	es7 "github.com/olivere/elastic/v7"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/canal"
	"github.com/siddontang/go-mysql/replication"
)

type posSaver struct {
	pos   mysql.Position
	force bool
}

type ReqJson struct {
	data []byte
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
	schema  := e.Table.Schema
	table   := e.Table.Name
	columns := e.Table.Columns
	action  := e.Action
	message := make(map[string]interface{})

	if (h.b.isInTblFilter(schema+"."+table) != true) {
		return h.b.ctx.Err()
	}

	var values []interface{}
	if action == "insert" || action == "delete" {
		values = e.Rows[0]
	} else if (action == "update"){
		values = e.Rows[1]
	}

	body := make(map[string]string)
	for i := 0; i < len(columns); i++ {
		body[columns[i].Name] = toString(values[i])
	}
	message["schema"] = schema
	message["table"]  = table
	message["action"] = action
	message["body"]   = body

	data, err := json.Marshal(message)
	if err != nil {
		return errors.Trace(err)
	}

	h.b.syncCh <- ReqJson{data}

	return h.b.ctx.Err()
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
	defer func() {b.finish<-true}()
	defer b.wg.Done()

	log.Infof("begin to sync binlog to es")

	ticker := time.NewTicker(time.Duration(b.c.Es.FlushDuration) * time.Millisecond)
	defer ticker.Stop()

	lastSavedTime := time.Now()
	row := make(map[string]interface{})
	var pos mysql.Position
	var err error
	var data []byte
	for {
		needPipe := false
		needFlush := false
		needSavePos := false
		select {
		case v := <-b.syncCh:
			switch v := v.(type) {
			case posSaver:
				now := time.Now()
				if v.force || now.Sub(lastSavedTime) > 3*time.Second {
					lastSavedTime = now
					needFlush = true
					needSavePos = true
					pos = v.pos
				}
			case ReqJson:
				data = v.data
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
			if err = json.Unmarshal(data, &row); err != nil {
				log.Errorf("json decode error, err:%v", err)
				b.cancel()
				return
			}

			if err = b.Pipeline(row); err != nil {
				log.Errorf("pipeline exc error, err:%v", err)
				b.cancel()
				return
			}

			if b.esCli.BulkService.NumberOfActions() >= b.c.Es.BulkSize {
				needFlush = true
			}
		}

		if needFlush && b.esCli.BulkService.NumberOfActions() > 0 {
			_, err = b.esCli.BulkService.Do(context.TODO())
			if err != nil {
				log.Errorf("BulkService Do error, err:%v", err)
				b.cancel()
				return
			}
		}

		if needSavePos {
			if err = b.master.Save(pos); err != nil {
				log.Errorf("save sync position %s err %v, close sync", pos, err)
				b.cancel()
				return
			}
		}
	}

	return
}

func (b *Bin2es) Pipeline(row map[string]interface{}) error {
	
	schema := row["schema"].(string)
	table  := row["table"].(string)
	action := row["action"].(string)

	confs := b.event2Pipe[strings.Join([]string{schema, table, action}, "_")]
	for _, conf := range confs {
		
		Rows := []map[string]interface{}{row}

		for _, Pipeline := range conf.Pipelines {
			for funcName, funcArgs := range Pipeline {

				TmpRows := make([]map[string]interface{}, 0)
				
				for _, Row := range Rows {

					Args := []reflect.Value{reflect.ValueOf(Row), reflect.ValueOf(funcArgs.(map[string]interface{}))}

					RetValues := b.refFuncMap[funcName].Call(Args)

					NewRows := RetValues[0].Interface().(ROWS)
					if len(NewRows) == 0 {
						return nil
					}
					TmpRows = append(TmpRows, NewRows...)

					if !RetValues[1].IsNil() {
						if err := RetValues[1].Interface().(error); err != nil {
							return errors.Trace(err)
						}
					}
				}

				Rows = TmpRows
			}
		}

		switch action {
		case "insert":
			for _, row := range Rows {
				request := es7.NewBulkIndexRequest().Index(conf.Dest.Index).Id(row["_id"].(string)).Doc(row)
				b.esCli.BulkService.Add(request).Refresh("true")
			}
		case "update":
			for _, row := range Rows {
				request := es7.NewBulkUpdateRequest().Index(conf.Dest.Index).Id(row["_id"].(string)).Doc(row).DocAsUpsert(true)
				b.esCli.BulkService.Add(request).Refresh("true")
			}
		case "delete":
			for _, row := range Rows {
				request := es7.NewBulkDeleteRequest().Index(conf.Dest.Index).Id(row["_id"].(string))
				b.esCli.BulkService.Add(request).Refresh("true")
			}
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