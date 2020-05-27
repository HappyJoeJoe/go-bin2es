package bin2es

import (
	//系统
	"fmt"
	"strings"
	//第三方
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/juju/errors"
	"github.com/siddontang/go-log/log"
)

type reflectFunc struct {
	b *Bin2es
}

/* fuction: 执行sql, 并将`SQL`的`?`自动替换为感兴趣表的主键id
 * eg: 若遇到了`Parent`表, 则自动将`?`替换为
 *     Parent.id = xx
 * 此函数一般用于Pipeline的第一个处理函数
 */
func (r reflectFunc) DoSQL(row map[string]interface{}, funcArgs map[string]interface{}) (ROWS, error) {
	rows := make(ROWS, 0)

	// 参数
	querySQL := funcArgs["sql"].(string)
	if querySQL == "" {
		return nil, errors.New("querySQL should not be empty")
	}

	schema := row["schema"].(string)
	table := row["table"].(string)
	body := row["body"].(map[string]string)
	db := r.b.sqlPool[schema]

	placeHolders := funcArgs["placeholders"].(map[string]interface{})
	key := placeHolders[table].(string)

	placeHolderStr := fmt.Sprintf("%s.%s = %s", table, key, body[key])
	querySQL = strings.Replace(querySQL, "?", placeHolderStr, -1)
	esRows, err := db.Query(querySQL)
	if err != nil {
		log.Errorf("querySQL:[%s] execute failed, err:%s", querySQL, errors.Trace(err))
		return nil, errors.Trace(err)
	}
	defer esRows.Close()

	columns, err := esRows.Columns()
	if err != nil {
		log.Errorf("get columns failed, err:%s", errors.Trace(err))
		return nil, errors.Trace(err)
	}

	values := make([]sql.RawBytes, len(columns))
	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	for esRows.Next() {
		// get RawBytes from data
		err = esRows.Scan(scanArgs...)
		if err != nil {
			log.Errorf("scan failed, err:%s", errors.Trace(err))
			return nil, errors.Trace(err)
		}

		esRow := make(map[string]interface{})
		for i, col := range values {
			//此时行数据该字段如果为`null`则忽略
			if col != nil {
				esRow[columns[i]] = string(col)
			}
		}

		rows = append(rows, esRow)
	}

	if err = esRows.Err(); err != nil {
		log.Errorf("query Err:%s", errors.Trace(err))
		return nil, errors.Trace(err)
	}

	return rows, nil
}

/* fuction: 处理对象类型
 * common 表示最终映射到es上的object名字
 * fields 表示sql映射到es上的[键:值]对
 */
func (r reflectFunc) Object(row map[string]interface{}, funcArgs map[string]interface{}) (ROWS, error) {
	rows := make(ROWS, 0)

	//参数
	common := funcArgs["common"].(string)
	fields := funcArgs["fields"].(map[string]interface{})

	//参数校验
	if common == "" || fields == nil || len(fields) == 0 {
		return nil, errors.Errorf("params invalid, common:%+v fields:%+v", common, fields)
	}

	object := make(map[string]interface{})
	for sqlName, esName := range fields {
		if sqlName == "" || esName == nil || esName.(string) == "" {
			return nil, errors.Errorf("fields invalid, fields:%+v", fields)
		}
		//此时不用强行要求`row[sqlName]`不得为空字符串, 给业务层面预留更大的空间
		if row[sqlName] != nil /* && row[SQLName].(string) != "" */ {
			object[esName.(string)] = row[sqlName]
		}
		delete(row, sqlName)
	}

	row[common] = object
	rows = append(rows, row)

	return rows, nil
}

/* fuction: 处理嵌套数组
 * sqlField 表示要解析的sql字段
 * common 表示最终映射到es上的object名字
 * pos2Fields 表示最终映射到es上的[键:值]对
 *     键: 表示es上的数组对象的key
 *     值: 表示行记录的`sqlField`字段被','解析的字符串数组的每个值, 该值又继续被'_'解析的字符串数组的对应的索引
 * fieldsSeprator 表示被连接字段之间的分隔符
 * groupSeprator  表示组分隔符
 * eg: '68_3,94_3,94_3'
 *     被解析为: [[68, 3], [94, 3], [94, 3]]
 *     其中`68`对应的位置是`1`, 而`3`对应的位置是`2`
 */
func (r reflectFunc) NestedArray(row map[string]interface{}, funcArgs map[string]interface{}) (ROWS, error) {
	rows := make(ROWS, 0)

	//参数
	sqlField := funcArgs["sql_field"].(string)
	common := funcArgs["common"].(string)
	pos2Fields := funcArgs["pos2fields"].(map[string]interface{})
	groupSeprator := funcArgs["group_seprator"].(string)
	fieldsSeprator := funcArgs["fields_seprator"].(string)

	//参数校验
	if sqlField == "" || common == "" || pos2Fields == nil || len(pos2Fields) == 0 || fieldsSeprator == "" || groupSeprator == "" {
		return nil, errors.Errorf("Params invalid, sqlField:%+v common:%+v pos2Fields:%+v fieldsSeprator:%+v groupSeprator:%+v row:%+v", sqlField, common, pos2Fields, fieldsSeprator, groupSeprator, row)
	}

	if row[sqlField] == nil || row[sqlField].(string) == "" {
		delete(row, sqlField)
		rows = append(rows, row)
		return rows, nil
	}

	toSplitFields := strings.Split(row[sqlField].(string), groupSeprator)

	resFields := make([][]string, 0)
	for _, field := range toSplitFields {
		res := strings.Split(field, fieldsSeprator)
		resFields = append(resFields, res)
	}

	objList := make([]map[string]string, 0)
	for _, res := range resFields {
		obj := make(map[string]string)
		for esName, sqlPos := range pos2Fields {
			if esName == "" || sqlPos == nil || uint64(sqlPos.(float64)) == 0 {
				return nil, errors.Errorf("resFields invalid, resFields:%+v", resFields)
			}
			obj[esName] = res[uint64(sqlPos.(float64))-1]
		}
		objList = append(objList, obj)
	}

	delete(row, sqlField)
	row[common] = objList
	rows = append(rows, row)

	return rows, nil
}

/* fuction: 用于设置elasticsearch的文档ID
 * docID 表示行数据里用于设置文档ID的字段
 */
func (r reflectFunc) SetDocID(row map[string]interface{}, funcArgs map[string]interface{}) (ROWS, error) {
	rows := make(ROWS, 0)

	//参数
	docID := funcArgs["doc_id"].(string)
	//参数校验
	if docID == "" || docID == "_id" || row[docID] == nil || row[docID] == "" || row["_id"] != nil {
		return nil, errors.Errorf("docID invalid, docID:%+v row:%+v", docID, row)
	}

	row["_id"] = row[docID].(string)

	rows = append(rows, row)

	return rows, nil
}

/* function: 用户自定义函数
 * row:  参数row必传, 表示每一个handler处理过的行数据
 * ROWS: 表示经处理过的行数据, 可以是多行, 比如: 经`DoSQL`处理过后变成了一行或多行数据
 */
func (r reflectFunc) UserDefinedFunc(row map[string]interface{}, funcArgs map[string]interface{}) (ROWS, error) {
	rows := make(ROWS, 0)
	rows = append(rows, row)

	//todo

	return rows, nil
}
