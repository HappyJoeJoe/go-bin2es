package bin2es

import (
	"strings"

	"github.com/juju/errors"
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
)

type reflectFunc struct {
	b *Bin2es
}

/* fuction: 执行sql, 并将`SQL`的`?`自动替换为感兴趣表的主键id
 * eg: 若遇到了`Parent`表, 则自动将`?`替换为
 *     Parent.id = xx
 * 此函数一般用于Pipeline的第一个处理函数
 */
func (r reflectFunc) PkDoSQL(row map[string]interface{}, funcArgs map[string]interface{}) (ROWS, error) {
	rows := make(ROWS, 0)

	// 参数
	SQL := funcArgs["sql"].(string)
	if SQL == "" {
		rows = append(rows, row)
		return nil, errors.New("sql should not be empty")
	}

	mapping := make(map[string]string)
	// 参数
	Replaces := funcArgs["replaces"].([]interface{})
	for _, Replace := range Replaces {
		for tblStr, fieldStr := range Replace.(map[string]interface{}) {
			if tblStr == "" || fieldStr == nil || fieldStr.(string) == "" {
				rows = append(rows, row)
				return nil, errors.New("replaces invalid")
			}
			mapping[tblStr] = fieldStr.(string)
		}
	}

	schema := row["schema"].(string)
	table  := row["table"].(string)
	body   := row["body"].(map[string]interface{})

	var replaceStr string
	replaceStr = strings.Join([]string{table, mapping[table]}, ".")
	replaceStr = strings.Join([]string{replaceStr, body[mapping[table]].(string)}, " = ")

	SQL = strings.Replace(SQL, "?", replaceStr, -1)

	db := r.b.sqlPool[schema]
	db_rows, err := db.Query(SQL)
	if err != nil {
		return nil, err
	}

	columns, err := db_rows.Columns()
	if err != nil {
		return nil, err
	}

	values := make([]sql.RawBytes, len(columns))

	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	for db_rows.Next() {
		// get RawBytes from data
		err = db_rows.Scan(scanArgs...)
		if err != nil {
			return nil, err
		}

		var value string
		row_ := make(map[string]interface{}) 
		for i, col := range values {
			if col != nil {
				value = string(col)
				row_[columns[i]] = value
			}
		}
		rows = append(rows, row_)
	}
	if err = db_rows.Err(); err != nil {
		return nil, err
	}

	return rows, nil
}

/* fuction: 处理嵌套对象
 * Common 表示最终映射到es上的object名字
 * Fields 表示sql映射到es上的键值对
 */
func (r reflectFunc) NestedObj(row map[string]interface{}, funcArgs map[string]interface{}) (ROWS, error) {
	rows := make(ROWS, 0)

	//参数
	Common := funcArgs["common"].(string)
	Fields := funcArgs["fields"].([]interface{})

	//参数校验
	if Common == "" || Fields == nil || len(Fields) == 0 {
		rows = append(rows, row)
		return nil, errors.New("params invalid")
	}

	common := make(map[string]interface{})
	for _, MapField := range Fields {
		for SQLName, ESName := range MapField.(map[string]interface{}) {
			if SQLName == "" || ESName == nil || ESName.(string) == "" {
				rows = append(rows, row)
				return nil, errors.New("Fields invalid")
			}
			if row[SQLName] != nil {
				common[ESName.(string)] = row[SQLName]
				delete(row, SQLName)
			}
		}
	}

	row[Common] = common
	rows = append(rows, row)

	return rows, nil
}

/* fuction: 处理嵌套数组
 * SQLField 表示要解析的sql字段
 * Common 表示最终映射到es上的object名字
 * Pos2Fields 表示最终映射到es上的[键:值]对
 *     键: 表示es上的数组对象的key
 *     值: 表示行记录的`SQLField`字段被','解析的字符串数组的每个值, 该值又继续被'_'解析的字符串数组的对应的索引
 * FieldsSeprator 表示被连接字段之间的分隔符
 * GroupSeprator  表示组分隔符
 * eg: '68_3,94_3,94_3'
 *     被解析为: [[68, 3], [94, 3], [94, 3]]
 *     其中`68`对应的位置是`1`, 而`3`对应的位置是`2`
 */
func (r reflectFunc) NestedArray(row map[string]interface{}, funcArgs map[string]interface{}) (ROWS, error) {
	rows := make(ROWS, 0)

	//参数
	SQLField       := funcArgs["sql_field"].(string)
	Common         := funcArgs["common"].(string)
	Pos2Fields     := funcArgs["pos2fields"].([]interface{})
	GroupSeprator  := funcArgs["group_seprator"].(string)
	FieldsSeprator := funcArgs["fields_seprator"].(string)

	//参数校验
	if SQLField == "" || row[SQLField] == nil || row[SQLField] == "" || Common == "" || Pos2Fields == nil || len(Pos2Fields) == 0 || FieldsSeprator == "" || GroupSeprator == "" {
		rows = append(rows, row)
		return nil, errors.New("params invalid")
	}

	toSplitFields := strings.Split(row[SQLField].(string), GroupSeprator)

	resFields := make([][]string, 0)
	for _, field := range toSplitFields {
		res := strings.Split(field, FieldsSeprator)
		resFields = append(resFields, res)
	}

	common := make([]map[string]string, 0)
	for _, res := range resFields {
		obj := make(map[string]string)
		for _, MapField := range Pos2Fields {
			for ESName, SQLPos := range MapField.(map[string]interface{}) {
				if ESName == "" || SQLPos == nil || uint64(SQLPos.(float64)) == 0 {
					rows = append(rows, row)
					return nil, errors.New("params invalid")
				}
				obj[ESName] = res[uint64(SQLPos.(float64))-1]
			}
		}
		common = append(common, obj)
	}

	delete(row, SQLField)
	row[Common] = common
	rows = append(rows, row)

	return rows, nil
}

/* fuction: 用于设置elasticsearch的文档ID
 * DocID 表示行数据里用于设置文档ID的字段
 */
func (r reflectFunc) SetDocID(row map[string]interface{}, funcArgs map[string]interface{}) (ROWS, error) {
	rows := make(ROWS, 0)

	//参数
	DocID := funcArgs["doc_id"].(string)
	//参数校验
	if DocID == "" || DocID == "_id" || row[DocID] == nil || row[DocID] == "" || row["_id"] != nil {
		rows = append(rows, row)
		return nil, errors.New("DocID invalid")
	}

	row["id"] = row[DocID].(string)
	
	rows = append(rows, row)

	return rows, nil
}

/* function: 用户自定义函数
 * row:  参数row必传, 表示每一个handler处理过的行数据
 * ROWS: 表示经处理过的行数据, 可以是多行, 比如: 经`PkDoSQL`处理过后变成了一行或多行数据
 */
func (r reflectFunc) UserDefinedFunc(row map[string]interface{}, funcArgs map[string]interface{}) (ROWS, error) {
	rows := make(ROWS, 0)
	rows = append(rows, row)
	
	//todo

	return rows, nil
}