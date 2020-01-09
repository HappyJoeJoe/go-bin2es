package bin2es

import (
	"strings"
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
)

type reflectFunc struct {
	b *Bin2es
}

/* fuction: 执行sql, 并将`SQL`的`?`自动替换为感兴趣表的主键id
 * eg: 若遇到了`Parent`表, 则自动将`?`替换为
 *     Parent.id = xx
 */
func (r reflectFunc) PkDoSQL(row map[string]interface{}, SQL string, replaces []Replace) (ROWS, error) {
	rows := make(ROWS, 0)

	if SQL == "" {
		rows = append(rows, row)
		return rows, nil
	}

	schema 	:= row["schema"].(string)
	table  	:= row["table"].(string)
	body  	:= row["body"].(map[string]interface{})

	mapping := make(map[string]string)
	var replaceStr string
	for _, replace := range replaces {
		for tblStr, fieldStr := range replace {
			mapping[tblStr] = fieldStr
		}
	}

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
			if col == nil {
				value = "NULL"
			} else {
				value = string(col)
			}
			row_[columns[i]] = value
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
 * fields 表示sql映射到es上的键值对
 */
func (r reflectFunc) NestedObj(row map[string]interface{}, Common string, fields []Field) (ROWS, error) {
	
	rows := make(ROWS, 0)

	//参数校验
	if Common == "" || fields == nil {
		rows = append(rows, row)
		return rows, nil
	}

	common := make(map[string]interface{})
	for _, map_field := range fields {
		for sql_name, es_name := range map_field {
			if row[sql_name] != nil {
				common[es_name] = row[sql_name]
				delete(row, sql_name)
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
 * pos2Fields 表示最终映射到es上的[键:值]对
 *     键: 表示es上的数组对象的key
 *     值: 表示行记录的`SQLField`字段被','解析的字符串数组的每个值, 该值又继续被'_'解析的字符串数组的对应的索引
 * eg: '68_3,94_3,94_3'
 *     被解析为: [[68, 3], [94, 3], [94, 3]]
 *     其中`68`对应的位置是`1`, 而`3`对应的位置是`2`
 */
func (r reflectFunc) NestedArray(row map[string]interface{}, SQLField string, Common string, pos2Fields []Pos2Field) (ROWS, error) {

	rows := make(ROWS, 0)
	//参数校验
	if SQLField == "" || row[SQLField] == "" || Common == "" || pos2Fields == nil {
		rows = append(rows, row)
		return rows, nil
	}
	//参数校验
	if _, ok := row[SQLField]; !ok {
		rows = append(rows, row)
		return rows, nil
	}

	toSplitFields := strings.Split(row[SQLField].(string), ",")

	resFields := make([][]string, 0)
	for _, field := range toSplitFields {
		res := strings.Split(field, "_")
		resFields = append(resFields, res)
	}

	common := make([]map[string]string, 0)
	
	for _, res := range resFields {
		obj := make(map[string]string)
		for _, config := range pos2Fields {
			for es_name, sql_pos := range config {
				obj[es_name] = res[sql_pos-1]
			}
		}
		common = append(common, obj)
	}

	delete(row, SQLField)
	row[Common] = common
	rows = append(rows, row)

	return rows, nil
}

/* function: 用户自定义函数
 * row:  参数row必传, 表示每一个handler处理过的行数据
 * ROWS: 表示经处理过的行数据, 可以是多行, 比如: 经`PkDoSQL`处理过后变成了一行或多行数据
 */
func (r reflectFunc) UserDefinedFunc(row map[string]interface{}, Args ...interface{}) (ROWS, error) {
	return nil, nil
}