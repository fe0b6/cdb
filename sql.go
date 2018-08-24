package cdb

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"reflect"
	"strings"
	"unsafe"

	"github.com/fe0b6/tools"

	"github.com/jmoiron/sqlx"
	// Подключаем драйвер postgress
	_ "github.com/lib/pq"
)

var (
	// Dbh - хэндлер базы
	Dbh *sqlx.DB
)

// Connect - Подключание к базе
func Connect(o InitConnect) {
	var err error

	// Кодировка по умолчанию
	if o.Charset == "" {
		o.Charset = "utf8mb4"
	}

	// Конфиг ssl
	var ssl string
	if o.SSL.SSLKey != "" {
		if o.SSL.SSLMode == "" {
			o.SSL.SSLMode = "verify-full"
		}

		ssl = fmt.Sprintf("&sslmode=%s&sslcert=%s&sslkey=%s&sslrootcert=%s",
			o.SSL.SSLMode, o.SSL.SSLCert, o.SSL.SSLKey, o.SSL.SSLRootCert)
	}

	// Коннефкт к базе
	Dbh, err = sqlx.Open("postgres", fmt.Sprintf("postgres://%s:%s@%s/%s?charset=%s%s", o.Login,
		o.Password, o.Socket, o.DBName, o.Charset, ssl))
	if err != nil {
		log.Fatalln("[fatal]", err)
		return
	}
	err = Dbh.Ping()
	if err != nil {
		log.Fatalln("[fatal]", err)
		return
	}
}

// Get - Получаем одну запись
func Get(i interface{}, o *ParamObj) (err error) {
	o.Limit = "1"

	rows, err := dbRq(i, o)
	if rows != nil {
		defer rows.Close()
	}
	if err != nil {
		log.Println("[error]", err)
		return
	}

	var find bool
	for rows.Next() {
		err = rows.StructScan(i)
		if err != nil {
			log.Println("[error]", err)
			return
		}
		find = true
	}

	if !find {
		err = errors.New("sql: no rows in result set")
		return
	}

	_setInitedData(i, o)

	return
}

// ForeachItem - Получаем список объектов
func ForeachItem(i interface{}, o *ParamObj) (err error) {

	rows, err := dbRq(i, o)
	if err != nil {
		log.Println("[error]", err)
		return
	}
	if rows != nil {
		defer rows.Close()
	}

	dest := reflect.Indirect(reflect.ValueOf(i))

	for rows.Next() {
		el := reflect.New(dest.Type().Elem()).Interface()

		err = rows.StructScan(el)
		if err != nil {
			log.Println("[error]", err)
			return
		}
		_setInitedData(el, o)

		dest.Set(reflect.Append(dest, reflect.ValueOf(el).Elem()))
	}

	return
}

// ForeachFunc - Получаем список объектов
func ForeachFunc(i interface{}, o *ParamObj, f func(interface{})) (err error) {

	rows, err := dbRq(i, o)
	if err != nil {
		log.Println("[error]", err)
		return
	}
	if rows != nil {
		defer rows.Close()
	}

	dest := reflect.Indirect(reflect.ValueOf(i))

	for rows.Next() {
		el := reflect.New(dest.Type().Elem()).Interface()

		err = rows.StructScan(el)
		if err != nil {
			log.Println("[error]", err)
			return
		}

		_setInitedData(el, o)

		f(el)
	}

	return
}

// Set - Записываем объект в базу
func Set(i interface{}) (err error) {
	el := reflect.Indirect(reflect.ValueOf(i))
	tx := (*sqlx.Tx)(unsafe.Pointer(el.FieldByName("DBParentObjTx").Pointer()))

	tinfo := getTagInfo(i, "insert")

	inited := el.FieldByName("DBParentObjInited").Bool()

	var sqlrq string
	if inited && tinfo["pkey"] != "" {
		if tinfo["upvalues"] == "" {
			return
		}
		sqlrq = fmt.Sprintf("UPDATE %s SET %s WHERE %s=:%s", tinfo["sourcetable"],
			tinfo["upvalues"], tinfo["pkey"], tinfo["pkey"])

		if tx != nil {
			sqlrq += " RETURNING NOTHING"
		}
	} else if tinfo["pkey"] != "" {
		sqlrq = fmt.Sprintf("INSERT INTO %s(%s) VALUES(%s) RETURNING %s", tinfo["sourcetable"],
			tinfo["fields"], tinfo["values"], tinfo["pkey"])
	} else {
		log.Fatalln("bad set rq")
	}

	//	log.Println(sqlrq)
	//	log.Printf("%+v", i)

	var rows *sqlx.Rows
	if tx != nil {
		rows, err = tx.NamedQuery(sqlrq, i)
	} else {
		rows, err = Dbh.NamedQuery(sqlrq, i)
	}
	if err != nil {
		log.Println("[error]", err)
		log.Println("[error]", sqlrq)
		return
	}
	defer rows.Close()

	// Если это добавление нового объекта
	if !inited {
		var id int64
		for rows.Next() {
			err = rows.Scan(&id)
			if err != nil {
				log.Println("[error]", err)
				return
			}
		}

		if rows.Err() != nil {
			err = rows.Err()
			return
		}

		el := reflect.Indirect(reflect.ValueOf(i))
		el.FieldByName("ID").SetInt(id)
		el.FieldByName("DBParentObjInited").SetBool(true)
	}

	InitObj(i, nil)

	return
}

func _setInitedData(i interface{}, o *ParamObj) {
	el := reflect.Indirect(reflect.ValueOf(i))
	el.FieldByName("DBParentObjInited").SetBool(true)

	si := reflect.ValueOf(i).Type()
	if si.Kind() == reflect.Ptr {
		si = si.Elem()
	}
	if si.Kind() == reflect.Slice {
		si = si.Elem()
	}
	for k := 0; k < si.NumField(); k++ {
		if si.Field(k).Type.String() == "[]uint8" {
			//JSON
			jsonField := el.FieldByName(si.Field(k).Name + "JSON")
			if jsonField.IsValid() {
				ptr := reflect.New(reflect.Indirect(jsonField).Type())
				err := json.Unmarshal(el.FieldByName(si.Field(k).Name).Bytes(), ptr.Interface())
				if err != nil {
					if string(el.FieldByName(si.Field(k).Name).Bytes()) != "" {
						log.Println("[error]", err, string(el.FieldByName(si.Field(k).Name).Bytes()))
					}
				} else {
					jsonField.Set(ptr.Elem())
				}
			}

			// GOB
			gobField := el.FieldByName(si.Field(k).Name + "GOB")
			if gobField.IsValid() {
				ptr := reflect.New(reflect.Indirect(gobField).Type())
				tools.FromGob(ptr.Interface(), el.FieldByName(si.Field(k).Name).Bytes())
				gobField.Set(ptr.Elem())
			}
		}
	}

	InitObj(i, o)
}

// Delete - удаление записи
func Delete(i interface{}) (err error) {
	el := reflect.Indirect(reflect.ValueOf(i))
	tx := (*sqlx.Tx)(unsafe.Pointer(el.FieldByName("DBParentObjTx").Pointer()))

	table := getTableName(i)
	pkey := getPrimaryKey(i)

	sqlrq := fmt.Sprintf("DELETE FROM %s WHERE %s=:%s", table, pkey, pkey)

	if tx != nil {
		_, err = tx.NamedExec(sqlrq, i)
	} else {
		_, err = Dbh.NamedExec(sqlrq, i)
	}
	if err != nil {
		log.Println("[error]", err)
		return
	}

	return
}

func dbRq(i interface{}, o *ParamObj) (rows *sqlx.Rows, err error) {
	if o == nil {
		o = &ParamObj{}
	}

	sqlrq := ReplacePlaceholder(getSQLStr(i, o))

	if o.Tx != nil {
		rows, err = o.Tx.Queryx(sqlrq, o.Values...)
	} else {
		rows, err = Dbh.Queryx(sqlrq, o.Values...)
	}
	if err != nil {
		log.Println("[error]", sqlrq, err)
		return
	}

	return
}

func getSQLStr(i interface{}, o *ParamObj) (sqlrq string) {

	// Чистим параметры
	o.clean()

	// Если не указаны поля для выборки - подставляем все
	if o.Fields == "" {
		o.Fields = getFiledsString(i)
	}

	if o.Table == "" {
		o.Table = getTableName(i)
	}

	if o.Where == "" {
		if o.PK != nil {
			o.Where = getPrimaryKey(i) + "=?"
			o.Values = append(o.Values, o.PK)
		}
	}

	// строка запроса
	sqlrq = fmt.Sprintf(`SELECT %s FROM %s`, o.Fields, o.Table)

	if o.Where != "" {
		sqlrq += " WHERE " + o.Where
	}

	if o.GroupBy != "" {
		sqlrq += " GROUP BY " + o.GroupBy
	}

	if o.OrderBy != "" {
		sqlrq += " ORDER BY " + o.OrderBy
	}

	if o.Limit != "" {
		sqlrq += " LIMIT " + o.Limit
	}

	if o.Offset != "" {
		sqlrq += " OFFSET " + o.Offset
	}

	return
}

func getTableName(i interface{}) string {
	h := getTagInfo(i, "sourcetable")
	return h["sourcetable"]
}

func getPrimaryKey(i interface{}) string {
	h := getTagInfo(i, "pkey")
	return h["pkey"]
}

// Получаем поля для выборки из базы
func getFiledsString(i interface{}) string {
	h := getTagInfo(i, "fields")
	return h["fields"]
}

// Получаем поля для выборки из базы
func getFiledsAndValues(i interface{}) (string, string) {
	h := getTagInfo(i, "fields")
	return h["fields"], h["values"]
}

// Получаем поля для выборки из базы
func getTagInfo(i interface{}, t string) map[string]string {
	si := reflect.ValueOf(i).Type()
	vi := reflect.ValueOf(i)

	if si.Kind() == reflect.Ptr {
		si = si.Elem()
		vi = vi.Elem()
	}
	if si.Kind() == reflect.Slice {
		si = si.Elem()
	}

	// Смотрим какие переменные были изменены
	changedValues := map[string]bool{}
	if t == "insert" {
		v := vi.FieldByName("DBParentInitValue")

		th := map[string]interface{}{}
		for _, key := range v.MapKeys() {
			strct := v.MapIndex(key)
			th[key.Interface().(string)] = strct.Interface()
		}

		for k := 0; k < si.NumField(); k++ {
			field := si.Field(k)
			name := field.Name

			if field.Type.String() == "[]uint8" {
				// Проверяем есть ли json для этой переменной
				jsonField := vi.FieldByName(name + "JSON")
				if jsonField.IsValid() {
					b, err := json.Marshal(jsonField.Interface())
					if err != nil {
						log.Println("[error]", err)
					}
					vi.FieldByName(name).SetBytes(b)
				}

				// Проверяем есть ли gob для этой переменной
				gobField := vi.FieldByName(name + "GOB")
				if gobField.IsValid() {
					b := tools.ToGob(gobField.Interface())
					vi.FieldByName(name).SetBytes(b)
				}
			}

			v, ok := th[name]
			if !ok {
				continue
			}

			if !reflect.DeepEqual(v, vi.Field(k).Interface()) {
				changedValues[name] = true
			}
		}
	}

	// Собираем данные
	fields := []string{}
	values := []string{}
	upvalues := []string{}
	var pkey, table string
	for k := 0; k < si.NumField(); k++ {
		field := si.Field(k)
		f := field.Tag.Get("db")
		pk := field.Tag.Get("pk")

		if pk == "true" {
			pkey = f
			if t == "pkey" {
				break
			}
		}

		st := field.Tag.Get("sourcetable")
		if st != "" {
			table = st
			if t == "sourcetable" {
				break
			}
		}

		if f != "" && f != "-" {
			if t == "insert" && pk == "true" {
				continue
			}
			if t == "insert" && !changedValues[field.Name] {
				continue
			}

			fields = append(fields, f)
			values = append(values, ":"+f)
			upvalues = append(upvalues, f+"=:"+f)
		}

	}

	h := map[string]string{
		"sourcetable": table,
		"pkey":        pkey,
		"fields":      strings.Join(fields, ","),
		"values":      strings.Join(values, ","),
		"upvalues":    strings.Join(upvalues, ","),
	}

	return h
}
