package cdb

import (
	"log"
	"regexp"
	"unsafe"

	"github.com/jmoiron/sqlx"
)

var (
	paramObjCleanReg map[string]*regexp.Regexp
	tcpSocketReg     *regexp.Regexp
)

func init() {
	tcpSocketReg = regexp.MustCompile("[^:]:[0-9]")

	paramObjCleanReg = map[string]*regexp.Regexp{
		"fields": regexp.MustCompile("[^0-9a-zA-Z_,.*]"),
		"table":  regexp.MustCompile("[^0-9a-zA-Z_,.]"),
		"where":  regexp.MustCompile("[^0-9a-zA-Z_,.() \n\r\t=$?<>*+-:]"),
		"group":  regexp.MustCompile("[^0-9a-zA-Z_,.() \n\r\t]"),
		"order":  regexp.MustCompile("[^0-9a-zA-Z_,.() \n\r\t]"),
		"limit":  regexp.MustCompile("[^0-9]"),
		"offset": regexp.MustCompile("[^0-9]"),
	}
}

// Parent - Объект структуры для объектов
type Parent struct {
	DBParentObjInited bool                   `json:"-"`
	DBParentObjTx     unsafe.Pointer         `json:"-"`
	DBParentInitValue map[string]interface{} `json:"-"`
}

// InitConnect - Объект конекта к базе
type InitConnect struct {
	Login    string
	Password string
	Socket   string
	DBName   string
	Charset  string
	SSL      SSLConf
}

// SSLConf - Объект конфига SSL
type SSLConf struct {
	SSLMode     string
	SSLCert     string
	SSLKey      string
	SSLRootCert string
}

// ParamObj - Объект инициализации структуры
type ParamObj struct {
	PK          interface{}
	Tx          *sqlx.Tx
	Table       string
	OrderBy     string
	Limit       string
	Offset      string
	GroupBy     string
	Where       string
	Fields      string
	CondEntries []string
	Values      []interface{}
}

func (o *ParamObj) clean() {
	if o == nil {
		return
	}

	for k, v := range paramObjCleanReg {
		switch k {
		case "fields":
			t := v.ReplaceAllString(o.Fields, "")
			if o.Fields != t {
				log.Println("[info]", k, t, o.Fields)
			}
			o.Fields = t
		case "table":
			t := v.ReplaceAllString(o.Table, "")
			if o.Table != t {
				log.Println("[info]", k, t, o.Table)
			}
			o.Table = t
		case "where":
			t := v.ReplaceAllString(o.Where, "")
			if o.Where != t {
				log.Println("[info]", k, t, o.Where)
			}
			o.Where = t
		case "group":
			t := v.ReplaceAllString(o.GroupBy, "")
			if o.GroupBy != t {
				log.Println("[info]", k, t, o.GroupBy)
			}
			o.GroupBy = t
		case "order":
			t := v.ReplaceAllString(o.OrderBy, "")
			if o.OrderBy != t {
				log.Println("[info]", k, t, o.OrderBy)
			}
			o.OrderBy = t
		case "limit":
			t := v.ReplaceAllString(o.Limit, "")
			if o.Limit != t {
				log.Println("[info]", k, t, o.Limit)
			}
			o.Limit = t
		case "offset":
			t := v.ReplaceAllString(o.Offset, "")
			if o.Offset != t {
				log.Println("[info]", k, t, o.Offset)
			}
			o.Offset = t
		}
	}
}
