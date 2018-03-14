package cdb

import (
	"reflect"
)

// InitObj - Инициализация объекта
func InitObj(i interface{}) {
	si := reflect.ValueOf(i).Type()
	vi := reflect.ValueOf(i)
	if si.Kind() == reflect.Ptr {
		si = si.Elem()
		vi = vi.Elem()
	}
	if si.Kind() == reflect.Slice {
		si = si.Elem()
	}

	h := make(map[string]interface{})

	for k := 0; k < si.NumField(); k++ {
		f := si.Field(k).Tag.Get("db")
		if f == "" || f == "-" {
			continue
		}

		h[si.Field(k).Name] = vi.Field(k).Interface()
	}

	el := reflect.Indirect(reflect.ValueOf(i))
	el.FieldByName("DBParentInitValue").Set(reflect.ValueOf(h))
}
