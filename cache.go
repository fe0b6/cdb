package cdb

import (
	"errors"
	"log"
	"time"

	"github.com/fe0b6/ramnet"
	"github.com/fe0b6/ramstore"
	"github.com/fe0b6/tools"
)

var (
	// Cdb - хэндлер кэша
	Cdb CacheObj
)

// CacheConnect - коннект к кэщу
func CacheConnect(o InitCacheConnect) (err error) {
	Cdb.conn.Addr = o.Host
	Cdb.prefix = o.Prefix

	err = Cdb.conn.Connet()
	if err != nil {
		log.Fatalln("[fatal]", err)
		return
	}

	return
}

// Get - Получаем объект из кэша
func (c *CacheObj) Get(key string) (obj ramstore.Obj, err error) {
	err = Cdb.conn.Send(ramnet.Rqdata{
		Action: "get",
		Data: tools.ToGob(ramnet.RqdataGet{
			Key: Cdb.prefix + key,
		}),
	})

	if err != nil {
		log.Println("[error]", err)
		return
	}

	var ans ramnet.Ansdata
	Cdb.conn.Gr.Decode(&ans)

	if ans.Error != "" {
		err = errors.New(ans.Error)
		return
	}

	obj = ans.Obj
	return
}

// GetObj - Получаем объект из кэша и сразу его преобразовываем
func (c *CacheObj) GetObj(key string, i interface{}) (err error) {
	err = Cdb.conn.Send(ramnet.Rqdata{
		Action: "get",
		Data: tools.ToGob(ramnet.RqdataGet{
			Key: Cdb.prefix + key,
		}),
	})

	if err != nil {
		log.Println("[error]", err)
		return
	}

	var ans ramnet.Ansdata
	Cdb.conn.Gr.Decode(&ans)

	if ans.Error != "" {
		err = errors.New(ans.Error)
		return
	}

	tools.FromGob(i, ans.Obj.Data)
	return
}

// SetObj - добавляем объект в кэш
func (c *CacheObj) SetObj(key string, i interface{}) (err error) {
	return c.SetObjEx(key, i, 0)
}

// SetObjEx - добавляем объект в кэш с указанием времени истечения
func (c *CacheObj) SetObjEx(key string, i interface{}, ex int) (err error) {
	data := tools.ToGob(i)
	return c.SetEx(key, data, ex)
}

// Set - добавляем объект в кэш
func (c *CacheObj) Set(key string, data []byte) (err error) {
	return c.SetEx(key, data, 0)
}

// SetStr - добавляем объект в кэш (строки)
func (c *CacheObj) SetStr(key string, data string) (err error) {
	return c.Set(key, []byte(data))
}

// SetEx - добавление объекта со сроком жизни
func (c *CacheObj) SetEx(key string, data []byte, ex int) (err error) {

	tnu := time.Now().Unix()

	if ex > 0 {
		ex += int(tnu)
	}

	err = Cdb.conn.Send(ramnet.Rqdata{
		Action: "set",
		Data: tools.ToGob(ramnet.RqdataSet{
			Key: Cdb.prefix + key,
			Obj: ramstore.Obj{
				Data:   data,
				Time:   tnu,
				Expire: ex,
			},
		}),
	})

	if err != nil {
		log.Println("[error]", err)
		return
	}

	var ans ramnet.Ansdata
	Cdb.conn.Gr.Decode(&ans)

	if ans.Error != "" {
		err = errors.New(ans.Error)
		return
	}

	return
}

// SetExStr - добавление строки со сроком жизни
func (c *CacheObj) SetExStr(key string, data string, ex int) (err error) {
	return c.SetEx(key, []byte(data), ex)
}

// MultiSet - массовое добавление объектов
func (c *CacheObj) MultiSet(h map[string][]byte) (err error) {

	tnu := time.Now().Unix()

	d := []ramnet.RqdataSet{}
	for k, v := range h {
		d = append(d, ramnet.RqdataSet{
			Key: Cdb.prefix + k,
			Obj: ramstore.Obj{
				Data: v,
				Time: tnu,
			},
		})
	}

	err = Cdb.conn.Send(ramnet.Rqdata{
		Action: "multi_set",
		Data:   tools.ToGob(d),
	})

	if err != nil {
		log.Println("[error]", err)
		return
	}

	var ans ramnet.Ansdata
	Cdb.conn.Gr.Decode(&ans)

	if ans.Error != "" {
		err = errors.New(ans.Error)
		return
	}

	return
}

// MultiGet - получаем список объектов из кэша
func (c *CacheObj) MultiGet(keys []string) (h map[string]ramstore.Obj, err error) {
	h = make(map[string]ramstore.Obj)
	err = c.MultiGetFunc(keys, func(k string, o ramstore.Obj) {
		h[k] = o
	})
	return
}

// MultiGetFunc - получаем список объектов из кэша в функцию
func (c *CacheObj) MultiGetFunc(keys []string, f func(string, ramstore.Obj)) (err error) {

	d := []ramnet.RqdataGet{}
	for _, k := range keys {
		d = append(d, ramnet.RqdataGet{
			Key: Cdb.prefix + k,
		})
	}

	err = Cdb.conn.Send(ramnet.Rqdata{
		Action: "multi_get",
		Data:   tools.ToGob(d),
	})

	if err != nil {
		log.Println("[error]", err)
		return
	}

	for {
		var ans ramnet.Ansdata
		Cdb.conn.Gr.Decode(&ans)
		if ans.Error != "" {
			err = errors.New(ans.Error)
			return
		}

		if ans.EOF {
			break
		}

		f(ans.Key, ans.Obj)
	}

	return
}

// Search - поиск ключей на соответствие
func (c *CacheObj) Search(q string, f func(string, ramstore.Obj)) (err error) {

	err = Cdb.conn.Send(ramnet.Rqdata{
		Action: "search",
		Data: tools.ToGob(ramnet.RqdataGet{
			Key: Cdb.prefix + q,
		}),
	})

	if err != nil {
		log.Println("[error]", err)
		return
	}

	for {
		var ans ramnet.Ansdata
		Cdb.conn.Gr.Decode(&ans)
		if ans.Error != "" {
			err = errors.New(ans.Error)
			return
		}

		if ans.EOF {
			break
		}

		log.Println(ans.Key, ans.EOF)

		f(ans.Key, ans.Obj)
	}

	return
}

// Del - Удаляем объект
func (c *CacheObj) Del(key string) (err error) {

	err = Cdb.conn.Send(ramnet.Rqdata{
		Action: "del",
		Data: tools.ToGob(ramnet.RqdataSet{
			Key: Cdb.prefix + key,
		}),
	})

	if err != nil {
		log.Println("[error]", err)
		return
	}

	var ans ramnet.Ansdata
	Cdb.conn.Gr.Decode(&ans)

	if ans.Error != "" {
		err = errors.New(ans.Error)
		return
	}

	return
}
