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
func CacheConnect(host string) (err error) {
	Cdb.conn.Addr = host

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
			Key: key,
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
			Key: key,
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
			Key: k,
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
			Key: k,
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
