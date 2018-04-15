package cdb

import (
	"errors"
	"log"
	"strings"
	"time"

	"github.com/fe0b6/ramnet"
	"github.com/fe0b6/ramstore"
	"github.com/fe0b6/tools"
)

const cacheQueueSize = 100

var (
	// Cdb - хэндлер кэша
	Cdb CacheObj
)

// CacheConnect - коннект к кэщу
func CacheConnect(o InitCacheConnect) (err error) {
	Cdb.addr = o.Host
	Cdb.prefix = o.Prefix

	Cdb.connectQueue = make(chan *ramnet.ClientConn, cacheQueueSize)

	err = Cdb.createConnet(o.QueueStartSize)
	if err != nil {
		log.Println("[error]", err)
		return
	}

	return
}

func (c *CacheObj) createConnet(s int) (err error) {
	if s == 0 {
		s = 1
	}

	for i := 0; i < s; i++ {
		var conn *ramnet.ClientConn
		conn, err = c.initConnect()
		if err != nil {
			log.Println("[error]", err)
			return
		}

		c.pushConnect(conn)
	}

	return
}

func (c *CacheObj) initConnect() (conn *ramnet.ClientConn, err error) {
	conn = &ramnet.ClientConn{Addr: c.addr}

	err = conn.Connet()
	if err != nil {
		log.Println("[error]", err)
		return
	}

	return
}

func (c *CacheObj) pushConnect(conn *ramnet.ClientConn) {
	c.Lock()
	if len(c.connectQueue) < cacheQueueSize/2 {
		c.connectQueue <- conn
	}
	c.Unlock()
}

func (c *CacheObj) getConnect() (conn *ramnet.ClientConn, err error) {
	c.Lock()
	if len(c.connectQueue) > 0 {
		conn = <-c.connectQueue
		c.Unlock()
	} else {
		c.Unlock()

		conn, err = c.initConnect()
		if err != nil {
			log.Println("[error]", err)
			return
		}
	}

	return
}
func (c *CacheObj) readAns(conn *ramnet.ClientConn, to time.Duration, i interface{}) (err error) {
	err = conn.Conn.SetReadDeadline(time.Now().Add(to))
	if err != nil {
		log.Println("[error]", err)
		return
	}

	err = conn.Gr.Decode(i)
	if err != nil {
		log.Println("[error]", err)
		return
	}
	return
}

// Get - Получаем объект из кэша
func (c *CacheObj) Get(key string) (obj ramstore.Obj, err error) {
	conn, err := c.getConnect()
	if err != nil {
		log.Println("[error]", err)
		return
	}
	defer c.pushConnect(conn)

	key = c.setPrefix(key)

	err = conn.Send(ramnet.Rqdata{
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
	err = c.readAns(conn, ramnet.ConnectTimeout, &ans)
	if err != nil {
		log.Println("[error]", err)
		return
	}

	if ans.Error != "" {
		err = errors.New(ans.Error)
		return
	}

	obj = ans.Obj
	return
}

// GetObj - Получаем объект из кэша и сразу его преобразовываем
func (c *CacheObj) GetObj(key string, i interface{}) (err error) {
	obj, err := c.Get(key)
	if err != nil {
		if err.Error() != "key not found" {
			log.Println("[error]", err)
		}
		return
	}

	tools.FromGob(i, obj.Data)
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
	conn, err := c.getConnect()
	if err != nil {
		log.Println("[error]", err)
		return
	}
	defer c.pushConnect(conn)

	tnu := time.Now().Unix()

	if ex > 0 {
		ex += int(tnu)
	}

	key = c.setPrefix(key)

	err = conn.Send(ramnet.Rqdata{
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
	err = c.readAns(conn, ramnet.ConnectTimeout, &ans)
	if err != nil {
		log.Println("[error]", err)
		return
	}

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
	conn, err := c.getConnect()
	if err != nil {
		log.Println("[error]", err)
		return
	}
	defer c.pushConnect(conn)

	tnu := time.Now().Unix()

	d := []ramnet.RqdataSet{}
	for k, v := range h {
		k = c.setPrefix(k)

		d = append(d, ramnet.RqdataSet{
			Key: k,
			Obj: ramstore.Obj{
				Data: v,
				Time: tnu,
			},
		})
	}

	err = conn.Send(ramnet.Rqdata{
		Action: "multi_set",
		Data:   tools.ToGob(d),
	})

	if err != nil {
		log.Println("[error]", err)
		return
	}

	var ans ramnet.Ansdata
	err = c.readAns(conn, ramnet.ConnectTimeout, &ans)
	if err != nil {
		log.Println("[error]", err)
		return
	}

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
	conn, err := c.getConnect()
	if err != nil {
		log.Println("[error]", err)
		return
	}
	defer c.pushConnect(conn)

	d := []ramnet.RqdataGet{}
	for _, k := range keys {
		k = c.setPrefix(k)

		d = append(d, ramnet.RqdataGet{
			Key: k,
		})
	}

	err = conn.Send(ramnet.Rqdata{
		Action: "multi_get",
		Data:   tools.ToGob(d),
	})

	if err != nil {
		log.Println("[error]", err)
		return
	}

	for {
		var ans ramnet.Ansdata
		err = c.readAns(conn, ramnet.ConnectTimeout, &ans)
		if err != nil {
			log.Println("[error]", err)
			return
		}

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
	conn, err := c.getConnect()
	if err != nil {
		log.Println("[error]", err)
		return
	}
	defer c.pushConnect(conn)

	q = c.setPrefix(q)

	err = conn.Send(ramnet.Rqdata{
		Action: "search",
		Data: tools.ToGob(ramnet.RqdataGet{
			Key: q,
		}),
	})

	if err != nil {
		log.Println("[error]", err)
		return
	}

	for {
		var ans ramnet.Ansdata
		err = c.readAns(conn, ramnet.ConnectTimeout, &ans)
		if err != nil {
			log.Println("[error]", err)
			return
		}

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

// Del - Удаляем объект
func (c *CacheObj) Del(key string) (err error) {
	conn, err := c.getConnect()
	if err != nil {
		log.Println("[error]", err)
		return
	}
	defer c.pushConnect(conn)

	key = c.setPrefix(key)

	err = conn.Send(ramnet.Rqdata{
		Action: "del",
		Data: tools.ToGob(ramnet.RqdataSet{
			Key: key,
		}),
	})

	if err != nil {
		log.Println("[error]", err)
		return
	}

	var ans ramnet.Ansdata
	err = c.readAns(conn, ramnet.ConnectTimeout, &ans)
	if err != nil {
		log.Println("[error]", err)
		return
	}

	if ans.Error != "" {
		err = errors.New(ans.Error)
		return
	}

	return
}

func (c *CacheObj) setPrefix(key string) string {
	if strings.Contains(key, c.prefix) {
		return key
	}
	return c.prefix + key
}
