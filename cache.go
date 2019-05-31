package cdb

import (
	"log"
	"strings"
	"time"

	"github.com/fe0b6/tools"
	"github.com/go-redis/redis"
)

var (
	// Cdb - хэндлер кэша
	Cdb CacheObj
)

// CacheConnect - коннект к кэщу
func CacheConnect(o InitCacheConnect) (err error) {
	Cdb.addrs = o.Hosts
	Cdb.prefix = o.Prefix

	err = Cdb.connect()
	if err != nil {
		log.Println("[error]", err)
		return
	}

	return
}

// CheckReconect - проверяем нежкен ли реконект
func (c *CacheObj) connect() (err error) {
	c.Conn = redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: c.addrs,
	})

	err = c.Conn.Ping().Err()
	if err != nil {
		log.Println("[error]", err)
		return
	}

	return
}

// CheckReconect - проверяем нежкен ли реконект
func (c *CacheObj) CheckReconect(err error) {
	if strings.Contains(err.Error(), "no route to host") {
		err = c.connect()
		if err != nil {
			log.Println("[error]", err)
			return
		}
	}
}

// GetObj - Получаем объект из кэша и сразу его преобразовываем
func (c *CacheObj) GetObj(key string, i interface{}) (err error) {
	key = c.setPrefix(key)

	var b []byte
	err = c.Conn.Get(key).Scan(&b)
	if err != nil {
		if err.Error() != "redis: nil" {
			log.Println("[error]", err)
		}
		return
	}

	tools.FromGob(i, b)
	return
}

// Get - Получаем объект из кэша
func (c *CacheObj) Get(key string) (b []byte, err error) {
	key = c.setPrefix(key)

	err = c.Conn.Get(key).Scan(&b)
	if err != nil {
		if err.Error() != "redis: nil" {
			log.Println("[error]", err)
		}
		c.CheckReconect(err)
		return
	}

	return
}

// Exists - Проверяем есть ли объект
func (c *CacheObj) Exists(key string) (ok bool, err error) {
	key = c.setPrefix(key)

	ans, err := c.Conn.Exists(key).Result()
	if err != nil {
		c.CheckReconect(err)
		log.Println("[error]", err)
		return
	}
	if ans == 1 {
		ok = true
	}

	return
}

// SetEx - добавление объекта со сроком жизни
func (c *CacheObj) SetEx(key string, data interface{}, ex int) (err error) {
	key = c.setPrefix(key)

	err = c.Conn.Set(key, data, time.Second*time.Duration(ex)).Err()
	if err != nil {
		c.CheckReconect(err)
		log.Println("[error]", err)
		return
	}

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
func (c *CacheObj) Set(key string, data interface{}) (err error) {
	return c.SetEx(key, data, 0)
}

// SetStrEx - добавляем объект в кэш
func (c *CacheObj) SetStrEx(key string, data string, ex int) (err error) {
	return c.SetEx(key, data, ex)
}

// SetStr - добавляем объект в кэш
func (c *CacheObj) SetStr(key string, data string) (err error) {
	return c.SetEx(key, data, 0)
}

// Incr - Увеличиваем значение
func (c *CacheObj) Incr(key string, i int64) (ai int64, err error) {
	key = c.setPrefix(key)

	ai, err = c.Conn.Incr(key).Result()
	if err != nil {
		c.CheckReconect(err)
		log.Println("[error]", err)
		return
	}

	return
}

// Expire - Ставим истечение для ключа
func (c *CacheObj) Expire(key string, ex int) (err error) {
	key = c.setPrefix(key)

	err = c.Conn.Expire(key, time.Second*time.Duration(ex)).Err()
	if err != nil {
		c.CheckReconect(err)
		log.Println("[error]", err)
		return
	}

	return
}

// Search - поиск ключей на соответствие
func (c *CacheObj) Search(q string, f func(string, []byte)) (err error) {
	q = c.setPrefix(q)

	log.Println(q)
	iter := c.Conn.Scan(0, q, 1000).Iterator()
	for iter.Next() {
		log.Println(iter.Val())
		var b []byte
		b, err = c.Get(iter.Val())
		if err != nil {
			log.Println("[error]", err)
			continue
		}

		f(iter.Val(), b)
	}
	if err = iter.Err(); err != nil {
		c.CheckReconect(err)
		log.Println("[error]", err)
		return
	}

	iter2 := c.Conn.Scan(0, "s*", 1000).Iterator()
	for iter2.Next() {
		log.Println(iter.Val())
	}

	return
}

// Del - Удаляем объект
func (c *CacheObj) Del(key string) (err error) {
	key = c.setPrefix(key)

	err = c.Conn.Del(key).Err()
	if err != nil {
		c.CheckReconect(err)
		log.Println("[error]", err)
		return
	}

	return
}

// Subscribe - подписываемся на уведомления
func (c *CacheObj) Subscribe(keys []string, f func(string, string) bool) (err error) {
	for i := range keys {
		keys[i] = c.setPrefix(keys[i])
	}

	// Подписываемся на каналы
	pubsub := c.Conn.Subscribe(keys...)
	defer pubsub.Close()

	// Ждем пока примут подписку
	_, err = pubsub.Receive()
	if err != nil {
		c.CheckReconect(err)
		log.Println("[error]", err)
		return
	}

	// Получаем канал для сообщений
	ch := pubsub.Channel()
	// Получаем сообщения
	for msg := range ch {
		if !f(msg.Channel, msg.Payload) {
			break
		}
	}

	return
}

// NotifyMulti - отправляем уведомление
func (c *CacheObj) NotifyMulti(keys []string, data []byte) (err error) {

	for _, key := range keys {
		key = c.setPrefix(key)
		err = c.Conn.Publish(key, data).Err()
		if err != nil {
			c.CheckReconect(err)
			log.Println("[error]", err)
			return
		}
	}

	return
}

// Notify - отправляем уведомление
func (c *CacheObj) Notify(key string, data []byte) (err error) {
	return c.NotifyMulti([]string{key}, data)
}

// NotifyStr - отправляем уведомление
func (c *CacheObj) NotifyStr(key string, data string) (err error) {
	return c.NotifyMulti([]string{key}, []byte(data))
}

func (c *CacheObj) setPrefix(key string) string {
	if strings.HasPrefix(key, c.prefix) {
		return key
	}
	return c.prefix + key
}
