package cdb

import (
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"strconv"
	"time"
)

var (
	// Qdh - хэгдлер очереди
	Qdh QueueObj
)

// QueueInit - Инициализируем очередь
func QueueInit(o InitQueueParam) (exitChan chan bool) {
	exitChan = make(chan bool)
	go queueWaitExit(exitChan)

	// Создаем очередь
	Qdh.Chan = make(chan queueData, o.QueueSize)

	Qdh.Path = o.Path

	// Читаем лог
	tn := time.Now()

	h := map[string]queueData{}

	// Если начало дня - прочтем еще и вчерашний лог
	if tn.Hour() < 3 {
		Qdh.readLog(tn.Add(-24*time.Hour), h)
	}
	Qdh.readLog(tn, h)

	// Закидываем данные назад в очередь
	for _, v := range h {
		Qdh.Chan <- v
	}

	// Открываем лог
	err := Qdh.openLog()
	if err != nil {
		log.Fatalln("[error]", err)
		return
	}

	rand.Seed(time.Now().UnixNano())

	return
}

func queueWaitExit(exitChan chan bool) {
	_ = <-exitChan

	Qdh.Exited = true
	Qdh.Wait()

	exitChan <- true
}

// Complete - отмечаем что событие было выполнено
func (q *QueueObj) Complete(id string) (err error) {
	q.Add(1)
	defer q.Done()

	if q.Exited {
		err = errors.New("queue is closed")
		return
	}

	qd := queueData{ID: id, Ok: true}

	// Пишем данные в лог
	err = q.writeLog(qd)
	if err != nil {
		log.Println("[error]", err)
		return
	}

	return
}

// Send - отправка сообщения в очередь
func (q *QueueObj) Send(data []byte) (err error) {
	q.Add(1)
	defer q.Done()

	if q.Exited {
		err = errors.New("queue is closed")
		return
	}

	qd := queueData{ID: q.getID(), Data: data}

	// Пишем данные в лог
	err = q.writeLog(qd)
	if err != nil {
		log.Println("[error]", err)
		return
	}

	// Ставим задание в очередь
	q.Chan <- qd

	return
}

// Получаем ИД для объекта в очереди
func (q *QueueObj) getID() string {
	return strconv.FormatInt(time.Now().UnixNano(), 32) + strconv.FormatInt(rand.Int63(), 32)
}

// Читаем лог и восстанавливаем потерянные данные
func (q *QueueObj) readLog(t time.Time, h map[string]queueData) {

	f, err := os.Open(q.getFilePath(t))
	if err != nil {
		if err.Error() != os.ErrNotExist.Error() {
			log.Println("[error]", err)
		}
		return
	}
	defer f.Close()

	gr := gob.NewDecoder(f)

	for {
		var qd queueData
		err = gr.Decode(&qd)
		if err != nil {
			if err.Error() != io.EOF.Error() {
				log.Println("[error]", err)
			}
			return
		}

		if qd.Ok {
			delete(h, qd.ID)
			continue
		}

		h[qd.ID] = qd
	}
}

// Открываем лог для записи
func (q *QueueObj) openLog() (err error) {

	if q.Fh != nil {
		q.Fh.Close()
	}

	q.FileOpenTime = time.Now()

	q.Fh, err = os.OpenFile(q.getFilePath(q.FileOpenTime), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0640)
	if err != nil {
		log.Println("[error]", err)
		return
	}

	q.Gw = gob.NewEncoder(q.Fh)
	return
}

// Пишем инфу в лог
func (q *QueueObj) writeLog(qd queueData) (err error) {
	if q.FileOpenTime.Day() != time.Now().Day() {
		err = q.openLog()
		if err != nil {
			log.Println("[error]", err)
			return
		}
	}

	err = q.Gw.Encode(qd)
	if err != nil {
		log.Println("[error]", err)
		return
	}

	return
}

// Получаем путь к файлу логов
func (q *QueueObj) getFilePath(t time.Time) string {
	return fmt.Sprintf("%s%s.log", q.Path, t.Format("2006-01-02"))
}
