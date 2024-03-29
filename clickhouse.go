package cdb

import (
	"fmt"
	"log"

	"github.com/jmoiron/sqlx"
	// подключаем clickhouse
	_ "github.com/ClickHouse/clickhouse-go"
	//_ "github.com/mailru/go-clickhouse"
)

var (
	// Chh - хэндлер clickhouse
	Chh *sqlx.DB
)

// ConnectClickhouse - Подключание к clickhouse
func ConnectClickhouse(o InitConnect) (err error) {

	// Коннефкт к базе
	Chh, err = sqlx.Open("clickhouse", fmt.Sprintf("tcp://%s?username=%s&password=%s&database=%s",
		o.Socket, o.Login, o.Password, o.DBName))
	if err != nil {
		log.Println("[error]", err)
		return
	}
	/*
		Chh, err = sqlx.Open("clickhouse", fmt.Sprintf("http://%s/%s", o.Socket, o.DBName))
		if err != nil {
			log.Println("[error]", err)
			return
		}*/
	err = Chh.Ping()
	if err != nil {
		log.Println("[error]", err)
		return
	}

	return
}
