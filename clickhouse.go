package cdb

import (
	"fmt"
	"log"

	"github.com/jmoiron/sqlx"
	// подключаем clickhouse
	_ "github.com/kshvakov/clickhouse"
)

var (
	// Chh - хэндлер clickhouse
	Chh *sqlx.DB
)

// ConnectClickhouse - Подключание к clickhouse
func ConnectClickhouse(o InitConnect) {
	var err error

	// Коннефкт к базе
	Chh, err = sqlx.Open("clickhouse", fmt.Sprintf("tcp://%s?username=%s&password=%s&database=%s",
		o.Socket, o.Login, o.Password, o.DBName))
	if err != nil {
		log.Fatalln("[fatal]", err)
		return
	}

}
