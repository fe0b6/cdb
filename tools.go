package cdb

import (
	"fmt"
	"strings"
)

func replacePlaceholder(sqlrq string) string {
	// Заменяем вопросы на нумерованные переменные
	for nParam := 1; strings.Contains(sqlrq, "?"); nParam++ {
		sqlrq = strings.Replace(sqlrq, "?", fmt.Sprintf("$%d", nParam), 1)
	}
	return sqlrq
}
