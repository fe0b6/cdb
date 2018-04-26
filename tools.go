package cdb

import (
	"fmt"
	"strconv"
	"strings"
)

// ReplacePlaceholder - Заменяем ? на $...
func ReplacePlaceholder(sqlrq string) string {
	// Заменяем вопросы на нумерованные переменные
	for nParam := 1; strings.Contains(sqlrq, "?"); nParam++ {
		sqlrq = strings.Replace(sqlrq, "?", fmt.Sprintf("$%d", nParam), 1)
	}
	return sqlrq
}

// GetPlaceholder - Формируем нужное кол-во placeholders
func GetPlaceholder(l int) string {
	arr := make([]string, l)
	for i := 1; i <= l; i++ {
		arr[i-1] = "$" + strconv.Itoa(i)
	}
	return strings.Join(arr, ",")
}
