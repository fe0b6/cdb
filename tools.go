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

// GetPlaceholderWithDiff - Формируем нужное кол-во placeholders со смещением
func GetPlaceholderWithDiff(diff, l int) string {
	arr := make([]string, l)
	for i := diff + 1; i <= diff+l; i++ {
		arr[i-1] = "$" + strconv.Itoa(i)
	}
	return strings.Join(arr, ",")
}

// GetMultiPlaceholder - Формируем нужное кол-во placeholders для множественной вставки
func GetMultiPlaceholder(l, c int) string {
	str := strings.TrimRight(strings.Repeat("?,", l), ",")
	str = strings.TrimRight(strings.Repeat("("+str+"),", c), ",")

	return ReplacePlaceholder(str)
}
