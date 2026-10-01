package util

import "strings"

func Capitalize(s string) string {
	if len(s) <= 1 {
		return s
	}
	return strings.ToUpper(s[0:1]) + s[1:]
}
