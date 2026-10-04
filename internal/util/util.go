/*
SPDX-FileCopyrightText: 2026 SAP SE or an SAP affiliate company and kafka-operator contributors
SPDX-License-Identifier: Apache-2.0
*/

package util

import "strings"

func Capitalize(s string) string {
	if len(s) <= 1 {
		return s
	}
	return strings.ToUpper(s[0:1]) + s[1:]
}
