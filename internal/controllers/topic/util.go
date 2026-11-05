/*
SPDX-FileCopyrightText: 2026 SAP SE or an SAP affiliate company and kafka-operator contributors
SPDX-License-Identifier: Apache-2.0
*/

package topic

import (
	"github.com/sap/go-generics/slices"

	"github.com/sap/kafka-operator/internal/kafka"
)

func isTopicInSync(tp *kafka.Topic, partitions int32, replicas int16, configs map[string]string) bool {
	if tp.Partitions != partitions {
		return false
	}
	if tp.Replicas != replicas {
		return false
	}
	for k, v := range configs {
		if slices.None(tp.Configs, func(t kafka.TopicConfig) bool {
			return t.Overridden && t.Key == k && t.Value == v
		}) {
			return false
		}
	}
	return true
}
