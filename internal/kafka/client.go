package kafka

import (
	"context"
)

type Client interface {
	ReadTopic(ctx context.Context, name string) (*Topic, error)
	CreateTopic(ctx context.Context, name string, partitions int32, replicas int16, configs map[string]string) error
	UpdateTopicPartitions(ctx context.Context, topic *Topic, partitions int32) (bool, error)
	UpdateTopicReplicas(ctx context.Context, topic *Topic, replicas int16) (bool, error)
	UpdateTopicConfig(ctx context.Context, topic *Topic, configs map[string]string) (bool, error)
	DeleteTopic(ctx context.Context, name string) error
}

type Topic struct {
	Name        string
	Id          string
	Partitions  int32
	Replicas    int16
	Assignments map[int32][]int32
	Internal    bool
	Configs     []TopicConfig
}

type TopicConfig struct {
	Key        string
	Value      string
	Overridden bool
}
