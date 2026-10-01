package franz

import (
	"context"
	"encoding/hex"
	"fmt"

	"github.com/sap/go-generics/maps"
	"github.com/sap/go-generics/slices"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/sap/kafka-operator/internal/kafka"
	"github.com/sap/kafka-operator/pkg/api"
)

// TODO: review error message/panics

type client struct {
	c *kadm.Client
}

var _ kafka.Client = &client{}

func NewClient(id string, connectionDetails *api.ConnectionDetails) (kafka.Client, error) {
	c, err := newAdminClient(id, connectionDetails)
	if err != nil {
		return nil, err
	}
	return &client{c: c}, nil
}

func (c *client) ReadTopic(ctx context.Context, name string) (*kafka.Topic, error) {
	details, err := c.c.ListTopicsWithInternal(ctx)
	if err != nil {
		return nil, fmt.Errorf("error listing topics: %w", err)
	}
	detail, ok := details[name]
	if !ok {
		return nil, nil
	}
	if detail.Err != nil {
		return nil, fmt.Errorf("error loading topic %s: %w", name, detail.Err)
	}
	if detail.Topic != name {
		panic(fmt.Sprintf("this should never happen: topic name mismatch, expected %s, got %s", name, detail.Topic))
	}

	var assignments = make(map[int32][]int32)
	for partitionId, partition := range detail.Partitions {
		if partition.Topic != name {
			panic(fmt.Sprintf("this should not happen: partition topic mismatch: expected %s, got %s", name, partition.Topic))
		}
		if partition.Partition != partitionId {
			panic(fmt.Sprintf("this should not happen: partition ID mismatch: expected %d, got %d", partitionId, partition.Partition))
		}
		assignments[partitionId] = partition.Replicas
	}

	var replicas int16 = -1
	for _, partition := range detail.Partitions {
		if replicas == -1 {
			replicas = int16(len(partition.Replicas))
		} else if replicas != int16(len(partition.Replicas)) {
			replicas = 0
			break
		}
	}

	resourceConfigs, err := c.c.DescribeTopicConfigs(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("error describing topic configs for topic %s: %w", name, err)
	}
	if len(resourceConfigs) != 1 {
		return nil, fmt.Errorf("expected exactly one resource config for topic %s, got %d", name, len(resourceConfigs))
	}
	resourceConfig := resourceConfigs[0]
	if resourceConfig.Err != nil {
		if errmsg := resourceConfig.ErrMessage; errmsg == "" {
			return nil, fmt.Errorf("error in resource config for topic %s: %w", name, resourceConfig.Err)
		} else {
			return nil, fmt.Errorf("error in resource config for topic %s: %w (%s)", name, resourceConfig.Err, errmsg)
		}
	}
	if resourceConfig.Name != name {
		panic(fmt.Sprintf("this should never happen: resource config name mismatch, expected %s, got %s", name, resourceConfig.Name))
	}
	for _, config := range resourceConfig.Configs {
		if config.Sensitive {
			return nil, fmt.Errorf("sensitive config found for topic %s: %s", name, config.Key)
		}
		if config.Value == nil {
			return nil, fmt.Errorf("empty config value found for topic %s: %s", name, config.Key)
		}
	}

	return &kafka.Topic{
		Name:        detail.Topic,
		Id:          hex.EncodeToString(detail.ID[:]),
		Partitions:  int32(len(detail.Partitions)),
		Replicas:    replicas,
		Assignments: assignments,
		Internal:    detail.IsInternal,
		Configs: slices.Collect(resourceConfig.Configs, func(config kadm.Config) kafka.TopicConfig {
			return kafka.TopicConfig{
				Key:        config.Key,
				Value:      *config.Value,
				Overridden: config.Source == kmsg.ConfigSourceDynamicTopicConfig,
			}
		}),
	}, nil
}

func (c *client) CreateTopic(ctx context.Context, name string, partitions int32, replicas int16, configs map[string]string) error {
	resp, err := c.c.CreateTopic(ctx, partitions, replicas, maps.Collect(configs, func(value string) *string {
		return &value
	}), name)
	if err != nil {
		return fmt.Errorf("error creating topic %s: %w", name, err)
	} else if resp.Err != nil {
		if errmsg := resp.Err.Error(); errmsg == "" {
			return fmt.Errorf("error creating topic %s: %w", name, resp.Err)
		} else {
			return fmt.Errorf("error creating topic %s: %w (%s)", name, resp.Err, errmsg)
		}
	}
	if resp.Topic != name {
		panic(fmt.Sprintf("this should never happen: topic name mismatch, expected %s, got %s", name, resp.Topic))
	}
	// TODO: check resp.NumPartitions vs. partitions
	// TODO: check resp.ReplicationFactor vs. replicas if replicas > 0
	// TODO: check resp.Configs vs. config
	for key, config := range resp.Configs {
		if config.Key != key {
			panic(fmt.Sprintf("this should never happen: config key mismatch, expected %s, got %s", key, config.Key))
		}
		if config.Sensitive {
			return fmt.Errorf("sensitive config found for topic %s: %s", name, config.Key)
		}
		if config.Value == nil {
			return fmt.Errorf("empty config value found for topic %s: %s", name, config.Key)
		}
	}
	return nil
}

func (c *client) UpdateTopicPartitions(ctx context.Context, topic *kafka.Topic, partitions int32) (bool, error) {
	if partitions < topic.Partitions {
		return false, fmt.Errorf("cannot decrease partitions for topic %s: current %d, requested %d", topic.Name, topic.Partitions, partitions)
	}
	if partitions == topic.Partitions {
		return false, nil
	}
	resps, err := c.c.UpdatePartitions(ctx, int(partitions), topic.Name)
	if err != nil {
		return false, fmt.Errorf("error updating partitions for topic %s: %w", topic.Name, err)
	}
	resp, ok := resps[topic.Name]
	if !ok {
		return false, fmt.Errorf("no response for topic %s when updating partitions", topic.Name)
	}
	if resp.Topic != topic.Name {
		panic(fmt.Sprintf("this should never happen: topic name mismatch, expected %s, got %s", topic.Name, resp.Topic))
	}
	if resp.Err != nil {
		if errmsg := resp.Err.Error(); errmsg == "" {
			return false, fmt.Errorf("error updating partitions for topic %s: %w", topic.Name, resp.Err)
		} else {
			return false, fmt.Errorf("error updating partitions for topic %s: %w (%s)", topic.Name, resp.Err, errmsg)
		}
	}
	return true, nil
}

func (c *client) UpdateTopicReplicas(ctx context.Context, topic *kafka.Topic, replicas int16) (bool, error) {
	if len(topic.Assignments) == 0 {
		panic(fmt.Sprintf("cannot update replicas for stale topic %s (re-read required)", topic.Name))
	}
	if replicas < 0 {
		// TODO: review this; it might happen that replicas might be out of sync, though
		return false, nil
	}
	changedAssignments := make(map[int32][]int32)
	var brokers []int32
	for partitionId, assignment := range topic.Assignments {
		if len(assignment) > int(replicas) {
			changedAssignments[partitionId] = assignment[:replicas]
		} else if len(assignment) < int(replicas) {
			if brokers == nil {
				metadata, err := c.c.BrokerMetadata(ctx)
				if err != nil {
					return false, fmt.Errorf("failed to get broker metadata: %w", err)
				}
				brokers = slices.Collect(metadata.Brokers, func(v kadm.BrokerDetail) int32 {
					return v.NodeID
				})
			}
			brokers := slices.Remove(brokers, assignment...)
			numRequiredBrokers := int(replicas) - len(assignment)
			if len(brokers) < numRequiredBrokers {
				return false, fmt.Errorf("error finding additional brokers for partition %d of topic %s", partitionId, topic.Name)
			}
			// TODO: add some randomization when selecting additional brokers
			changedAssignments[partitionId] = append(assignment, brokers[:numRequiredBrokers]...)
		}
	}
	if len(changedAssignments) == 0 {
		return false, nil
	}
	resps, err := c.c.AlterPartitionAssignments(ctx, kadm.AlterPartitionAssignmentsReq{
		topic.Name: changedAssignments,
	})
	if err != nil {
		return false, fmt.Errorf("error altering partition assignments for topic %s: %w", topic.Name, err)
	}
	if _, ok := resps[topic.Name]; !ok {
		return false, fmt.Errorf("no response for topic %s when altering partition assignments", topic.Name)
	}
	for partitionId := range changedAssignments {
		resp, ok := resps[topic.Name][partitionId]
		if !ok {
			return false, fmt.Errorf("no response for partition %d of topic %s when altering partition assignments", partitionId, topic.Name)
		}
		if resp.Topic != topic.Name {
			panic(fmt.Sprintf("this should never happen: topic name mismatch, expected %s, got %s", topic.Name, resp.Topic))
		}
		if resp.Partition != partitionId {
			panic(fmt.Sprintf("this should never happen: partition ID mismatch for topic %s, expected %d, got %d", topic.Name, partitionId, resp.Partition))
		}
		if resp.Err != nil {
			if errmsg := resp.Err.Error(); errmsg == "" {
				return false, fmt.Errorf("error altering assignments for partition %d for topic %s: %w", partitionId, topic.Name, resp.Err)
			} else {
				return false, fmt.Errorf("error altering assignments for partition %d for topic %s: %w (%s)", partitionId, topic.Name, resp.Err, errmsg)
			}
		}
	}
	return true, nil
}

func (c *client) UpdateTopicConfig(ctx context.Context, topic *kafka.Topic, configs map[string]string) (bool, error) {
	if len(topic.Configs) == 0 {
		panic(fmt.Sprintf("cannot update config for stale topic %s (re-read required)", topic.Name))
	}
	var changedConfigs []kadm.AlterConfig
	for key, value := range configs {
		found := false
		for _, config := range topic.Configs {
			if config.Key == key {
				found = true
				if config.Value != value || !config.Overridden {
					changedConfigs = append(changedConfigs, kadm.AlterConfig{
						Op:    kadm.SetConfig,
						Name:  key,
						Value: &value,
					})
				}
			}
		}
		if !found {
			changedConfigs = append(changedConfigs, kadm.AlterConfig{
				Op:    kadm.SetConfig,
				Name:  key,
				Value: &value,
			})
		}
	}
	for _, config := range topic.Configs {
		if _, ok := configs[config.Key]; !ok && config.Overridden {
			changedConfigs = append(changedConfigs, kadm.AlterConfig{
				Op:   kadm.DeleteConfig,
				Name: config.Key,
			})
		}
	}
	if len(changedConfigs) == 0 {
		return false, nil
	}
	resps, err := c.c.AlterTopicConfigs(ctx, changedConfigs, topic.Name)
	if err != nil {
		return false, fmt.Errorf("error altering topic configs for topic %s: %w", topic.Name, err)
	}
	for _, resp := range resps {
		if resp.Name != topic.Name {
			return false, fmt.Errorf("topic name mismatch in alter config response, expected %s, got %s", topic.Name, resp.Name)
		}
		if resp.Err != nil {
			if errmsg := resp.Err.Error(); errmsg == "" {
				return false, fmt.Errorf("error altering config for topic %s: %w", topic.Name, resp.Err)
			} else {
				return false, fmt.Errorf("error altering config for topic %s: %w (%s)", topic.Name, resp.Err, errmsg)
			}
		}
	}
	return true, nil
}

func (c *client) DeleteTopic(ctx context.Context, name string) error {
	resp, err := c.c.DeleteTopic(ctx, name)
	if err != nil {
		return fmt.Errorf("error deleting topic %s: %w", name, err)
	} else if resp.Err != nil {
		if errmsg := resp.Err.Error(); errmsg == "" {
			return fmt.Errorf("error deleting topic %s: %w", name, resp.Err)
		} else {
			return fmt.Errorf("error deleting topic %s: %w (%s)", name, resp.Err, errmsg)
		}
	}
	return nil
}
