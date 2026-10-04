/*
SPDX-FileCopyrightText: 2026 SAP SE or an SAP affiliate company and kafka-operator contributors
SPDX-License-Identifier: Apache-2.0
*/

package v1alpha1

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// TopicSpec defines the desired state of Topic.
type TopicSpec struct {
	// Reference to a secret containing the connection details (see pkg/api#ConnectionDetails)
	ConnectionSecretRef *SecretKeyReference `json:"connectionSecretRef" fallbackKeys:"value"`
	// +kubebuilder:validation:Pattern=`^[a-zA-Z0-9._-]{1,249}$`
	Name *string `json:"name,omitempty"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=2147483647
	Partitions *int64 `json:"partitions,omitempty"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=32767
	Replicas *int64            `json:"replicas,omitempty"`
	Configs  map[string]string `json:"configs,omitempty"`
}

// TopicStatus defines the observed state of Topic.
type TopicStatus struct {
	CommonStatus `json:",inline"`
	State        TopicState    `json:"state,omitempty"`
	Details      *TopicDetails `json:"details,omitempty"`
}

type TopicState string

const (
	TopicStatePending    TopicState = "Pending"
	TopicStateInProgress TopicState = "InProgress"
	TopicStateReady      TopicState = "Ready"
	TopicStateFailed     TopicState = "Failed"
	TopicStateDeleting   TopicState = "Deleting"
)

const (
	TopicReadyReasonUnknown    = "Unknown"
	TopicReadyReasonFirstSeen  = "FirstSeen"
	TopicReadyReasonProcessing = "Processing"
	TopicReadyReasonSynced     = "Synced"
	TopicReadyReasonDeleting   = "Deleting"
	TopicReadyReasonRetrying   = "Retrying"
	TopicReadyReasonError      = "Error"
)

// TODO: include partition assignment details?
type TopicDetails struct {
	Id         string            `json:"id,omitempty"`
	Name       string            `json:"name,omitempty"`
	Partitions int32             `json:"partitions,omitempty"`
	Replicas   int16             `json:"replicas,omitempty"`
	Configs    map[string]string `json:"configs,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Topic",type=string,JSONPath=`.status.details.name`
// +kubebuilder:printcolumn:name="Partitions",type=integer,JSONPath=`.status.details.partitions`
// +kubebuilder:printcolumn:name="Replicas",type=integer,JSONPath=`.status.details.replicas`
// +kubebuilder:printcolumn:name="State",type=string,JSONPath=`.status.state`
// +kubebuilder:printcolumn:name="Reason",type=string,JSONPath=`.status.conditions[?(@.type=='Ready')].reason`
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"
// +genclient

// Topic is the Schema for the Topics API.
// +kubebuilder:validation:XValidation:rule="!has(oldSelf.spec.name) && !has(self.spec.name) || !has(oldSelf.spec.name) && has(self.spec.name) && self.spec.name == self.metadata.name || has(oldSelf.spec.name) && !has(self.spec.name) && oldSelf.spec.name == self.metadata.name || has(oldSelf.spec.name) && has(self.spec.name) && oldSelf.spec.name == self.spec.name",message="spec.name is immutable"
// +kubebuilder:validation:XValidation:rule="!has(oldSelf.spec.partitions) || has(oldSelf.spec.partitions) && !has(self.spec.partitions) && oldSelf.spec.partitions == 1 || has(oldSelf.spec.partitions) && has(self.spec.partitions) && oldSelf.spec.partitions <= self.spec.partitions",message="spec.partitions can only be increased"
// +kubebuilder:validation:XValidation:rule="!has(oldSelf.spec.replicas) || has(self.spec.replicas)",message="spec.replicas cannot be unset"
type Topic struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec TopicSpec `json:"spec,omitempty"`
	// +kubebuilder:default={"observedGeneration":-1}
	Status TopicStatus `json:"status,omitempty"`
}

func (t *Topic) LoadReferences(ctx context.Context, clnt client.Client) error {
	getFallbackKeys := func(v any, fieldName string, tag string) []string {
		t := reflect.TypeOf(v)
		field, haveField := t.FieldByName(fieldName)
		if !haveField {
			panic(fmt.Sprintf("this should never happen (field %s not found in type %T)", fieldName, v))
		}
		value := field.Tag.Get(tag)
		if value == "" {
			return nil
		}
		return strings.Split(value, ",")
	}

	if t.Spec.ConnectionSecretRef != nil {
		if err := t.Spec.ConnectionSecretRef.load(ctx, clnt, t.Namespace, getFallbackKeys(t.Spec, "ConnectionSecretRef", "fallbackKeys")...); err != nil {
			return err
		}
	}

	return nil
}

// +kubebuilder:object:root=true

// TopicList contains a list of Topic.
type TopicList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Topic `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Topic{}, &TopicList{})
}
