package v1alpha1

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/sap/kafka-operator/pkg/types"
)

type SecretKeyReference struct {
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`
	// +kubebuilder:validation:MinLength=1
	Key    string `json:"key,omitempty"`
	loaded bool   `json:"-"`
	value  []byte `json:"-"`
}

func (s *SecretKeyReference) load(ctx context.Context, clnt client.Client, namespace string, fallbackKeys ...string) error {
	if s.loaded {
		panic("this should never happen (secret ref loaded more than once)")
	}
	secret := &corev1.Secret{}
	if err := clnt.Get(ctx, client.ObjectKey{Namespace: namespace, Name: s.Name}, secret); err != nil {
		if apierrors.IsNotFound(err) {
			return types.NewRetriableError(err, 10*time.Second)
		}
		return err
	}
	var keys []string
	if s.Key != "" {
		keys = append(keys, s.Key)
	}
	keys = append(keys, fallbackKeys...)
	if len(keys) == 0 {
		return fmt.Errorf("secret ref %s/%s has no key specified and no fallback keys provided", namespace, s.Name)
	}
	for _, key := range keys {
		if value, ok := secret.Data[key]; ok {
			s.value = value
			s.loaded = true
			return nil
		}
	}
	return fmt.Errorf("secret ref %s/%s has none of the specified keys: %v", namespace, s.Name, keys)
}

func (s *SecretKeyReference) Value() []byte {
	if !s.loaded {
		panic("this should never happen (secret ref not loaded)")
	}
	return s.value
}
