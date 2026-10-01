package topic

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/sap/go-generics/maps"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	cskafkav1alpha1 "github.com/sap/kafka-operator/api/v1alpha1"
	"github.com/sap/kafka-operator/internal/connection"
	"github.com/sap/kafka-operator/internal/kafka"
	"github.com/sap/kafka-operator/internal/kafka/franz"
	"github.com/sap/kafka-operator/internal/util"
	"github.com/sap/kafka-operator/pkg/types"
)

type Reconciler struct {
	name          string
	finalizer     string
	fieldOwner    string
	client        client.Client
	eventRecorder record.EventRecorder
}

var _ reconcile.Reconciler = &Reconciler{}

func NewReconciler(name string, clnt client.Client, eventRecorder record.EventRecorder) *Reconciler {
	finalizer := fmt.Sprintf("%s/finalize", name)
	fieldOwner := name

	return &Reconciler{
		name:          name,
		finalizer:     finalizer,
		fieldOwner:    fieldOwner,
		client:        client.WithFieldOwner(clnt, fieldOwner),
		eventRecorder: eventRecorder,
	}
}

func (r *Reconciler) Reconcile(ctx context.Context, request ctrl.Request) (result reconcile.Result, err error) {
	log := ctrl.LoggerFrom(ctx)
	log.V(1).Info("Running reconcile")

	now := metav1.Now()

	// read requested topic
	topic := &cskafkav1alpha1.Topic{}
	if err := r.client.Get(ctx, request.NamespacedName, topic); err != nil {
		if apierrors.IsNotFound(err) {
			log.V(1).Info("not found; ignoring")
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("unexpected get error: %w", err)
	}
	savedTopic := topic.DeepCopy()

	// deferred status handling
	skipStatusUpdate := false
	defer func() {
		// recover panic and re-panic; this is needed to avoid the logic in this function to be executed in panic cases
		if r := recover(); r != nil {
			log.Error(fmt.Errorf("panic occurred during reconcile"), "panic", r)
			// TODO: should we nevertheless update the status informing about the panic?
			// because otherwise, in the case that controller-runtime finally recovers users might see confusing/misleading status information
			// re-panic in order skip the remaining steps
			panic(r)
		}

		// in error cases, always set state and Ready condition
		if err != nil {
			if retriableError, ok := errors.AsType[types.RetriableError](err); ok {
				topic.Status.State = cskafkav1alpha1.TopicStatePending
				topic.Status.SetReadyCondition(metav1.ConditionFalse, cskafkav1alpha1.TopicReadyReasonRetrying, util.Capitalize(retriableError.Error()))
				result.RequeueAfter = retriableError.RetryAfter()
				err = nil
			} else {
				topic.Status.State = cskafkav1alpha1.TopicStateFailed
				topic.Status.SetReadyCondition(metav1.ConditionFalse, cskafkav1alpha1.TopicReadyReasonError, util.Capitalize(err.Error()))
			}
		}

		// set some Ready condition if there is none;
		// this helps observers such as kstatus to understand things right
		if topic.Status.GetReadyCondition() == nil {
			topic.Status.SetReadyCondition(metav1.ConditionFalse, cskafkav1alpha1.TopicReadyReasonUnknown, "Topic state cannot be determined")
		}

		// set observed generations
		topic.Status.ObservedGeneration = topic.Generation
		for i := 0; i < len(topic.Status.Conditions); i++ {
			topic.Status.Conditions[i].ObservedGeneration = topic.Generation
		}

		// write log
		log.V(1).Info("reconcile done", "withError", err != nil, "requeue", result.Requeue || result.RequeueAfter > 0, "requeueAfter", result.RequeueAfter.String())

		// see if status has changed so far; this must be done before updating the timestamps
		statusChanged := !reflect.DeepEqual(topic.Status, savedTopic.Status)

		// update timestamps
		topic.Status.LastObservedAt = &now
		for i := 0; i < len(topic.Status.Conditions); i++ {
			cond := &topic.Status.Conditions[i]
			if savedCond := savedTopic.Status.GetCondition(cond.Type); savedCond == nil || cond.Status != savedCond.Status {
				cond.LastTransitionTime = now
			}
		}

		// update status (unless suppressed)
		if !skipStatusUpdate {
			if updateErr := r.client.Status().Update(ctx, topic); updateErr != nil {
				err = errors.Join(err, fmt.Errorf("failed to update status for topic %s/%s: %w", topic.Namespace, topic.Name, updateErr))
				result = ctrl.Result{}
			}
		}

		// send events; we do it only if something essentially changed, in order to avoid eventing noise,
		// and to avoid running into events being dropped by the event broadcaster because of rate limits
		if statusChanged {
			readyCondition := topic.Status.GetReadyCondition()
			if topic.Status.State == cskafkav1alpha1.TopicStateFailed {
				r.eventRecorder.Event(topic, corev1.EventTypeWarning, readyCondition.Reason, readyCondition.Message)
			} else {
				r.eventRecorder.Event(topic, corev1.EventTypeNormal, readyCondition.Reason, readyCondition.Message)
			}
		}
	}()

	// set a first status and requeue immediately
	if topic.Status.State == "" {
		topic.Status.State = cskafkav1alpha1.TopicStateInProgress
		topic.Status.SetReadyCondition(metav1.ConditionFalse, cskafkav1alpha1.TopicReadyReasonFirstSeen, "First seen")
		return ctrl.Result{Requeue: true}, nil
	}

	// extract requested topic details from the spec
	var name string = topic.Name
	if topic.Spec.Name != nil {
		// TODO: revalidate topic.Spec.Name?
		name = *topic.Spec.Name
	}
	var partitions int32 = 1
	if topic.Spec.Partitions != nil {
		// TODO: revalidate topic.Spec.Partitions?
		partitions = int32(*topic.Spec.Partitions)
	}
	var replicas int16 = -1
	if topic.Spec.Replicas != nil {
		// TODO: revalidate topic.Spec.Replicas?
		replicas = int16(*topic.Spec.Replicas)
	}
	var configs = topic.Spec.Configs

	// load references
	if err := topic.LoadReferences(ctx, r.client); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to load references for topic %s/%s: %w", topic.Namespace, topic.Name, err)
	}

	// parse and validate connection details
	connectionDetails, err := connection.ParseConnectionDetails(topic.Spec.ConnectionSecretRef.Value())
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to parse connection details for topic %s/%s: %w", topic.Namespace, topic.Name, err)
	}

	// get kafka client
	cli, err := franz.NewClient(r.name, connectionDetails)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to create Kafka client: %w", err)
	}

	// read existing topic
	tp, err := cli.ReadTopic(ctx, name)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("error reading topic %s: %w", name, err)
	}

	if topic.DeletionTimestamp.IsZero() {
		// apply case
		if controllerutil.AddFinalizer(topic, r.finalizer) {
			if err := r.client.Update(ctx, topic); err != nil {
				return ctrl.Result{}, fmt.Errorf("failed to add finalizer to topic %s/%s: %w", topic.Namespace, topic.Name, err)
			}
		}

		if tp == nil {
			if err := cli.CreateTopic(ctx, name, partitions, replicas, configs); err != nil {
				return ctrl.Result{}, fmt.Errorf("error creating topic %s: %w", name, err)
			}
			topic.Status.State = cskafkav1alpha1.TopicStateInProgress
			topic.Status.SetReadyCondition(metav1.ConditionFalse, cskafkav1alpha1.TopicReadyReasonProcessing, "Topic created successfully")
			return ctrl.Result{RequeueAfter: 100 * time.Millisecond}, nil
		} else {
			if tp.Internal {
				return ctrl.Result{}, fmt.Errorf("topic %s is internal and cannot be managed", name)
			}

			if updated, err := cli.UpdateTopicPartitions(ctx, tp, partitions); err != nil {
				return ctrl.Result{}, fmt.Errorf("error updating partitions for topic %s: %w", name, err)
			} else if updated {
				topic.Status.State = cskafkav1alpha1.TopicStateInProgress
				topic.Status.SetReadyCondition(metav1.ConditionFalse, cskafkav1alpha1.TopicReadyReasonProcessing, "Topic updated successfully")
				return ctrl.Result{RequeueAfter: 100 * time.Millisecond}, nil
			}

			if updated, err := cli.UpdateTopicReplicas(ctx, tp, replicas); err != nil {
				return ctrl.Result{}, fmt.Errorf("error updating topic replicas for topic %s: %w", name, err)
			} else if updated {
				topic.Status.State = cskafkav1alpha1.TopicStateInProgress
				topic.Status.SetReadyCondition(metav1.ConditionFalse, cskafkav1alpha1.TopicReadyReasonProcessing, "Topic updated successfully")
				return ctrl.Result{RequeueAfter: 100 * time.Millisecond}, nil
			}

			if updated, err := cli.UpdateTopicConfig(ctx, tp, configs); err != nil {
				return ctrl.Result{}, fmt.Errorf("error updating topic config for topic %s: %w", name, err)
			} else if updated {
				topic.Status.State = cskafkav1alpha1.TopicStateInProgress
				topic.Status.SetReadyCondition(metav1.ConditionFalse, cskafkav1alpha1.TopicReadyReasonProcessing, "Topic updated successfully")
				return ctrl.Result{RequeueAfter: 100 * time.Millisecond}, nil
			}

			topic.Status.Details = &cskafkav1alpha1.TopicDetails{
				Id:         tp.Id,
				Name:       tp.Name,
				Partitions: tp.Partitions,
				Replicas:   tp.Replicas,
				Configs: maps.CollectSlice(tp.Configs, func(config kafka.TopicConfig) (string, string) {
					key := config.Key
					value := config.Value
					if config.Overridden {
						// TODO: review
						value = fmt.Sprintf("*%s", value)
					}
					return key, value
				}),
			}
			topic.Status.State = cskafkav1alpha1.TopicStateReady
			topic.Status.SetReadyCondition(metav1.ConditionTrue, cskafkav1alpha1.TopicReadyReasonSynced, "Topic synchronized successfully")
			return ctrl.Result{RequeueAfter: 10 * time.Minute}, nil
		}
	} else {
		// delete case
		if tp != nil {
			if err := cli.DeleteTopic(ctx, name); err != nil {
				return ctrl.Result{}, fmt.Errorf("error deleting topic %s: %w", name, err)
			}
			topic.Status.State = cskafkav1alpha1.TopicStateDeleting
			topic.Status.SetReadyCondition(metav1.ConditionFalse, cskafkav1alpha1.TopicReadyReasonDeleting, "Topic deleted successfully")
			return ctrl.Result{RequeueAfter: 100 * time.Millisecond}, nil
		} else {
			if controllerutil.RemoveFinalizer(topic, r.finalizer) {
				if err := r.client.Update(ctx, topic); err != nil {
					return ctrl.Result{}, fmt.Errorf("failed to add finalizer to topic %s/%s: %w", topic.Namespace, topic.Name, err)
				}
			}
			skipStatusUpdate = true
			return ctrl.Result{}, nil
		}
	}
}

func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&cskafkav1alpha1.Topic{}, builder.WithPredicates(predicate.Or(predicate.GenerationChangedPredicate{}, predicate.AnnotationChangedPredicate{}))).
		Complete(r)
}
