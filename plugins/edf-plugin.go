package edf

import (
	"context"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/queuesort"
	frameworkruntime "k8s.io/kubernetes/pkg/scheduler/framework/runtime"
)

const (
	// Name : name of plugin used in the plugin registry and configurations.
	Name = "EDFQueueSort"
)

// EDFQueueSortArgs : configuration args for EDFQueueSort plugin.
// NOTE: This is decoded from pluginConfig.args (YAML/JSON) at runtime.
type EDFQueueSortArgs struct {
	// DeadlineAnnotation defines annotation key used to read deadline (RFC3339).
	// Example: "scheduling.ui.ac.id/deadline"
	DeadlineAnnotation string `json:"deadlineAnnotation,omitempty"`
}

// EDFQueueSort : Sort pods based on earliest deadline first (EDF)
type EDFQueueSort struct {
	logger      klog.Logger
	handle      framework.Handle
	deadlineKey string
}

var _ framework.QueueSortPlugin = &EDFQueueSort{}

// Name : returns the name of the plugin.
func (es *EDFQueueSort) Name() string {
	return Name
}

// getArgs : decode args from runtime.Object into EDFQueueSortArgs.
// Kube-scheduler passes pluginConfig.args as *runtime.Unknown (raw JSON/YAML),
// so we decode it ourselves (no scheme registration needed).
func getArgs(obj runtime.Object) (*EDFQueueSortArgs, error) {
	args := &EDFQueueSortArgs{}
	if err := frameworkruntime.DecodeInto(obj, args); err != nil {
		return nil, err
	}
	return args, nil
}

// New : create an instance of an EDFQueueSort plugin
func New(ctx context.Context, obj runtime.Object, handle framework.Handle) (framework.Plugin, error) {
	logger := klog.FromContext(ctx).WithValues("plugin", Name)
	logger.V(4).Info("Creating new instance of the EDFQueueSort plugin")

	args, err := getArgs(obj)
	if err != nil {
		return nil, err
	}

	key := args.DeadlineAnnotation
	if key == "" {
		key = "scheduling.ui.ac.id/deadline"
	}

	pl := &EDFQueueSort{
		logger:      logger,
		handle:      handle,
		deadlineKey: key,
	}
	return pl, nil
}

// Less is the function used by the activeQ heap algorithm to sort pods.
// 1) Sort Pods based on earliest deadline first (EDF).
// 2) Otherwise, follow the strategy of the in-tree QueueSort Plugin (PrioritySort Plugin)
func (es *EDFQueueSort) Less(pInfo1, pInfo2 fwk.QueuedPodInfo) bool {
	pod1 := pInfo1.GetPodInfo().GetPod()
	pod2 := pInfo2.GetPodInfo().GetPod()

	logger := es.logger.WithValues("ExtensionPoint", "Less")

	d1, ok1 := es.getDeadline(pod1.Annotations)
	d2, ok2 := es.getDeadline(pod2.Annotations)

	// If deadline missing/invalid, fallback to default behavior
	if !ok1 || !ok2 {
		logger.V(4).Info("Deadline missing/invalid, fallback to PrioritySort",
			"pod1", pod1.Name,
			"pod2", pod2.Name,
		)
		s := &queuesort.PrioritySort{}
		return s.Less(pInfo1, pInfo2)
	}

	// Earliest deadline first
	if d1.Before(d2) {
		return true
	}
	if d2.Before(d1) {
		return false
	}

	// Tie-break: fallback to default kube sort (priority, timestamp, etc.)
	s := &queuesort.PrioritySort{}
	return s.Less(pInfo1, pInfo2)
}

// getDeadline : parse deadline annotation (RFC3339)
func (es *EDFQueueSort) getDeadline(annotations map[string]string) (time.Time, bool) {
	if annotations == nil {
		return time.Time{}, false
	}

	raw := annotations[es.deadlineKey]
	if raw == "" {
		return time.Time{}, false
	}

	t, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		es.logger.V(4).Info("Invalid deadline format",
			"key", es.deadlineKey,
			"value", raw,
			"error", err,
		)
		return time.Time{}, false
	}

	return t, true
}