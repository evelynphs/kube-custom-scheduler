package plugin

import (
	"context"
	"time"
	"fmt"
    "strconv"
    "strings"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

	DefaultDurationSeconds = 600
)

// EDFQueueSortArgs : configuration args for EDFQueueSort plugin.
// NOTE: This is decoded from pluginConfig.args (YAML/JSON) at runtime.
type EDFQueueSortArgs struct {
	DeadlineDurationAnnotation string `json:"deadlineAnnotation,omitempty"`
	DeadlineTimestampAnnotation string `json:"deadlineTimestampAnnotation,omitempty"`
}

// EDFQueueSort : Sort pods based on earliest deadline first (EDF)
type EDFQueueSort struct {
	logger      klog.Logger
	handle      framework.Handle
	deadlineDurationKey string
	deadlineTimestampKey string
}

var _ framework.QueueSortPlugin = &EDFQueueSort{}
var _ framework.PreEnqueuePlugin = &EDFQueueSort{}

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
	logger.Info("Creating new instance of the EDFQueueSort plugin")

	args, err := getArgs(obj)
	if err != nil {
		return nil, err
	}

	key := args.DeadlineDurationAnnotation
	if key == "" {
		key = "scheduling/deadline-duration"
	}

	key2 := args.DeadlineTimestampAnnotation
	if key2 == "" {
		key2 = "scheduling/deadline-timestamp"
	}

	pl := &EDFQueueSort{
		logger:      logger,
		handle:      handle,
		deadlineDurationKey: key,
		deadlineTimestampKey: key2,
	}
	return pl, nil
}

func (es *EDFQueueSort) PreEnqueue(ctx context.Context, pod *v1.Pod) *fwk.Status {
	es.logger.Info("PreEnqueue called", "pod", pod.Name, "namespace", pod.Namespace)

	if ann := pod.Annotations; ann != nil {
        if _, exists := ann[es.deadlineTimestampKey]; exists {
            return fwk.NewStatus(fwk.Success)
        }
    }
	
    deadlineDuration := es.parseDeadlineDuration(pod)
    deadline := time.Now().Add(deadlineDuration)

	es.logger.Info("Deadlilne duration calculated: ", deadlineDuration.String(), " for pod ", pod.Name)

    // write deadline annotation back to the pod via API server
    podCopy := pod.DeepCopy()
    if podCopy.Annotations == nil {
        podCopy.Annotations = map[string]string{}
    }
    podCopy.Annotations[es.deadlineTimestampKey] = deadline.UTC().Format(time.RFC3339)

    _, err := es.handle.ClientSet().CoreV1().Pods(pod.Namespace).Update(
        ctx,
        podCopy,
        metav1.UpdateOptions{},
    )
    if err != nil {
        // don't block scheduling if annotation fails — just log and continue
        // the QueueSort will fall back to creation time
        return fwk.NewStatus(fwk.Success,
            fmt.Sprintf("failed to annotate deadline: %v", err))
    }

    return fwk.NewStatus(fwk.Success)
}

// Less is the function used by the activeQ heap algorithm to sort pods.
// 1) Pods with a valid deadline are always prioritized over pods without a deadline.
// 2) If both pods have deadlines, they are ordered by Earliest Deadline First (EDF).
// 3) If neither pod has a valid deadline, fallback to the in-tree QueueSort Plugin (PrioritySort).
func (es *EDFQueueSort) Less(pInfo1, pInfo2 fwk.QueuedPodInfo) bool {
	pod1 := pInfo1.GetPodInfo().GetPod()
	pod2 := pInfo2.GetPodInfo().GetPod()

	logger := es.logger.WithValues("ExtensionPoint", "Less")

	d1, ok1 := es.getDeadline(pod1.Annotations)
	d2, ok2 := es.getDeadline(pod2.Annotations)

	// Rule 1: Pods WITH a valid deadline come before pods WITHOUT a valid deadline.
	if ok1 != ok2 {
		// ok1=true means pod1 has deadline => pod1 should be ordered first (Less=true)
		logger.V(4).Info("One pod has deadline and the other does not; prioritizing deadline pod",
			"pod1", pod1.Name, "pod1HasDeadline", ok1,
			"pod2", pod2.Name, "pod2HasDeadline", ok2,
		)
		return ok1
	}

	// Rule 2: If neither has deadline, fallback to default kube sort.
	if !ok1 && !ok2 {
		logger.V(4).Info("Both pods missing/invalid deadline, fallback to PrioritySort",
			"pod1", pod1.Name,
			"pod2", pod2.Name,
		)
		s := &queuesort.PrioritySort{}
		return s.Less(pInfo1, pInfo2)
	}

	// Rule 3: Both have deadlines -> EDF (earliest deadline first).
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

// Helpers====================================================================================

func (es *EDFQueueSort) parseDeadlineDuration(pod *v1.Pod) time.Duration {
    annotations := pod.Annotations

    if annotations == nil {
        return DefaultDurationSeconds * time.Second
    }

    raw, exists := annotations[es.deadlineDurationKey]
    if !exists {
        return DefaultDurationSeconds * time.Second
    }

	es.logger.Info("parseDeadlineDuration called, deadline duration: ", raw, " for pod ", pod.Name)

    // handle "600s" format
    raw = strings.TrimSuffix(raw, "s")
    secs, err := strconv.ParseFloat(raw, 64)
    if err != nil || secs <= 0 {
        return DefaultDurationSeconds * time.Second
    }

    return time.Duration(secs) * time.Second
}

// getDeadline : parse deadline annotation (RFC3339)
func (es *EDFQueueSort) getDeadline(annotations map[string]string) (time.Time, bool) {
	if annotations == nil {
		return time.Time{}, false
	}

	raw := annotations[es.deadlineTimestampKey]
	if raw == "" {
		return time.Time{}, false
	}

	t, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		es.logger.V(4).Info("Invalid deadline format",
			"key", es.deadlineTimestampKey,
			"value", raw,
			"error", err,
		)
		return time.Time{}, false
	}

	return t, true
}
