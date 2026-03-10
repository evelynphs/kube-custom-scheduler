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
	Name = "PreenqueueDefault"

	DefaultDurationSeconds = 600
)

// PreenqueueDefaultArgs : configuration args for PreenqueueDefault plugin.
// NOTE: This is decoded from pluginConfig.args (YAML/JSON) at runtime.
type PreenqueueDefaultArgs struct {
	DeadlineDurationAnnotation string `json:"deadlineAnnotation,omitempty"`
	DeadlineTimestampAnnotation string `json:"deadlineTimestampAnnotation,omitempty"`
}

// PreenqueueDefault : Sort pods based on earliest deadline first (EDF)
type PreenqueueDefault struct {
	logger      klog.Logger
	handle      framework.Handle
	deadlineDurationKey string
	deadlineTimestampKey string
}

var _ framework.PreEnqueuePlugin = &PreenqueueDefault{}

// Name : returns the name of the plugin.
func (es *PreenqueueDefault) Name() string {
	return Name
}

// getArgs : decode args from runtime.Object into PreenqueueDefaultArgs.
// Kube-scheduler passes pluginConfig.args as *runtime.Unknown (raw JSON/YAML),
// so we decode it ourselves (no scheme registration needed).
func getArgs(obj runtime.Object) (*PreenqueueDefaultArgs, error) {
	args := &PreenqueueDefaultArgs{}
	if err := frameworkruntime.DecodeInto(obj, args); err != nil {
		return nil, err
	}
	return args, nil
}

// New : create an instance of an PreenqueueDefault plugin
func New(ctx context.Context, obj runtime.Object, handle framework.Handle) (framework.Plugin, error) {
	logger := klog.FromContext(ctx).WithValues("plugin", Name)
	logger.Info("Creating new instance of the PreenqueueDefault plugin")

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

	pl := &PreenqueueDefault{
		logger:      logger,
		handle:      handle,
		deadlineDurationKey: key,
		deadlineTimestampKey: key2,
	}
	return pl, nil
}

func (es *PreenqueueDefault) PreEnqueue(ctx context.Context, pod *v1.Pod) *fwk.Status {
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

// Helpers====================================================================================

func (es *PreenqueueDefault) parseDeadlineDuration(pod *v1.Pod) time.Duration {
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
