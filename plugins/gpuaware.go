// plugins/gpuaware.go
package plugins

import (
    "context"
    "fmt"

    v1 "k8s.io/api/core/v1"
    "k8s.io/apimachinery/pkg/runtime"
    "k8s.io/kube-scheduler/framework"
)

const (
    GPUAwareName = "GPUAware"
    // Minimum GPU memory required (in GB)
    MinGPUMemory = 8
)

type GPUAwarePlugin struct {
    handle framework.Handle
}

func NewGPUAwarePlugin(obj runtime.Object, handle framework.Handle) (framework.Plugin, error) {
    return &GPUAwarePlugin{
        handle: handle,
    }, nil
}

func (g *GPUAwarePlugin) Name() string {
    return GPUAwareName
}

// Filter eliminates nodes that don't meet GPU requirements
func (g *GPUAwarePlugin) Filter(
    ctx context.Context,
    state *framework.CycleState,
    pod *v1.Pod,
    nodeInfo *framework.NodeInfo,
) *framework.Status {
    // Check if pod requests GPU
    gpuRequest := getGPURequest(pod)
    if gpuRequest == 0 {
        return framework.NewStatus(framework.Success)
    }

    node := nodeInfo.Node()
    if node == nil {
        return framework.NewStatus(framework.Error, "node not found")
    }

    // Get GPU capacity from node
    gpuCapacity := getGPUCapacity(node)
    gpuAllocated := getGPUAllocated(nodeInfo)
    gpuAvailable := gpuCapacity - gpuAllocated

    if gpuAvailable < gpuRequest {
        return framework.NewStatus(
            framework.Unschedulable,
            fmt.Sprintf("insufficient GPU: need %d, available %d", gpuRequest, gpuAvailable),
        )
    }

    // Check GPU memory availability
    gpuMemory := getGPUMemory(node)
    if gpuMemory < MinGPUMemory {
        return framework.NewStatus(
            framework.Unschedulable,
            fmt.Sprintf("insufficient GPU memory: need %dGB, available %dGB", MinGPUMemory, gpuMemory),
        )
    }

    return framework.NewStatus(framework.Success)
}

// Score ranks nodes based on GPU characteristics
func (g *GPUAwarePlugin) Score(
    ctx context.Context,
    state *framework.CycleState,
    pod *v1.Pod,
    nodeName string,
) (int64, *framework.Status) {
    nodeInfo, err := g.handle.SnapshotSharedLister().NodeInfos().Get(nodeName)
    if err != nil {
        return 0, framework.NewStatus(framework.Error, err.Error())
    }

    node := nodeInfo.Node()
    score := int64(0)

    // Factor 1: GPU utilization (prefer less utilized nodes)
    gpuCapacity := getGPUCapacity(node)
    gpuAllocated := getGPUAllocated(nodeInfo)
    if gpuCapacity > 0 {
        utilizationPct := (gpuAllocated * 100) / gpuCapacity
        score += (100 - utilizationPct) * 2 // Weight: 2x
    }

    // Factor 2: GPU memory (prefer more memory)
    gpuMemory := getGPUMemory(node)
    score += gpuMemory * 3 // Weight: 3x

    // Factor 3: GPU interconnect (prefer NVLink topology)
    if hasNVLink(node) {
        score += 100 // Bonus for NVLink
    }

    // Factor 4: GPU generation (prefer newer GPUs)
    gpuGen := getGPUGeneration(node)
    score += gpuGen * 10

    return score, framework.NewStatus(framework.Success)
}

// Helper functions to extract GPU information from nodes
func getGPURequest(pod *v1.Pod) int64 {
    var total int64
    for _, container := range pod.Spec.Containers {
        if val, ok := container.Resources.Requests["nvidia.com/gpu"]; ok {
            total += val.Value()
        }
    }
    return total
}

func getGPUCapacity(node *v1.Node) int64 {
    if val, ok := node.Status.Capacity["nvidia.com/gpu"]; ok {
        return val.Value()
    }
    return 0
}

func getGPUAllocated(nodeInfo *framework.NodeInfo) int64 {
    var allocated int64
    for _, podInfo := range nodeInfo.Pods {
        pod := podInfo.Pod
        for _, container := range pod.Spec.Containers {
            if val, ok := container.Resources.Requests["nvidia.com/gpu"]; ok {
                allocated += val.Value()
            }
        }
    }
    return allocated
}

func getGPUMemory(node *v1.Node) int64 {
    // Read from node labels or annotations
    if val, ok := node.Labels["gpu-memory-gb"]; ok {
        // Parse the value and return
        // Implementation depends on your labeling scheme
        return parseMemoryValue(val)
    }
    return 0
}

func hasNVLink(node *v1.Node) bool {
    val, ok := node.Labels["gpu-interconnect"]
    return ok && val == "nvlink"
}

func getGPUGeneration(node *v1.Node) int64 {
    if val, ok := node.Labels["gpu-generation"]; ok {
        // Extract generation number (e.g., "a100" -> 8, "h100" -> 9)
        return parseGPUGeneration(val)
    }
    return 0
}

func parseMemoryValue(val string) int64 {
    // Simplified parsing - implement proper parsing
    return 16 // Default to 16GB
}

func parseGPUGeneration(val string) int64 {
    genMap := map[string]int64{
        "v100": 7,
        "a100": 8,
        "h100": 9,
    }
    if gen, ok := genMap[val]; ok {
        return gen
    }
    return 0
}