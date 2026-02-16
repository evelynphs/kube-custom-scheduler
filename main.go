// main.go
package main

import (
    "os"

    "k8s.io/component-base/cli"
    "k8s.io/kubernetes/cmd/kube-scheduler/app"

    // Import your custom plugins
    "github.com/evelynphs/custom-scheduler/plugins"
)

func main() {
    // Register custom plugins with the scheduler framework
    command := app.NewSchedulerCommand(
        app.WithPlugin(plugins.GPUAwareName, plugins.NewGPUAwarePlugin),
        app.WithPlugin(plugins.CostOptimizedName, plugins.NewCostOptimizedPlugin),
    )

    code := cli.Run(command)
    os.Exit(code)
}