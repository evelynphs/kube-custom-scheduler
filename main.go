// main.go
package main

import (
    "os"

    "k8s.io/component-base/cli"
    schedapp "k8s.io/kube-scheduler/app"

    // Import your custom plugins
    "github.com/evelynphs/kube-custom-scheduler/plugins"
)

func main() {
    // Register custom plugins with the scheduler framework
    command := schedapp.NewSchedulerCommand(
        schedapp.WithPlugin(plugins.GPUAwareName, plugins.NewGPUAwarePlugin),
    )

    code := cli.Run(command)
    os.Exit(code)
}