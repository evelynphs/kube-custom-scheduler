// punya rossgray

package main

import (
	"github.com/evelynphs/kube-custom-scheduler/plugins"
	"k8s.io/klog/v2"
	scheduler "k8s.io/kubernetes/cmd/kube-scheduler/app"
)

func main() {
	command := scheduler.NewSchedulerCommand(
		scheduler.WithPlugin(plugin.Name, plugin.New),
	)
	if err := command.Execute(); err != nil {
		klog.Fatal(err)
	}
}