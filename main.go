// punya rossgray

package main

import (
	"github.com/evelynphs/kube-custom-scheduler/plugins"
	"k8s.io/klog"
	scheduler "k8s.io/kubernetes/cmd/kube-scheduler/app"
)

func main() {
	command := scheduler.NewSchedulerCommand(
		scheduler.WithPlugin(edf.Name, edf.New),
	)
	if err := command.Execute(); err != nil {
		klog.Fatal(err)
	}
}