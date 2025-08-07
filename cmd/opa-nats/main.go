package main

import (
	"log"

	"github.com/open-policy-agent/opa/cmd"
	"github.com/open-policy-agent/opa/v1/runtime"
	natsstore "github.com/permitio/opa-nats/pkg/natsstore"
)

func main() {
	pluginFactory := natsstore.NewPluginFactory()
	runtime.RegisterPlugin(natsstore.PluginName, pluginFactory)

	if err := cmd.RootCommand.Execute(); err != nil {
		log.Fatalf("Failed to execute OPA runtime: %v", err)
	}
}
