package main

import (
	"github.com/andreaTP/debezium-smt-go-pdk"
)

type value_after struct {
	LastName string `json:"last_name"`
}

//export process
func process(proxyPtr uint32) uint32 {
	var version = debezium.GetString(debezium.Get(proxyPtr, "value.source.version"))

	if version == "version!" {
		return debezium.SetString("!version")
	} else {
		return debezium.SetNull()
	}
}

func main() {}
