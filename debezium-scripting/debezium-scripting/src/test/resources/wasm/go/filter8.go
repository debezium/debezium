package main

import (
	"github.com/debezium/debezium-smt-go-pdk"
)

//export process
func process(proxyPtr uint32) uint32 {
	var valueSchemaType = debezium.GetSchemaName(debezium.Get(proxyPtr, "valueSchema"))
	var opType = debezium.GetSchemaType(debezium.Get(proxyPtr, "valueSchema.op"))

	return debezium.SetBool(valueSchemaType == "dummy.Envelope" || opType == "string")
}

func main() {}
