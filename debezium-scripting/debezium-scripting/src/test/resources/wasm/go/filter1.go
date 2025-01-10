package main

import (
	"github.com/andreaTP/debezium-smt-go-pdk"
)

//export process
func process(proxyPtr uint32) uint32 {
	var op = debezium.GetString(debezium.Get(proxyPtr, "value.op"))
	var beforeId = debezium.GetInt(debezium.Get(proxyPtr, "value.before.id"))

	// value.op != 'd' || value.before.id != 2
	return debezium.SetBool(op != "d" || beforeId != 2)
}

func main() {}
