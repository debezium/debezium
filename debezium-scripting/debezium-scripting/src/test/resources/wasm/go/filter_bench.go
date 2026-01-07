package main

import (
	"github.com/debezium/debezium-smt-go-pdk"
)

//export process
func process(proxyPtr uint32) uint32 {
	var op = debezium.GetString(debezium.Get(proxyPtr, "value.op"))

	// value.get('op') == 'd'
	return debezium.SetBool(op != "d")
}

func main() {}
