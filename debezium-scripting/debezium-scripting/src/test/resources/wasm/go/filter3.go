package main

import (
	"github.com/debezium/debezium-smt-go-pdk"
)

//export process
func process(proxyPtr uint32) uint32 {
	var headerIdh = debezium.GetInt(debezium.Get(proxyPtr, "header.idh.value"))

	// header.idh.value == 1
	return debezium.SetBool(headerIdh == 1)
}

func main() {}
