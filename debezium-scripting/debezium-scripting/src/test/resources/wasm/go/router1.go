package main

import (
	"github.com/debezium/debezium-smt-go-pdk"
)

//export process
func process(proxyPtr uint32) uint32 {
	// boolean
	return debezium.SetBool(false)
}

func main() {}
