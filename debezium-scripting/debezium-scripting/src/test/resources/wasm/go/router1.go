package main

import (
	"github.com/andreaTP/debezium-smt-go-pdk"
)

//export process
func process(proxyPtr uint32) uint32 {
	// 1
	return debezium.SetInt(1)
}

func main() {}
