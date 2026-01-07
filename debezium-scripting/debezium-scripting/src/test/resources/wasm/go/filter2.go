package main

import (
	"github.com/debezium/debezium-smt-go-pdk"
)

//export process
func process(proxyPtr uint32) uint32 {
	var topicNamePtr = debezium.Get(proxyPtr, "topic")
	var topicName = debezium.GetString(topicNamePtr)

	// topic == 'dummy1'
	return debezium.SetBool(topicName == "dummy1")
}

func main() {}
