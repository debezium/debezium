package main

import (
	"github.com/andreaTP/debezium-smt-go-pdk"
	"strings"
)

//export process
func process(proxyPtr uint32) uint32 {
	var headerIdh = debezium.GetInt(debezium.Get(proxyPtr, "header.idh.value"))
	var topicName = debezium.GetString(debezium.Get(proxyPtr, "topic"))

	// header.idh.value == 1 && topic.startsWith('dummy')
	return debezium.SetBool(headerIdh == 1 && strings.HasPrefix(topicName, "dummy"))
}

func main() {}
