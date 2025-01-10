package main

import (
	"github.com/debezium/debezium-smt-go-pdk"
)

//export process
func process(proxyPtr uint32) uint32 {
	var valueId = debezium.GetInt(debezium.Get(proxyPtr, "value.after.id"))
	var valueLsn = debezium.GetInt(debezium.Get(proxyPtr, "value.source.lsn"))
	var valueVersion = debezium.GetString(debezium.Get(proxyPtr, "value.source.version"))
	var topicName = debezium.GetString(debezium.Get(proxyPtr, "topic"))

	// value.after.id == 1 && value.source.lsn == 1234 && value.source.version == "version!" && topic == "dummy"
	return debezium.SetBool(valueId == 1 && valueLsn == 1234 && valueVersion == "version!" && topicName == "dummy")
}

func main() {}
