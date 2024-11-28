package main

import (
	"github.com/andreaTP/debezium-smt-go-pdk"
)

//export process
func process(proxyPtr uint32) uint32 {
	var value = debezium.Get(proxyPtr, "value")

	// value == null ? 'nulls' : (value.before.id == 1 ? 'ones' : null)
	if debezium.IsNull(value) {
		return debezium.SetString("nulls")
	} else {
		var beforeId = debezium.GetUInt32(debezium.Get(proxyPtr, "value.before.id"))

		if beforeId == 1 {
			return debezium.SetString("ones")
		} else {
			return debezium.SetNull()
		}
	}
}

func main() {}
