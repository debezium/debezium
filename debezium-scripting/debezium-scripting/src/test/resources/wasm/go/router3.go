package main

import (
	"encoding/json"
	"github.com/andreaTP/debezium-smt-go-pdk"
)

type value_after struct {
	LastName string `json:"last_name"`
}

//export process
func process(proxyPtr uint32) uint32 {
	var value = debezium.Get(proxyPtr, "value")

	// value == null ? 'nulls' : ((new groovy.json.JsonSlurper()).parseText(value.after).last_name == 'Kretchmar' ? 'kretchmar' : null)
	if debezium.IsNull(value) {
		return debezium.SetString("nulls")
	} else {
		var valueAfter = debezium.GetString(debezium.Get(proxyPtr, "value.after"))
		res := value_after{}
		json.Unmarshal([]byte(valueAfter), &res)

		if res.LastName == "Kretchmar" {
			return debezium.SetString("kretchmar")
		} else {
			return debezium.SetNull()
		}
	}
}

func main() {}
