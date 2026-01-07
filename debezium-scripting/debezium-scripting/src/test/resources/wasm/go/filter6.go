package main

import (
	"github.com/debezium/debezium-smt-go-pdk"
)

//export process
func process(proxyPtr uint32) uint32 {
	var source0 = debezium.GetBool(debezium.Get(proxyPtr, "value.source.0"))
	var source1 = debezium.GetBytes(debezium.Get(proxyPtr, "value.source.1"))
	var source2 = debezium.GetFloat32(debezium.Get(proxyPtr, "value.source.2"))
	var source3 = debezium.GetFloat64(debezium.Get(proxyPtr, "value.source.3"))
	var source4 = debezium.GetInt16(debezium.Get(proxyPtr, "value.source.4"))
	var source5 = debezium.GetInt32(debezium.Get(proxyPtr, "value.source.5"))
	var source6 = debezium.GetInt64(debezium.Get(proxyPtr, "value.source.6"))
	var source7 = debezium.GetInt8(debezium.Get(proxyPtr, "value.source.7"))
	// optional values
	var source8 = debezium.IsNull(debezium.Get(proxyPtr, "value.source.8"))
	var source9 = debezium.IsNull(debezium.Get(proxyPtr, "value.source.9"))
	var source10 = debezium.IsNull(debezium.Get(proxyPtr, "value.source.10"))
	var source11 = debezium.IsNull(debezium.Get(proxyPtr, "value.source.11"))
	var source12 = debezium.IsNull(debezium.Get(proxyPtr, "value.source.12"))
	var source13 = debezium.IsNull(debezium.Get(proxyPtr, "value.source.13"))
	var source14 = debezium.IsNull(debezium.Get(proxyPtr, "value.source.14"))
	var source15 = debezium.IsNull(debezium.Get(proxyPtr, "value.source.15"))
	var source16 = debezium.IsNull(debezium.Get(proxyPtr, "value.source.16"))

	var source17 = debezium.GetString(debezium.Get(proxyPtr, "value.source.17"))

	// all fields and matching types
	var result = source0 == true &&
		source1[0] == byte(1) &&
		source2 == float32(2.2) &&
		source3 == float64(3.3) &&
		source4 == int16(4) &&
		source5 == int32(5) &&
		source6 == int64(6) &&
		source7 == int8(7) &&
		source8 == true &&
		source9 == true &&
		source10 == true &&
		source11 == true &&
		source12 == true &&
		source13 == true &&
		source14 == true &&
		source15 == true &&
		source16 == true &&
		source17 == "seventeen"

	return debezium.SetBool(result)
}

func main() {}
