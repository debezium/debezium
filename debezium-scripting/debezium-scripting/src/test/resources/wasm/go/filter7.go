package main

import (
	"github.com/debezium/debezium-smt-go-pdk"
)

//export process
func process(proxyPtr uint32) uint32 {
	var arr = debezium.Get(proxyPtr, "value.source.arr")
	var arrSize = debezium.GetArraySize(arr)
	var elem0 = debezium.GetArrayElem(arr, 0)
	var elem0Bool = debezium.GetBool(debezium.Get(elem0, "bool"))
	var elem0Int = debezium.GetInt32(debezium.Get(elem0, "int"))
	var elem1 = debezium.GetArrayElem(arr, 1)
	var elem1Bool = debezium.GetBool(debezium.Get(elem1, "bool"))
	var elem1Int = debezium.GetInt32(debezium.Get(elem1, "int"))

	return debezium.SetBool(arrSize == 2 && elem0Bool == true && elem0Int == 123 && elem1Bool == false && elem1Int == 321)
}

func main() {}
