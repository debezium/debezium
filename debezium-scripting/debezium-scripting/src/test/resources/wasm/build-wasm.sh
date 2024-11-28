#!/bin/bash
set -euo pipefail

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

# TODO: the loop with the container in the middle is slow, let see if we can use an alternative
for filename in ${SCRIPT_DIR}/go/*.go; do

    name=$(basename ${filename} .go)
    docker run --rm \
        -v ${SCRIPT_DIR}/:/src \
        -w /src tinygo/tinygo:0.34.0 bash \
        -c "cd go && tinygo build -ldflags='-extldflags --import-memory' --no-debug -target=wasm-unknown -o /tmp/tmp.wasm ${name}.go && cat /tmp/tmp.wasm" > \
        ${SCRIPT_DIR}/compiled/${name}.wasm

done

# Copy the filter to be used in the debezium-benchmark in the right location
rm -rf ${SCRIPT_DIR}/../../../../../../debezium-microbenchmark/src/main/resources/wasm/filter_bench.wasm
cp ${SCRIPT_DIR}/compiled/filter_bench.wasm ${SCRIPT_DIR}/../../../../../../debezium-microbenchmark/src/main/resources/wasm/filter_bench.wasm
