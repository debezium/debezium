#!/bin/bash

curl -H "Content-Type: application/json" --data '{"docker_tag": "nightly"}' -X POST https://registry.hub.docker.com/u/debezium/connect/trigger/f0629dc1-3568-47f8-b03f-0fb122280506/
