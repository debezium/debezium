#!/bin/bash

curl -H "Content-Type: application/json" --data '{"docker_tag": "nightly"}' -X POST https://hub.docker.com/api/build/v1/source/97204472-110c-4586-a455-4fb08f3d8b06/trigger/f0629dc1-3568-47f8-b03f-0fb122280506/call/
