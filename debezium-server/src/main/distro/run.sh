#!/bin/bash
#
# Copyright Debezium Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

if [ -z "$JAVA_HOME" ]; then
  JAVA_BINARY="java"
else
  JAVA_BINARY="$JAVA_HOME/bin/java"
fi

RUNNER=$(ls debezium-server-*runner.jar)

exec $JAVA_BINARY $DEBEZIUM_OPTS $JAVA_OPTS -cp "$RUNNER:conf:lib/*" io.debezium.server.Main
