Copyright Debezium Authors.
Licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0).

# Debezium Kafka Connect REST Extension

Debezium is an open source distributed platform for change data capture (CDC).

This repository contains Debezium-specific extensions to Kafka Connect's REST API.

## Setup

1. Install or mount the Debezium Kafka Connect REST Extension jar into a separate
   Kafka Connect plugin directory.

    For example with Docker Compose:

```yaml
    volumes:
     - debezium-kcd-rest-extension-1.0.0.jar:/kafka/connect/dbz-rest-extension/debezium-kcd-rest-extension-1.0.0.jar
```

2. Register the REST extension with Kafka Connect:

```yaml
    environment:
     - CONNECT_REST_EXTENSION_CLASSES=io.debezium.kcrestextension.DebeziumConnectRestExtension
```

or set `rest.extension.classes=io.debezium.kcrestextension.DebeziumConnectRestExtension` in your Kafka Connect properties file.

## Contribution

This project is under active development, any contributions are very welcome.
