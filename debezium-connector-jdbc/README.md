[![License](http://img.shields.io/:license-apache%202.0-brightgreen.svg)](http://www.apache.org/licenses/LICENSE-2.0.html)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.debezium/debezium-parent/badge.svg)](https://central.sonatype.com/search?smo=true&q=io.debezium)
[![User chat](https://img.shields.io/badge/chat-users-brightgreen.svg)](https://debezium.zulipchat.com/#narrow/stream/302529-users)
[![Developer chat](https://img.shields.io/badge/chat-devs-brightgreen.svg)](https://debezium.zulipchat.com/#narrow/stream/302533-dev)
[![Google Group](https://img.shields.io/:mailing%20list-debezium-brightgreen.svg)](https://groups.google.com/forum/#!forum/debezium)
[![Stack Overflow](http://img.shields.io/:stack%20overflow-debezium-brightgreen.svg)](http://stackoverflow.com/questions/tagged/debezium)

Copyright Debezium Authors.
Licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0).

# Debezium JDBC Sink Connector

Debezium is an open source project that provides a low latency data streaming platform for change data capture (CDC).
This connector provides a sink implementation for streaming changes emitted by Debezium into a relational database.

## What's different from other JDBC sink connectors?

This connector implementation is Debezium-source aware.
This means that the connector can consume native Debezium change events without needing to use the `ExtractNewRecordState` to flatten the event structure.
This reduces the necessary configuration to use a JDBC sink connector.
In addition, this also means that the sink side of the pipeline can take advantage of Debezium metadata, such as column type propagation to seamlessly support proper column type resolution on the sink connector side of the  pipeline.

## Architecture

The JDBC sink connector is a traditional Kafka Connect sink connector (aka consumer).
Its job is to read records from a one or more Kafka topics and to produce SQL statements that are executed on the configured destination database.

### Sink record descriptors

A `JdbcKafkaSinkRecord` is an object that gets constructed from every `SinkRecord`.
Most methods that would otherwise take a `SinkRecord` take this descriptor object instead.
The descriptor is in effect a pre-processed version of the `SinkRecord`, which allows us to perform this pre-processing once and to then make use of this information across the connector.
When adding new methods, you generally will want to use a `JdbcKafkaSinkRecord`.

### Dialects

Each sink database will typically have its own `DatabaseDialect` implementation that should extend `GeneralDatabaseDialect`.
The dialect is one of the core mechanisms used by the JDBC sink connector in order to resolve SQL statements and other database characteristics for the database the connector will be writing consumed events into.
The JDBC sink connector relies on the dialect resolution of Hibernate to drive the dialect wrapper used by the connector.

If no dialect mapping is detected for the sink database being used, the JDBC sink connector will default to using the `GeneralDatabaseDialect` implementation.
This generalized implementation does not support every aspect of the connector, for example UPSERT insert mode is not supported when this dialect is chosen as the UPSERT statement is generally unique to the database being used.
It's generally a good idea to add a new dialect implementation if a new sink database is to have full compatibility with the JDBC sink connector's vast behavior.

### Types

Every field in a Kafka message is associated with a schema type, but this type information can also carry other metadata such as a name or even parameters that have been provided by the source connector.
The JDBC sink connector utilizes a type system, which is based on the `io.debezium.connector.jdbc.type.Type` contract, in order to handle value binding, default value resolution, and other characteristic that could be type-specific.

There are effectively three different types of `Type` implementations:

* Those to support Kafka Connect's schema types, found in `io.debezium.connector.jdbc.type.connect`.
* Those to support Debezium-specific named schema types, found in `io.debezium.connector.jdbc.type.debezium`.
* Dialect-specific types, found in `io.debezium.connector.jdbc.dialect` hierarchy.

Types are registered in a hierarchical pattern, starting with the Kafka Connect types, then the Debezium types, and lastly the dialect-specific types.
This enables the Debezium types to override Kafka Connect types if needed and finally the dialect to override any other contributed type.

Types are resolved by first looking at the Kafka schema name and mapping this to a type registration.
If the schema does not have a name, the type of the schema is then used to resolve to a type.
This allows the  base Kafka Connect types to have a final say in how data is interpreted if no other type implementation is detected for the field.

### Naming Strategies

There are two naming strategies used by the JDBC sink connector:

* Table/Collection naming strategy, `CollectionNamingStrategy`, found in the `debezium-sink` module.
* Column naming strategy, `ColumnNamingStrategy`

The JDBC sink connector is shipped with default implementations of both, found in the `io.debezium.connector.jdbc.naming` package.
The default behavior of these two strategies are as follows:

* The table naming strategy replaces all occurrences of `.` with `_` and uses the configured `table.name.format` value to resolve the table's final name.
  So assuming that the topic name of the event is `server.schema.table` with the default `table.name.format=dbo.${topic}`, the destination table will be created as `dbo.server_schema_table`.
* The column naming strategy allows you to define any custom behavior on column name resolution.
  The default behavior is to simply return the field name as the column name.

These two strategies can be overridden by specifying fully qualified class name references in the connector configuration.
An example configuration:

     collection.naming.strategy=io.debezium.sink.naming.DefaultCollectionNamingStrategy
     column.naming.strategy=io.debezium.connector.jdbc.naming.DefaultColumnNamingStrategy

### Enhanced Naming Strategies

The JDBC sink connector includes enhanced naming strategies that provide more flexible naming transformations for collections (tables) and columns:

#### Enhanced Collection Naming

The enhanced collection naming strategy allows you to:
* Apply various naming styles (snake_case, camelCase, kebab-case, UPPERCASE, lowercase)
* Add prefixes and suffixes to table names
* Extract table names intelligently from topics with formats like `server.schema.table`

To enable enhanced collection naming:

```properties
# Enable enhanced collection naming
collection.naming.style.enabled=true

# Configure naming style - choose one of: default, snake_case, camel_case, kebab-case, upper_case, lower_case
collection.naming.style=snake_case

# Optional prefix and suffix
collection.naming.prefix=app_
collection.naming.suffix=_table
```

For example, with the configuration above, a topic named ServerInventory.ProductItems would be transformed to a table named app_product_items_table.

Enhanced Column Naming
Similarly, column names can be transformed with styles, prefixes, and suffixes:
```
# Enable enhanced column naming
column.naming.style.enabled=true

# Configure naming style - choose one of: default, snake_case, camel_case, kebab-case, upper_case, lower_case
column.naming.style=camel_case

# Optional prefix and suffix
column.naming.prefix=db_
column.naming.suffix=_col
```

### Relational model

The JDBC sink connector maintains an in-memory relational model, similar to Debezium source connectors.
These relational model classes can be found in the `io.debezium.connector.jdbc.relational` package.

## Building the JDBC sink connector

The following is required in order to work with the Debezium JDBC sink connector code base, and to build it locally:

* [Git](https://git-scm.com) 2.2.1 or later
* JDK 21 or later, e.g. [OpenJDK](http://openjdk.java.net/projects/jdk)
* [Docker Engine](https://docs.docker.com/engine/install/) or [Docker Desktop](https://docs.docker.com/desktop/) 1.9 or later
* [Apache Maven](https://maven.apache.org/index.html) 3.9.8 or later
  (or invoke the wrapper with `.mvnw` for Maven commands)

### Why Docker?

The test suite is heavily based on TestContainer usage, and automatically starts a variety of source and sink databases automatically.
Without a Docker-compatible environment, the integration tests will not run.
If you don't have a Docker environment, you can skip the integration tests by using the `-DskipITs` command line argument, shown below:

    $ ./mvnw clean verify -DskipITs

### Running the tests

There are three types of types in the test suite:

* Unit tests
* Sink-based integration tests
* End to end matrix-based Integration tests

By default all unit tests are executed as a part of the build.
The sink-based integration tests are only executed for MySQL, PostgreSQL, and SQL Server by default, while none of the end-to-end matrix-based tests are executed.

In order to execute the sink-based integration tests for Oracle and DB2, the `-Dtest.tags` argument must be provided to include these in the build.
In order to do this, add all the integration tests to be executed, as shown below for all databases:

    $ ./mvnw clean install -Dtest.tags=it-mysql,it-postgresql,it-sqlserver,it-oracle,it-db2

In order to run all sink-based integration tests for all databases, a short-cut tag is provided:

    $ ./mvnw clean install -Dtest.tags=it

Similarly, in order to enable specific end to end tests, the `-Dtest.tags` argument can also be supplied with the necessary tags for each sink database type:

    $ ./mvnw clean install -Dtest.tags=e2e-mysql,e2e-postgresql,e2e-sqlserver,e2e-oracle,e2e-db2

In order to run all end to end integration tests, a short-cut tag is provided as well:

    $ ./mvnw clean install -Dtest.tags=e2e

In order to run all tests for all source/sink combinations:

    $ ./mvnw clean install -Dtest.tags=all

