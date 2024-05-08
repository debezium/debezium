# Ingesting Oracle change events

This module defines the connector that ingests change events from Oracle databases.

## Using the Oracle connector with Kafka Connect

The Oracle connector is designed to work with [Kafka Connect](http://kafka.apache.org/documentation.html#connect) and to be deployed to a Kafka Connect runtime service. The deployed connector will monitor one or more schemas within a database server
and write all change events to Kafka topics, which can be independently consumed by one or more clients. Kafka Connect can be distributed to provide fault tolerance to ensure the connectors are running and continually keeping up with changes in the database.

Kafka Connect can also be run standalone as a single process, although doing so is not tolerant of failures.

## Embedding the Oracle connector

The Oracle connector can also be used as a library without Kafka or Kafka Connect, enabling applications and services to directly connect to a Oracle database and obtain the ordered change events. This approach requires the application to record the progress of the connector so that upon restart the connect can continue where it left off. Therefore, this may be a useful approach for less critical use cases. For production use cases, we highly recommend using this connector with Kafka and Kafka Connect.

## Building

Please see the [README.md](https://github.com/debezium/debezium#building-debezium) for general instructions on how to build Debezium from source (prerequisites, usage of Docker etc).

**Note**: The Debezium Oracle connector comes with two adapters, one which uses the XStream API for ingesting change events from the Oracle database.
Using this API in a production environment requires a license for the Golden Gate product.

In order to build this connector, the following pre-requisites must be met:

* Oracle DB installed, enabled for change data capture and configured as per the [README.md](https://github.com/debezium/oracle-vagrant-box) of the debezium-vagrant-box project (Running Oracle in VirtualBox is not a requirement, but we find it to be the easiest in terms of set-up).
* The Oracle Instant Client is downloaded (e.g. [from here](http://www.oracle.com/technetwork/topics/linuxx86-64soft-092277.html) for Linux) and unpacked
* The _xstreams.jar_ from the Instant Client directory must be installed in the local Maven repository.

```bash
mvn install:install-file \
  -DgroupId=com.oracle.instantclient \
  -DartifactId=xstreams \
  -Dversion=21.1.0.0 \
  -Dpackaging=jar \
  -Dfile=xstreams.jar
```

The Oracle connector can then be built one of two ways, varying on the adapter you wish to use.

* [XStreams](#xstreams)
* [LogMiner](#logminer)

<a href="#xstreams"></a>
### XStreams

```bash
mvn clean install -pl debezium-connector-oracle -am -Poracle-xstream -Dinstantclient.dir=/path/to/instant-client-dir
```

<a href="#logminer"></a>
### LogMiner

```bash
mvn clean install -pl debezium-connector-oracle -am
```

## For Oracle 11g

Additionally, the connector ignores several built-in tables and schemas in Oracle 12+ but those tables differ in Oracle 11.
When using Debezium Oracle connector with Oracle 11, its important to specify the `table.include.list`, as shown below:

```json
"table.include.list": "ORCL\\\\.DEBEZIUM\\\\.(.*)"
```

## Testing

This module contains both unit tests and integration tests.

A *unit test* is a JUnit test class named `*Test.java` or `Test*.java` that never requires or uses external services, though it can use the file system and can run any components within the same JVM process. They should run very quickly, be independent of each other, and clean up after itself.

An *integration test* is a JUnit test class named `*IT.java` or `IT*.java` that uses a Oracle database server running in a custom Docker container, see [debezium/oracle-vagrant-box](https://github.com/debezium/oracle-vagrant-box) maintained by the Debezium team.
The docker image can be built to run a variety of Oracle versions and must be configured and built prior to running integration tests.

Running `mvn install` will compile all code and run the unit and integration tests. If there are any compile problems or any of the unit tests fail, the build will stop immediately. Otherwise, the command will continue to create the module's artifacts, run the integration tests, and run checkstyle on the code. If there are still no problems, the build will then install the module's artifacts into the local Maven repository.

You should always default to using `mvn install`, especially prior to committing changes to Git. However, there are a few situations where you may want to run a different Maven command.

### Running some tests

If you are trying to get the test methods in a single integration test class to pass and would rather not run *all* of the integration tests, you can instruct Maven to just run that one integration test class and to skip all of the others. For example, use the following command to run the tests in the `OracleConnectorIT.java` class:

    $ mvn -Dit.test=OracleConnectorIT install

Of course, wildcards also work:

    $ mvn -Dit.test=OracleConn*IT install

These commands do not manage the Oracle database Docker container and therefore the Oracle database must be configured and started prior to running these commands.

### Testing with Oracle using CDB vs non-CDB mode

The Debezium Oracle connector test suite assumes that the installed Oracle database is in CDB mode, meaning that the database supported pluggable databases.
This is the default installation used by Oracle 12 or later.
In order to run the Debezium Oracle connector tests against a non-CDB installation, the `database.pdb.name` argument must be explicitly specified with an empty value.0

To test with Logminer, use:

```bash
mvn clean install -pl debezium-connector-oracle -am -Poracle-tests -Ddatabase.pdb.name=
```

To test with Xstream, use:

```bash
mvn clean install -pl debezium-connector-oracle -am -Poracle-xstream,oracle-tests -Dinstantclient.dir=/path/to/instant-client-dir -Ddatabase.pdb.name=
```

The test suite automatically detects the provided `database.pdb.name` parameter with no value and interprets this to mean non-CDB mode.
