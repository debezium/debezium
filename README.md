[![License](http://img.shields.io/:license-apache%202.0-brightgreen.svg)](http://www.apache.org/licenses/LICENSE-2.0.html)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.debezium/debezium-incubator-parent/badge.svg)](http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22io.debezium%22)
[![Build Status](https://travis-ci.com/debezium/debezium-incubator.svg?branch=master)](https://travis-ci.com/debezium/debezium-incubator/)
[![User chat](https://img.shields.io/badge/chat-users-brightgreen.svg)](https://gitter.im/debezium/user)
[![Developer chat](https://img.shields.io/badge/chat-devs-brightgreen.svg)](https://gitter.im/debezium/dev)
[![Google Group](https://img.shields.io/:mailing%20list-debezium-brightgreen.svg)](https://groups.google.com/forum/#!forum/debezium)
[![Stack Overflow](http://img.shields.io/:stack%20overflow-debezium-brightgreen.svg)](http://stackoverflow.com/questions/tagged/debezium)

Copyright Debezium Authors.
Licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0).

# Debezium Incubator

Debezium is an open source project that provides a low latency data streaming platform for change data capture (CDC).

This repository contains incubating connectors and modules which are in an **early stage of their development**.
You are encouraged to explore these connectors and test them, but typically they are not recommended yet for production usage.
E.g. the format of emitted messages may change, specific features may not be implemented yet etc.

Once connectors are deemed mature enough, they may be promoted into the Debezium main repository.

## Building Debezium Incubator Modules

Please see the [README.md](https://github.com/debezium/debezium#building-debezium) in the main repository for general instructions on building Debezium from source (prerequisites, usage of Docker etc).

### Building and testing the Db2 connector

Running `mvn install` will compile all code and run the unit and integration tests. If there are any compile problems or any of the unit tests fail, the build will stop immediately. Otherwise, the command will continue to create the module's artifacts, create the Docker image with DB2 and custom scripts, start the Docker container, run the integration tests, stop the container (even if there are integration test failures), and run checkstyle on the code. If there are still no problems, the build will then install the module's artifacts into the local Maven repository.

An *integration test* is a JUnit test class named `*IT.java` or `IT*.java` that uses a DB2 database server running in a custom Docker container based upon the [ibmcom/db2](https://hub.docker.com/r/ibmcom/db2) Docker image maintained by the DB2 team. The build will automatically start the DB2 container before the integration tests are run and automatically stop and remove it after all of the integration tests complete (regardless of whether they succeed or fail). All databases used in the integration tests are defined and populated using `*.sql` files and `*.sh` scripts in the `src/test/docker/db2-cdc-docker` directory, which are copied into the Docker image and run by DB2 upon startup. Multiple test methods within a single integration test class can reuse the same database, but generally each integration test class should use its own dedicated database(s).


You should always default to using `mvn install`, especially prior to committing changes to Git. However, there are a few situations where you may want to run a different Maven command. For details on running individual tests or inspecting the Db2 database for debugging continue reading [here](debezium-connector-db2/README.md).

### Building the Oracle connector

**Note:** The Debezium Oracle connector currently exclusively uses the XStream API for ingesting change events from the Oracle database; using this API in production requires to have a license for the Golden Gate product.
We're going to explore alternatives to XStream which may be friendly in terms of licensing.

In order to build the Debezium Oracle connector, the following prerequisites must be met:

* Oracle DB is installed, enabled for change data capturing and configured as described in the [README.md](https://github.com/debezium/oracle-vagrant-box) of the debezium-vagrant-box project
(Running Oracle in VirtualBox is not a requirement, but we found it to be the easiest in terms of set-up)
* The Instant Client is downloaded (e.g. [from here](http://www.oracle.com/technetwork/topics/linuxx86-64soft-092277.html) for Linux) and unpacked
* The _xstream.jar_ from the Instant Client directory must be installed to the local Maven repository:

```bash
mvn install:install-file \
  -DgroupId=com.oracle.instantclient \
  -DartifactId=xstreams \
  -Dversion=12.2.0.1 \
  -Dpackaging=jar \
  -Dfile=xstreams.jar
```

Then the Oracle connector can be built like so:

    $ mvn clean install -pl debezium-connector-oracle -am -Poracle -Dinstantclient.dir=/path/to/instant-client-dir

#### For Oracle 11g

To run Debezium Oracle connector with Oracle 11g, add these additional parameters. If running with Oracle 12c+, leave these parameters to default.
Note: The required configuration values will be determined automatically in a future version,
making these parameters potentially obsolete.

```json
"database.tablename.case.insensitive": "true"
"database.oracle.version": "11"
```

By default, Debezium will ignore some admin tables in Oracle 12c.
But as those tables are different in Oracle 11g, Debezium will report an error on those tables. So when use Debezium on Oracle 11g, use table white list (remember to use lower case):

```json
"table.whitelist":"orcl\\.debezium\\.(.*)"
```

Making this configuration obsolete is tracked under [DBZ-1045](https://issues.jboss.org/browse/DBZ-1045).

## Contributing

The Debezium community welcomes anyone that wants to help out in any way, whether that includes reporting problems, helping with documentation, or contributing code changes to fix bugs, add tests, or implement new features. See [this document](https://github.com/debezium/debezium/blob/master/CONTRIBUTE.md) for details.
