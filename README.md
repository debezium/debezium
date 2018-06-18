[![License](http://img.shields.io/:license-apache%202.0-brightgreen.svg)](http://www.apache.org/licenses/LICENSE-2.0.html)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.debezium/debezium-parent/badge.svg)](http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22io.debezium%22)
[![Build Status](https://travis-ci.org/debezium/debezium.svg?branch=master)](https://travis-ci.org/debezium/debezium)
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

### Building the Oracle connector

**Note:** The Debezium Oracle connector currently exclusively uses the XStream API for ingesting change events from the Oracle database; using this API in production requires to have a license for the Golden Gate product.
We're going to explore alternatives to XStream which may be friendly in terms of licensing.

In order to build the Debezium Oracle connector, the following prerequisites must be met:

* Oracle DB is installed, enabled for change data capturing and configured as described in the [README.md](https://github.com/debezium/oracle-vagrant-box) of the debezium-vagrant-box project
(Running Oracle in VirtualBox is not a requirement, but we found it to be the easiest in terms of set-up)
* The Instant Client is downloaded (e.g. [from here](http://www.oracle.com/technetwork/topics/linuxx86-64soft-092277.html) for Linux) and unpacked
* The _xstream.jar_ and _ojdbc8.jar_ from the Instant Client directory must be installed to the local Maven repository:
  * mvn install:install-file \
  -DgroupId=com.oracle.instantclient \
  -DartifactId=ojdbc8 \
  -Dversion=12.1.0.2 \
  -Dpackaging=jar \
  -Dfile=ojdbc8.jar`
  * mvn install:install-file \
  -DgroupId=com.oracle.instantclient \
  -DartifactId=xstreams \
  -Dversion=12.1.0.2 \
  -Dpackaging=jar \
  -Dfile=xstreams.jar

Then the Oracle connector can be built like so:

    $ mvn clean install -pl debezium-connector-oracle -am -Poracle -Dinstantclient.dir=/path/to/instant-client-dir

## Contributing

The Debezium community welcomes anyone that wants to help out in any way, whether that includes reporting problems, helping with documentation, or contributing code changes to fix bugs, add tests, or implement new features. See [this document](CONTRIBUTE.md) for details.
