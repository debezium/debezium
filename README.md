[![License](http://img.shields.io/:license-apache%202.0-brightgreen.svg)](http://www.apache.org/licenses/LICENSE-2.0.html)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.debezium/debezium-parent/badge.svg)](http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22io.debezium%22)
[![Build Status](https://travis-ci.org/debezium/debezium.svg?branch=master)](https://travis-ci.org/debezium/debezium)
[![User chat](https://img.shields.io/badge/chat-users-brightgreen.svg)](https://gitter.im/debezium/user)
[![Developer chat](https://img.shields.io/badge/chat-devs-brightgreen.svg)](https://gitter.im/debezium/dev)
[![Google Group](https://img.shields.io/:mailing%20list-debezium-brightgreen.svg)](https://groups.google.com/forum/#!forum/debezium)
[![Stack Overflow](http://img.shields.io/:stack%20overflow-debezium-brightgreen.svg)](http://stackoverflow.com/questions/tagged/debezium)

Copyright Debezium Authors.
Licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0).
The Antlr grammars within the debezium-ddl-parser module are licensed under the [MIT License](https://opensource.org/licenses/MIT).

English | [Chinese](README_ZH.md)

# Debezium

Debezium is an open source project that provides a low latency data streaming platform for change data capture (CDC). You setup and configure Debezium to monitor your databases, and then your applications consume events for each row-level change made to the database. Only committed changes are visible, so your application doesn't have to worry about transactions or changes that are rolled back. Debezium provides a single model of all change events, so your application does not have to worry about the intricacies of each kind of database management system. Additionally, since Debezium records the history of data changes in durable, replicated logs, your application can be stopped and restarted at any time, and it will be able to consume all of the events it missed while it was not running, ensuring that all events are processed correctly and completely.

Monitoring databases and being notified when data changes has always been complicated. Relational database triggers can be useful, but are specific to each database and often limited to updating state within the same database (not communicating with external processes). Some databases offer APIs or frameworks for monitoring changes, but there is no standard so each database's approach is different and requires a lot of knowledged and specialized code. It still is very challenging to ensure that all changes are seen and processed in the same order while minimally impacting the database.

Debezium provides modules that do this work for you. Some modules are generic and work with multiple database management systems, but are also a bit more limited in functionality and performance. Other modules are tailored for specific database management systems, so they are often far more capable and they leverage the specific features of the system.

## Basic architecture

Debezium is a change data capture (CDC) platform that achieves its durability, reliability, and fault tolerance qualities by reusing Kafka and Kafka Connect. Each connector deployed to the Kafka Connect distributed, scalable, fault tolerant service monitors a single upstream database server, capturing all of the changes and recording them in one or more Kafka topics (typically one topic per database table). Kafka ensures that all of these data change events are replicated and totally ordered, and allows many clients to independently consume these same data change events with little impact on the upstream system. Additionally, clients can stop consuming at any time, and when they restart they resume exactly where they left off. Each client can determine whether they want exactly-once or at-least-once delivery of all data change events, and all data change events for each database/table are delivered in the same order they occurred in the upstream database.

Applications that don't need or want this level of fault tolerance, performance, scalability, and reliability can instead use Debezium's *embedded connector engine* to run a connector directly within the application space. They still want the same data change events, but prefer to have the connectors send them directly to the application rather than persist them inside Kafka.

## Common use cases

There are a number of scenarios in which Debezium can be extremely valuable, but here we outline just a few of them that are more common.

### Cache invalidation

Automatically invalidate entries in a cache as soon as the record(s) for entries change or are removed. If the cache is running in a separate process (e.g., Redis, Memcache, Infinispan, and others), then the simple cache invalidation logic can be placed into a separate process or service, simplifying the main application. In some situations, the logic can be made a little more sophisticated and can use the updated data in the change events to update the affected cache entries.

### Simplifying monolithic applications

Many applications update a database and then do additional work after the changes are committed: update search indexes, update a cache, send notifications, run business logic, etc. This is often called "dual-writes" since the application is writing to multiple systems outside of a single transaction. Not only is the application logic complex and more difficult to maintain, dual writes also risk losing data or making the various systems inconsistent if the application were to crash after a commit but before some/all of the other updates were performed. Using change data capture, these other activities can be performed in separate threads or separate processes/services when the data is committed in the original database. This approach is more tolerant of failures, does not miss events, scales better, and more easily supports upgrading and operations.

### Sharing databases

When multiple applications share a single database, it is often non-trivial for one application to become aware of the changes committed by another application. One approach is to use a message bus, although non-transactional message busses suffer from the "dual-writes" problems mentioned above. However, this becomes very straightforward with Debezium: each application can monitor the database and react to the changes.

### Data integration

Data is often stored in multiple places, especially when it is used for different purposes and has slightly different forms. Keeping the multiple systems synchronized can be challenging, but simple ETL-type solutions can be implemented quickly with Debezium and simple event processing logic.

### CQRS

The [Command Query Responsibility Separation (CQRS)](http://martinfowler.com/bliki/CQRS.html) architectural pattern uses a one data model for updating and one or more other data models for reading. As changes are recorded on the update-side, those changes are then processed and used to update the various read representations. As a result CQRS applications are usually more complicated, especially when they need to ensure reliable and totally-ordered processing. Debezium and CDC can make this more approachable: writes are recorded as normal, but Debezium captures those changes in durable, totally ordered streams that are consumed by the services that asynchronously update the read-only views. The write-side tables can represent domain-oriented entities, or when CQRS is paired with [Event Sourcing](http://martinfowler.com/eaaDev/EventSourcing.html) the write-side tables are the append-only event log of commands.

## Building Debezium

The following software is required to work with the Debezium codebase and build it locally:

* [Git 2.2.1](https://git-scm.com) or later
* [JDK 8](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html) or [OpenJDK 8](http://openjdk.java.net/projects/jdk8/)
* [Maven 3.2.1](https://maven.apache.org/index.html) or later
* [Docker Engine 1.9](http://docs.docker.com/engine/installation/) or later

See the links above for installation instructions on your platform. You can verify the versions are installed and running:

    $ git --version
    $ javac -version
    $ mvn -version
    $ docker --version

### Why Docker?

Many open source software projects use Git, Java, and Maven, but requiring Docker is less common. Debezium is designed to talk to a number of external systems, such as various databases and services, and our integration tests verify Debezium does this correctly. But rather than expect you have all of these software systems installed locally, Debezium's build system uses Docker to automatically download or create the necessary images and start containers for each of the systems. The integration tests can then use these services and verify Debezium behaves as expected, and when the integration tests finish, Debezium's build will automatically stop any containers that it started.

Debezium also has a few modules that are not written in Java, and so they have to be required on the target operating system. Docker lets our build do this using images with the target operating system(s) and all necessary development tools.

Using Docker has several advantages:

1. You don't have to install, configure, and run specific versions of each external services on your local machine, or have access to them on your local network. Even if you do, Debezium's build won't use them.
1. We can test multiple versions of an external service. Each module can start whatever containers it needs, so different modules can easily use different versions of the services.
1. Everyone can run complete builds locally. You don't have to rely upon a remote continuous integration server running the build in an environment set up with all the required services.
1. All builds are consistent. When multiple developers each build the same codebase, they should see exactly the same results -- as long as they're using the same or equivalent JDK, Maven, and Docker versions. That's because the containers will be running the same versions of the services on the same operating systems. Plus, all of the tests are designed to connect to the systems running in the containers, so nobody has to fiddle with connection properties or custom configurations specific to their local environments.
1. No need to clean up the services, even if those services modify and store data locally. Docker *images* are cached, so reusing them to start containers is fast and consistent. However, Docker *containers* are never reused: they always start in their pristine initial state, and are discarded when they are shutdown. Integration tests rely upon containers, and so cleanup is handled automatically.

### Configure your Docker environment

The Docker Maven Plugin will resolve the docker host by checking the following environment variables:

    export DOCKER_HOST=tcp://10.1.2.2:2376
    export DOCKER_CERT_PATH=/path/to/cdk/.vagrant/machines/default/virtualbox/.docker
    export DOCKER_TLS_VERIFY=1

These can be set automatically if using Docker Machine or something similar.

### Building the code

First obtain the code by cloning the Git repository:

    $ git clone https://github.com/debezium/debezium.git
    $ cd debezium

Then build the code using Maven:

    $ mvn clean install

The build starts and uses several Docker containers for different DBMSes. Note that if Docker is not running or configured, you'll likely get an arcane error -- if this is the case, always verify that Docker is running, perhaps by using `docker ps` to list the running containers.

### Don't have Docker running locally for builds?

You can skip the integration tests and docker-builds with the following command:

    $ mvn clean install -DskipITs

### Running tests of the Postgres connector using the wal2json or pgoutput logical decoding plug-ins

The Postgres connector supports three logical decoding plug-ins for streaming changes from the DB server to the connector: decoderbufs (the default), wal2json, and pgoutput.
To run the integration tests of the PG connector using wal2json, enable the "wal2json-decoder" build profile:

    $ mvn clean install -pl :debezium-connector-postgres -Pwal2json-decoder
    
To run the integration tests of the PG connector using pgoutput, enable the "pgoutput-decoder" and "postgres-10" build profiles:

    $ mvn clean install -pl :debezium-connector-postgres -Ppgoutput-decoder,postgres-10

A few tests currently don't pass when using the wal2json plug-in.
Look for references to the types defined in `io.debezium.connector.postgresql.DecoderDifferences` to find these tests.

### Running tests of the Postgres connector with specific Apicurio Version
To run the tests of PG connector using wal2json or pgoutput logical decoding plug-ins with a specific version of Apicurio, a test property can be passed as:

    $ mvn clean install -pl debezium-connector-postgres -Pwal2json-decoder 
          -Ddebezium.test.apicurio.version=1.3.1.Final

In absence of the property the stable version of Apicurio will be fetched.

### Running tests of the Postgres connector against an external database, e.g. Amazon RDS
Please note if you want to test against a *non-RDS* cluster, this test requires `<your user>` to be a superuser with not only `replication` but permissions
to login to `all` databases in `pg_hba.conf`.  It also requires `postgis` packages to be available on the target server for some of the tests to pass.

    $ mvn clean install -pl debezium-connector-postgres -Pwal2json-decoder \
         -Ddocker.skip.build=true -Ddocker.skip.run=true -Dpostgres.host=<your PG host> \
         -Dpostgres.user=<your user> -Dpostgres.password=<your password> \
         -Ddebezium.test.records.waittime=10

Adjust the timeout value as needed.

See [PostgreSQL on Amazon RDS](debezium-connector-postgres/RDS.md) for details on setting up a database on RDS to test against.

## Contributing

The Debezium community welcomes anyone that wants to help out in any way, whether that includes reporting problems, helping with documentation, or contributing code changes to fix bugs, add tests, or implement new features. See [this document](CONTRIBUTE.md) for details.

A big thank you to all the Debezium contributors!

<a href="https://github.com/debezium/debezium/graphs/contributors">
  <img src="https://contributors-img.web.app/image?repo=debezium/debezium" />
</a>
