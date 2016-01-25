[![Build Status](https://travis-ci.org/debezium/debezium.svg?branch=master)](https://travis-ci.org/debezium/debezium)
Copyright 2008-2015 Red Hat and contributors. Licensed under the Apache License, Version 2.0.

# Debezium

Debezium is an open source project for change data capture tools. With its libraries, your application can monitor databases and receive detailed events for each row-level change made to the database. Only committed changes are visible, so your application doesn't have to worry about transactions or changes that are rolled back. Debezium provides a single model of all change events, so your application does not have to worry about the intricacies of each kind of database management system. Additionally, when your application restarts it is able to consume all of the events it missed while it was not running, ensuring that all events are processed correctly and completely.

Monitoring databases and being notified when data changes has always been complicated. Relational database triggers can be useful, but are often limited to updating state within the same database (not communicating with external processes) and often don't work with views or schema tables. Some databases offer APIs or frameworks for monitoring changes, but there is no standard so each database's approach is different and requires a lot of knowledged and specialized code.

Debezium provides modules that do this work for you. Some modules are generic and work with multiple database management systems, but are also a bit more limited in functionality and performance. Other modules are tailored for specific database management systems, so they are often far more capable and they leverage the specific features of the system. 

## Common use cases

There are a number of scenarios in which Debezium can be extremely valuable, but here we outline just a few of the more common ones.

### Cache invalidation

Automatically purge and/or update a second-level cache when the record(s) for entries change or are removed. The cache and monitoring logic can be in separate processes from the application(s).

### Updating multiple data systems

Many applications need to update a database and then do additional work after the commit, such as update separate search indexes, update a cache, send notifications, activate business logic, etc. If the application were to crash after the commit but before all of these other activities are performed, updates might be lost or other systems might become inconsistent or invalid. Using change data capture, these other activities can be performed in separate threads or separate processes/services when the data is committed in the original database. This approach is more tolerant of failures, does not miss events, scales better, and more easily supports upgrading and operations.

### Pre-computed 

### Event processing

### 


## Requirements

The following software is required to work with the Debezium codebase and build it locally:

* [Git 2.2.1](https://git-scm.com) or later
* [JDK 8](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html) or [OpenJDK 8](http://openjdk.java.net/projects/jdk8/)
* [Maven 3.2.1](https://maven.apache.org/index.html) or later
* [Docker Engine 1.4](http://docs.docker.com/engine/installation/) or later

See the links above for installation instructions on your platform.

### Why Docker?

Many open source software projects use Java and Maven, but requiring Docker is less common. Debezium is designed to talk to a number of external systems, such as various databases and services, and our integration tests verify Debezium does this correctly. But rather than expect you have all of these software systems installed locally, Debezium's build system uses Docker to automatically download or create the necessary images and start containers for each of the systems. The integration tests can then use these services and verify Debezium behaves as expected, and when the integration tests finish, Debezum's build will automatically stop any containers that it started.

Debezium also has a few modules that are not written in Java, and so they have to be required on the target operating system. Docker lets our build do this using images with the target operating system(s) and all necessary development tools.

Using Docker has several advantages:

1. You don't have to install, configure, and run specific versions of each external services on your local machine, or have access to them on your local network. Even if you do, Debezium's build won't use them.
1. We can test multiple versions of an external service. Each module can start whatever containers it needs, so different modules can easily use different versions of the services.
1. Everyone can run complete builds locally. You don't have to rely upon a remote continuous integration server running the build in an environment set up with all the required services. 
1. All builds are consistent. When multiple developers each build the same codebase, they should see exactly the same results -- as long as they're using the same or equivalent JDK, Maven, and Docker versions. That's because the containers will be running the same versions of the services on the same operating systems. Plus, all of the tests are designed to connect to the systems running in the containers, so nobody has to fiddle with connection properties or custom configurations specific to their local environments.
1. No need to clean up the services, even if those services modify and store data locally. Docker *images* are cached, so building them reusing them to start containers is fast and consistent. However, Docker *containers* are never reused: they always start in their pristine initial state, and are discarded when they are shutdown. Integration tests rely upon containers, and so cleanup is handled automatically.

