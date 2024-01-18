# Ingesting PostgreSQL change events

This module defines the connector that ingests change events from PostgreSQL databases.

## Using the PostgreSQL connector with Kafka Connect

The PostgreSQL connector is designed to work with [Kafka Connect](http://kafka.apache.org/documentation.html#connect) and to be deployed to a Kafka Connect runtime service. The deployed connector will monitor one or more schemas within a database server
and write all change events to Kafka topics, which can be independently consumed by one or more clients. Kafka Connect can be distributed to provide fault tolerance to ensure the connectors are running and continually keeping up with changes in the database.

Kafka Connect can also be run standalone as a single process, although doing so is not tolerant of failures.

## Embedding the PostgreSQL connector

The PostgreSQL connector can also be used as a library without Kafka or Kafka Connect, enabling applications and services to directly connect to a PostgreSQL database and obtain the ordered change events. This approach requires the application to record the progress of the connector so that upon restart the connect can continue where it left off. Therefore, this may be a useful approach for less critical use cases. For production use cases, we highly recommend using this connector with Kafka and Kafka Connect.

## Testing

This module contains both unit tests and integration tests.

A *unit test* is a JUnit test class named `*Test.java` or `Test*.java` that never requires or uses external services, though it can use the file system and can run any components within the same JVM process. They should run very quickly, be independent of each other, and clean up after itself.

An *integration test* is a JUnit test class named `*IT.java` or `IT*.java` that uses a PostgreSQL database server running in a custom Docker container based upon the [quay.io/debezium/postgres:10](https://github.com/debezium/container-images/tree/main/postgres/10) Docker image maintained by the Debezium team.
This docker image uses a default PostgreSQL 10 image on top of which it installs the [Debezium Logical Decoding plugin](https://github.com/debezium/postgres-decoderbufs) which is required in order to be able to receive database events.
The build will automatically start the PostgreSQL container before the integration tests are run and automatically stop and remove it after all of the integration tests complete (regardless of whether they succeed or fail).

Running `mvn install` will compile all code and run the unit and integration tests. If there are any compile problems or any of the unit tests fail, the build will stop immediately. Otherwise, the command will continue to create the module's artifacts, create the Docker image with PostgreSQL and custom scripts, start the Docker container, run the integration tests, stop the container (even if there are integration test failures), and run checkstyle on the code. If there are still no problems, the build will then install the module's artifacts into the local Maven repository.

You should always default to using `mvn install`, especially prior to committing changes to Git. However, there are a few situations where you may want to run a different Maven command.

### Running some tests

If you are trying to get the test methods in a single integration test class to pass and would rather not run *all* of the integration tests, you can instruct Maven to just run that one integration test class and to skip all of the others. For example, use the following command to run the tests in the `ConnectionIT.java` class:

    $ mvn -Dit.test=ConnectionIT install

Of course, wildcards also work:

    $ mvn -Dit.test=Connect*IT install

These commands will automatically manage the PostgreSQL Docker container.

### Debugging tests

If you want to debug integration tests by stepping through them in your IDE, using the `mvn install` command will be problematic since it will not wait for your IDE's breakpoints. There are ways of doing this, but it is typically far easier to simply start the Docker container and leave it running so that it is available when you run the integration test(s). The following command:

    $ mvn docker:start

will start the default PostgreSQL container and run the database server. Now you can use your IDE to run/debug one or more integration tests. Just be sure that the integration tests clean up their database before (and after) each test, and that you run the tests with VM arguments that define the required system properties, including:

* `database.dbname` - the name of the database that your integration test will use; default is `postgres`
* `database.hostname` - the IP address or name of the host where the Docker container is running; defaults to `localhost` which is likely for Linux, but on OS X and Windows Docker it will have to be set to the IP address of the VM that runs Docker (which you can find by looking at the `DOCKER_HOST` environment variable).
* `database.port` - the port on which PostgreSQL is listening; defaults to `5432` and is what this module's Docker container uses
* `database.user` - the name of the database user; defaults to `postgres` and is correct unless your database script uses something different
* `database.password` - the password of the database user; defaults to `postgres` and is correct unless your database script uses something different

For example, you can define these properties by passing these arguments to the JVM:

    -Ddatabase.dbname=<DATABASE_NAME> -Ddatabase.hostname=<DOCKER_HOST> -Ddatabase.port=5432 -Ddatabase.user=postgres -Ddatabase.password=postgres

When you are finished running the integration tests from your IDE, you have to stop and remove the Docker container before you can run the next build:

    $ mvn docker:stop


### Analyzing the database

Sometimes you may want to inspect the state of the database(s) after one or more integration tests are run. The `mvn install` command runs the tests but shuts down and removes the container after the integration tests complete. To keep the container running after the integration tests complete, use this Maven command:

    $ mvn integration-test

### Stopping the Docker container

This instructs Maven to run the normal Maven lifecycle through `integration-test`, and to stop before the `post-integration-test` phase when the Docker container is normally shut down and removed. Be aware that you will need to manually stop and remove the container before running the build again:

    $ mvn docker:stop
