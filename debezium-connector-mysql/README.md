# Ingesting MySQL change events

This module defines the connector that ingests change events from MySQL databases.

## Using the MySQL connector with Kafka Connect

The MySQL connector is designed to work with [Kafka Connect](http://kafka.apache.org/documentation.html#connect) and to be deployed to a Kafka Connect runtime service. The deployed connector will monitor one or more databases and write all change events to Kafka topics, which can be independently consumed by one or more clients. Kafka Connect can be distributed to provide fault tolerance to ensure the connectors are running and continually keeping up with changes in the database.

Kafka Connect can also be run standalone as a single process, although doing so is not tolerant of failures.

## Embedding the MySQL connector

The MySQL connector can also be used as a library without Kafka or Kafka Connect, enabling applications and services to directly connect to a MySQL database and obtain the ordered change events. This approach requires the application to record the progress of the connector so that upon restart the connect can continue where it left off. Therefore, this may be a useful approach for less critical use cases. For production use cases, we highly recommend using this connector with Kafka and Kafka Connect.





## Testing

This module contains both unit tests and integration tests.

A *unit test* is a JUnit test class named `*Test.java` or `Test*.java` that never requires or uses external services, though it can use the file system and can run any components within the same JVM process. They should run very quickly, be independent of each other, and clean up after itself.

An *integration test* is a JUnit test class named `*IT.java` or `IT*.java` that uses a MySQL database server running in a custom Docker container. The build will automatically start the MySQL container before the integration tests are run and automatically stop and remove it after all of the integration tests complete (regardless of whether they suceed or fail). All databases used in the integration tests are defined and populated using `*.sql` files and `*.sh` scripts in the `src/test/docker/init` directory, which are copied into the Docker image and run in lexicographical order by MySQL upon startup. Multiple test methods within a single integration test class can reuse the same database, but generally each integration test class should use its own dedicated database(s).

Running `mvn install` will compile all code and run the unit tests. If there are any compile problems or any of the unit tests fail, the build will stop immediately. Otherwise, the command will continue to create the module's artifacts, create the Docker image with MySQL and custom scripts, start the Docker container, run the integration tests, stop the container (even if there are integration test failures), and run checkstyle on the code. If there are still no problems, the build will then install the module's artifacts into the local Maven repository.

You should always default to using `mvn install`, especially prior to committing changes to Git. However, there are a few situations where you may want to run a different Maven command.

### Running some tests

If you are trying to get the test methods in a single integration test class to pass and would rather not run *all* of the integration tests, you can instruct Maven to just run that one integration test class and to skip all of the others. For example, use the following command to run the tests in the `ConnectionIT.java` class:

    $ mvn -Dit.test=ConnectionIT install

Of course, wildcards also work:

    $ mvn -Dit.test=Connect*IT install

These commands will automatically manage the MySQL Docker container.

### Debugging tests

If you want to debug integration tests by stepping through them in your IDE, using the `mvn install` command will be problematic since it will not wait for your IDE's breakpoints. There are ways of doing this, but it is typically far easier to simply start the Docker container and leave it running so that it is available when you run the integration test(s). The following command:

    $ mvn docker:start

will start the default MySQL container based upon the [mysql/mysql-server:5.7](https://hub.docker.com/r/mysql/mysql-server/) Docker image maintained by the MySQL team, or

    $ mvn docker:start -Palt-mysql

to start the MySQL container named based upon the Docker-maintained [mysql](https://hub.docker.com/r/_/mysql/) Docker image maintained by Docker (the company). (The former is used by default because it starts a bit faster, though it doesn't have a full installation of MySQL. The latter is a full-installation whose startup is a bit slower and more verbose, and it includes tools such as `mysqldump` and `mysqlbinlog`. Both containers are named `database`, configured identically, and initialized with the same databases and content, so they should behave the same for the integration tests.)

Again, the container and database server will be initialized as usual but will continue to run. Now you can use your IDE to run/debug one or more integration tests. Just be sure that the integration tests clean up their database before (and after) each test, and that you run the tests with VM arguments that define the required system properties, including:

* `database.dbname` - the name of the database that your integration test will use; there is no default
* `database.hostname` - the IP address or name of the host where the Docker container is running; defaults to `localhost` which is likely for Linux, but on OS X and Windows Docker it will have to be set to the IP address of the VM that runs Docker (which you can find by looking at the `DOCKER_HOST` environment variable).
* `database.port` - the port on which MySQL is listening; defaults to `3306` and is what this module's Docker container uses
* `database.user` - the name of the database user; defaults to `mysql` and is correct unless your database script uses something different
* `database.password` - the password of the database user; defaults to `mysqlpw` and is correct unless your database script uses something different

For example, you can define these properties by passing these arguments to the JVM:

    -Ddatabase.dbname=<DATABASE_NAME> -Ddatabase.hostname=<DOCKER_HOST> -Ddatabase.port=3306 -Ddatabase.user=mysqluser -Ddatabase.password=mysqlpw

When you are finished running the integration tests from your IDE, you have to stop and remove the Docker container (conveniently named "database") before you can run the next build:

    $ docker stop database
    $ docker rm database

Please note that when running the MySQL database Docker container, the output is written to the Maven build output and includes several lines with `[Warning] Using a password on the command line interface can be insecure.` You can ignore these warnings, since we don't need a secure database server for our transient database testing.

### Analyzing the database

Sometimes you may want to inspect the state of the database(s) after one or more integration tests are run. The `mvn install` command runs the tests but shuts down and removes the container after the integration tests complete. To keep the container running after the integration tests complete, use this Maven command:

    $ mvn integration-test

This instructs Maven to run the normal Maven lifecycle through `integration-test`, and to stop before the `post-integration-test` phase when the Docker container is normally shut down and removed. Be aware that you will need to manually stop and remove the container (conveniently named "database") before running the build again:

    $ docker stop database
    $ docker rm database

