== Ingesting PostgreSQL


== Unit and integration tests

This module contains both unit tests and integration tests.

A *unit test* is a JUnit test class named `*Test.java` or `Test*.java` that never requires or uses external services, though it can use the file system and can run any components within the same JVM process. They should run very quickly, be independent of each other, and clean up after itself.

An *integration test* is a JUnit test class named `*IT.java` or `IT*.java` that uses one or more PostgreSQL databases running in a custom Docker container automatically started before the integration tests are run and automatically stopped and removed after all of the integration tests complete (regardless of whether they suceed or fail). All databases used in the integration tests are defined and populated using `*.sql` files and `*.sh` scripts in the `src/test/docker` directory, which are copied into the Docker image and run (in lexicographical order) by PostgreSQL upon startup. Multiple test methods within a single integration test class can reuse the same database, but generally each integration test class should use its own dedicated database(s).

Running `mvn install` will compile all code and run the unit tests. If there are any problems, such as failing unit tests, the build will stop immediately. Otherwise, the build will create the module's artifacts, create the Docker image with PostgreSQL, start the Docker container, run the integration tests, and stop the container even if there are integration test failures. If there are no problems, the build will end by installing the artifacts into the local Maven repository.

You should always default to using `mvn install`, especially prior to committing changes to Git. However, there are a few situations where you may want to run a different Maven command:

* Normally, the PostgreSQL Docker container is stopped and removed after the integration tests are run. If instead you want to inspect the state of the database(s) after the integration tests are run, you can use `mvn integration-test` to instruct Maven to run the normal Maven lifecycle but stop before shutting down the Docker container during the `post-integration-test` phase. Be aware that you will need to manually stop and remove the container before running the build again, and to make this more convenient we give the PostgreSQL container the alias `database`:

    $ docker stop database
    $ docker rm database

* If you are trying to get the test methods in a single integration test class to pass and would rather not run *all* of the integration tests, you can instruct Maven to just run that one integration test class and to skip all of the others. For example, use the following command to run the tests in the `ConnectionIT.java` class:

    $ mvn -Dit.test=ConnectionIT install
