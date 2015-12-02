## Ingesting MySQL


## Unit and integration tests

This module contains both unit tests and integration tests.

A *unit test* is a JUnit test class named `*Test.java` or `Test*.java` that never requires or uses external services, though it can use the file system and can run any components within the same JVM process. They should run very quickly, be independent of each other, and clean up after itself.

An *integration test* is a JUnit test class named `*IT.java` or `IT*.java` that uses one or more MySQL databases running in a custom Docker container automatically started before the integration tests are run and automatically stopped and removed after all of the integration tests complete (regardless of whether they suceed or fail). All databases used in the integration tests are defined and populated using `*.sql` files and `*.sh` scripts in the `src/test/docker` directory, which are copied into the Docker image and run (in lexicographical order) by MySQL upon startup. Multiple test methods within a single integration test class can reuse the same database, but generally each integration test class should use its own dedicated database(s).

Running `mvn install` will compile all code and run the unit tests. If there are any problems, such as failing unit tests, the build will stop immediately. Otherwise, the build will create the module's artifacts, create the Docker image with MySQL, start the Docker container, run the integration tests, and stop the container even if there are integration test failures. If there are no problems, the build will end by installing the artifacts into the local Maven repository.

You should always default to using `mvn install`, especially prior to committing changes to Git. However, there are a few situations where you may want to run a different Maven command.

### Running some tests

If you are trying to get the test methods in a single integration test class to pass and would rather not run *all* of the integration tests, you can instruct Maven to just run that one integration test class and to skip all of the others. For example, use the following command to run the tests in the `ConnectionIT.java` class:

    $ mvn -Dit.test=ConnectionIT install

Of course, wildcards also work:

    $ mvn -Dit.test=Connect*IT install

### Debugging tests

Normally, the MySQL Docker container is stopped and removed after the integration tests are run. One way to debug tests is to configure the build to wait for a remote debugging client, but then you also have to set up your IDE to connect. It's often far easier to debug a single test directly from within your IDE. To do that, you want to start the MySQL Docker container and keep it running:

    $ mvn docker:start

Then use your IDE to run one or more unit tests, optionally debugging them as needed. Just be sure that the unit tests clean up their database before (and after) each test.

To stop the container, simply use Docker to stop and remove the MySQL Docker container named `database`:

    $ docker stop database
    $ docker rm database

### Analyzing the database

Sometimes you may want to inspect the state of the database(s) after one or more integration tests are run. The `mvn install` command runs the tests but shuts down and removes the container after the tests complete. To keep the container running after the tests complete, use this Maven command:

    $ mvn integration-test

This instructs Maven to run the normal Maven lifecycle through `integration-test`, and to stop before the `post-integration-test` phase when the Docker container is normally shut down and removed. Be aware that you will need to manually stop and remove the container before running the build again, and to make this more convenient we give the MySQL container the alias `database`:

    $ docker stop database
    $ docker rm database

