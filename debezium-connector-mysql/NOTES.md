This module builds a custom Docker image based upon the [mysql/mysql-server:5.7](https://hub.docker.com/r/mysql/mysql-server/) Docker image, and run that by default for our integration testing. This base image is maintained and "optimized" by the MySQL development team, and excludes some of the developer and administration utilities commonly needed when working with MySQL. More importantly, it starts a bit faster and is less verbose upon initialization and startup, making it a better fit for our default builds.

The [mysql](https://hub.docker.com/r/_/mysql/) images are maintained by Docker, and provide a more complete installation with all of the development tools (including the `mysqlbinlog` utility). Startup is a lot more verbose, and only with recent images could our build easily discover when the server was finally ready (since the server is started and stopped several times).

## Using MySQL Server

As mentioned in the [README.md]() file, our Maven build can be used to start a container using either one of these images. The `mysql/mysql-server:5.7` image is used:

    $ mvn docker:start

The command leaves the container running so that you can use the running MySQL server. For example, you can establish a `bash` shell inside the container (named `database`) by using Docker in another terminal:

    $ docker exec -it database bash

Using the shell, you can view the persisted database files and log files:

    # cd /var/lib/mysql

Or you can run integration tests from your IDE, as described in detail in the [README.md]() file.

To stop and remove the `database` container, simply use the following commands:

    $ docker stop database
    $ docker rm database

or

    $ mvn docker:stop

## Using the alternative MySQL Server

To use the more complete installation of MySQL found in the `mysql:5.7` Docker image, use the `alt-server` Maven profile when starting or stopping the Docker container:

    $ mvn docker:start -Palt-server

and

    $ mvn docker:stop -Palt-server

All other functionality remains exactly the same, including the ability to run the build using this Docker image:

    $ mvn clean install -Palt-server


## Using Docker directly

Although using the Maven command is far simpler, the Maven command for the `alt-server` profile really just runs (via the Jolokia Maven plugin) a Docker command to start the container, so it's equivalent to:

    $ docker run -it --name database -p 3306:3306 -v $(pwd)/src/test/docker/alt-server:/etc/mysql/conf.d -v $(pwd)/src/test/docker/init:/docker-entrypoint-initdb.d -e MYSQL_DATABASE=mysql -e MYSQL_ROOT_PASSWORD=debezium-rocks -e MYSQL_USER=mysqluser -e MYSQL_PASSWORD=mysqlpw mysql:5.7

This will use the `mysql:5.7` image to start a new container named `database` where the MySQL instance uses the combined startup settings from `/etc/mysql/my.cnf` (defined in the Docker image) and the same local configuration file we used in the integration test MySQL container, `src/test/docker/mysql.cnf` (mounted into the container at `/etc/mysql/conf.d/mysql.cnf`). The settings from the latter file take precedence.

The second volume mount, namely `-v src/test/docker/init:/docker-entrypoint-initdb.d`, makes available all of our existing scripts inside the `src/test/docker/init` directory so that they are run upon server initialization.

The command also defines the same `mysql` database and uses the same username and password(s) as our integration test MySQL container.
