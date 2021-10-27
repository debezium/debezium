This module uses the [debezium/postgres:9.6](https://github.com/debezium/docker-images/tree/main/postgres/9.6) Docker image maintained by the Debezium team. This image uses a default PostgreSQL 9.6 image on top of which it installs the [Debezium Logical Decoding plugin](https://github.com/debezium/postgres-decoderbufs) which is required in order to be able to receive database events.  

## Using the PostgreSQL Server

As mentioned in the [README.md]() file, our Maven build can be used to start a container using either one of these images. The `debezium/postgres:9.6` image is used:

    $ mvn docker:start

The command leaves the container running so that you can use the running PostgreSQL server.

To stop and remove the container, simply use the following commands:

    $ mvn docker:stop

## Using Docker directly

Although using the Maven command is far simpler, the Maven command really just runs a Docker command to start the container, so it's equivalent to:

    $ docker run -it -p 5432:5432 -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=postgres debezium/postgres:9.6 postgres

This will use the `debezium/postgres:9.6` image to start a new container where the PostgreSQL instance uses the settings from [the docker image](https://github.com/debezium/docker-images/blob/main/postgres/9.6/postgresql.conf.sample) 