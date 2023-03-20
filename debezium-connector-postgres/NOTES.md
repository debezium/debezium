This module uses the [quay.io/debezium/postgres:10.0](https://github.com/debezium/container-images/tree/main/postgres/10) container image maintained by the Debezium team. This image uses a default PostgreSQL 10.0 image on top of which it installs the [Debezium Logical Decoding plugin](https://github.com/debezium/postgres-decoderbufs) which is required in order to be able to receive database events.

## Using the PostgreSQL Server

As mentioned in the [README.md]() file, our Maven build can be used to start a container using either one of these images. The `debezium/postgres:10.0` image is used:

    $ mvn docker:start

The command leaves the container running so that you can use the running PostgreSQL server.

To stop and remove the container, simply use the following commands:

    $ mvn docker:stop

## Using Docker directly

Although using the Maven command is far simpler, the Maven command really just runs a Docker command to start the container, so it's equivalent to:

    $ docker run -it -p 5432:5432 -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=postgres quay.io/debezium/postgres:10.0 postgres

This will use the `quay.io/debezium/postgres:10.0` image to start a new container where the PostgreSQL instance uses the settings from [the container image](https://github.com/debezium/container-images/tree/main/postgres/10/postgresql.conf.sample) 