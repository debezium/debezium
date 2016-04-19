This module builds and runs two containers based upon the [mongo:3.2](https://hub.docker.com/_/mongo/) Docker image. The first _primary_ container starts MongoDB, while the second _initiator_ container initializes the replica set and then terminates.

## Using MongoDB

As mentioned in the [README.md]() file, our Maven build can be used to start a container using either one of these images. The `mongo:3.2` image is used:

    $ mvn docker:start

The command leaves the primary container running so that you can use the running MySQL server. For example, you can establish a `bash` shell inside the container (named `mongo1`) by using Docker in another terminal:

    $ docker exec -it mongo1 bash

Or you can run integration tests from your IDE, as described in detail in the [README.md]() file.

To stop and remove the `mongo1` container, simply use the following Maven command:

    $ mvn docker:stop

or use the following Docker commands:

    $ docker stop mongo1
    $ docker rm mongo1

## Using Docker directly

Although using the Maven command is far simpler, the Maven commands really just run  for the `alt-server` profile really just runs (via the Jolokia Maven plugin) a Docker command to start the container, so it's equivalent to:

    $ docker run -it --rm --name mongo mongo:latest --replSet rs0 --oplogSize=2 --enableMajorityReadConcern

This will use the `mongo:3.2` image to start a new container named `mongo`. This can be repeated multiple times to start multiple MongoDB secondary nodes:

    $ docker run -it --rm --name mongo1 mongo:latest --replSet rs0 --oplogSize=2 --enableMajorityReadConcern

    $ docker run -it --rm --name mongo2 mongo:latest --replSet rs0 --oplogSize=2 --enableMajorityReadConcern

Then, run the initiator container to initialize the replica set by assigning the `mongo` container as primary and the other containers as secondary nodes:

    $ docker run -it --rm --name mongoinit --link mongo:mongo --link mongo1:mongo1 --link mongo2:mongo2 -e REPLICASET=rs0 -e debezium/mongo-replicaset-initiator:3.2

Once the replica set is initialized, the `mongoinit` container will complete and be removed.


### Use MongoDB client

The following command can be used to manually start up a Docker container to run the MongoDB command line client:

    $ docker run -it --link mongo:mongo --rm mongo:3.2 sh -c 'exec mongo "$MONGO_PORT_27017_TCP_ADDR:$MONGO_PORT_27017_TCP_PORT"'

Note that it must be linked to the Mongo container to which it will connect.
