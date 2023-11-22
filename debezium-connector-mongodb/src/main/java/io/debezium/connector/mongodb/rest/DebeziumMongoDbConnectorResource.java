/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.rest;

import java.util.List;
import java.util.stream.Collectors;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.apache.kafka.connect.connector.Connector;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.connector.mongodb.Module;
import io.debezium.connector.mongodb.MongoDbConnector;
import io.debezium.connector.mongodb.MongoDbTaskContext;
import io.debezium.connector.mongodb.connection.MongoDbConnection;
import io.debezium.connector.mongodb.connection.ReplicaSet;
import io.debezium.rest.ConnectionValidationResource;
import io.debezium.rest.FilterValidationResource;
import io.debezium.rest.SchemaResource;
import io.debezium.rest.model.DataCollection;

/**
 * A JAX-RS Resource class defining endpoints of the Debezium MongoDB Connect REST Extension
 *
 */
@Path(DebeziumMongoDbConnectorResource.BASE_PATH)
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class DebeziumMongoDbConnectorResource implements SchemaResource, ConnectionValidationResource, FilterValidationResource {

    public static final String BASE_PATH = "/debezium/mongodb";
    public static final String VERSION_ENDPOINT = "/version";

    @GET
    @Path(VERSION_ENDPOINT)
    public String getConnectorVersion() {
        return Module.version();
    }

    @Override
    public Connector getConnector() {
        return new MongoDbConnector();
    }

    protected MongoDbConnection getConnection(Configuration configuration) {
        MongoDbTaskContext context = new MongoDbTaskContext(configuration);
        ReplicaSet replicaSet = new ReplicaSet(context.getConnectionContext().connectionString());
        return context.getConnectionContext().connect(replicaSet, context.filters(), (s, throwable) -> {
            throw new DebeziumException(s, throwable);
        });
    }

    @Override
    public List<DataCollection> getMatchingCollections(Configuration configuration) {
        try (MongoDbConnection connection = getConnection(configuration)) {
            return connection.collections().stream()
                    .map(collectionId -> new DataCollection(collectionId.replicaSetName(), collectionId.dbName(), collectionId.name()))
                    .collect(Collectors.toList());
        }
        catch (InterruptedException e) {
            throw new DebeziumException(e);
        }
    }

    @Override
    public String getSchemaFilePath() {
        return "/META-INF/resources/mongodb.json";
    }

}
