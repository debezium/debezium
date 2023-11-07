/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.rest;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.apache.kafka.connect.connector.Connector;

import io.debezium.DebeziumException;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.mongodb.CollectionId;
import io.debezium.connector.mongodb.Module;
import io.debezium.connector.mongodb.MongoDbConnector;
import io.debezium.connector.mongodb.MongoDbConnectorConfig;
import io.debezium.connector.mongodb.MongoDbTaskContext;
import io.debezium.connector.mongodb.connection.MongoDbConnection;
import io.debezium.connector.mongodb.connection.ReplicaSet;
import io.debezium.rest.ConnectionValidationResource;
import io.debezium.rest.DataCollection;
import io.debezium.rest.FilterValidationResource;
import io.debezium.rest.SchemaResource;
import io.debezium.spi.schema.DataCollectionId;

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

    protected MongoDbConnection primary(MongoDbTaskContext context) throws DebeziumException {
        ReplicaSet replicaSet = new ReplicaSet(context.getConnectionContext().connectionString());
        return context.getConnectionContext().connect(replicaSet, context.filters(), (s, throwable) -> {
            throw new DebeziumException(s, throwable);
        });
    }

    protected <T extends DataCollectionId> Stream<T> determineDataCollectionsToBeSnapshotted(
                                                                                             CommonConnectorConfig connectorConfig,
                                                                                             final Collection<T> allDataCollections) {
        final Set<Pattern> snapshotAllowedDataCollections = connectorConfig.getDataCollectionsToBeSnapshotted()
                .stream()
                .map(regex -> Pattern.compile(regex, Pattern.CASE_INSENSITIVE))
                .collect(Collectors.toSet());
        if (snapshotAllowedDataCollections.size() == 0) {
            return allDataCollections.stream();
        }
        else {
            return allDataCollections.stream()
                    .filter(dataCollectionId -> snapshotAllowedDataCollections.stream()
                            .anyMatch(s -> s.matcher(dataCollectionId.identifier()).matches()));
        }
    }

    @Override
    public List<DataCollection> getMatchingCollections(Configuration configuration) {
        MongoDbTaskContext context = new MongoDbTaskContext(configuration);
        MongoDbConnectorConfig config = new MongoDbConnectorConfig(configuration);

        List<CollectionId> collections;
        try (MongoDbConnection primary = primary(context)) {
            collections = determineDataCollectionsToBeSnapshotted(config, primary.collections()).collect(Collectors.toList());
        }
        catch (InterruptedException e) {
            throw new DebeziumException(e);
        }

        return collections.stream()
                .map(collectionId -> new DataCollection(collectionId.replicaSetName(), collectionId.dbName(), collectionId.name()))
                .collect(Collectors.toList());
    }

    @Override
    public String getSchemaFilePath() {
        return "/META-INF/resources/mongodb.json";
    }

}
