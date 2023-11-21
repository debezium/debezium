/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.rest;

import java.sql.SQLException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.apache.kafka.connect.connector.Connector;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.Module;
import io.debezium.connector.postgresql.PostgresConnector;
import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.relational.TableId;
import io.debezium.rest.ConnectionValidationResource;
import io.debezium.rest.FilterValidationResource;
import io.debezium.rest.SchemaResource;
import io.debezium.rest.model.DataCollection;

/**
 * A JAX-RS Resource class defining endpoints of the Debezium Postgres Connect REST Extension
 *
 */
@Path(DebeziumPostgresConnectorResource.BASE_PATH)
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class DebeziumPostgresConnectorResource implements SchemaResource, ConnectionValidationResource, FilterValidationResource {

    public static final String BASE_PATH = "/debezium/postgres";
    public static final String VERSION_ENDPOINT = "/version";

    @GET
    @Path(VERSION_ENDPOINT)
    public String getConnectorVersion() {
        return Module.version();
    }

    @Override
    public Connector getConnector() {
        return new PostgresConnector();
    }

    @Override
    public List<DataCollection> getMatchingCollections(Configuration configuration) {
        PostgresConnectorConfig config = new PostgresConnectorConfig(configuration);
        try (PostgresConnection connection = new PostgresConnection(config.getJdbcConfig(), PostgresConnection.CONNECTION_GENERAL)) {
            Set<TableId> tables;
            try {
                tables = connection.readTableNames(config.databaseName(), null, null, new String[]{ "TABLE" });

                return tables.stream()
                        .filter(tableId -> config.getTableFilters().dataCollectionFilter().isIncluded(tableId))
                        .map(tableId -> new DataCollection(tableId.schema(), tableId.table()))
                        .collect(Collectors.toList());
            }
            catch (SQLException e) {
                throw new DebeziumException(e);
            }
        }
    }

    @Override
    public String getSchemaFilePath() {
        return "/META-INF/resources/postgres.json";
    }
}
