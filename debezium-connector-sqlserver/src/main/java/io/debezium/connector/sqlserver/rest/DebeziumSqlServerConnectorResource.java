/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver.rest;

import java.sql.SQLException;
import java.util.Collections;
import java.util.HashSet;
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
import io.debezium.connector.sqlserver.Module;
import io.debezium.connector.sqlserver.SqlServerConnection;
import io.debezium.connector.sqlserver.SqlServerConnector;
import io.debezium.connector.sqlserver.SqlServerConnectorConfig;
import io.debezium.relational.TableId;
import io.debezium.rest.ConnectionValidationResource;
import io.debezium.rest.DataCollection;
import io.debezium.rest.FilterValidationResource;
import io.debezium.rest.SchemaResource;

/**
 * A JAX-RS Resource class defining endpoints of the Debezium SQL Server Connect REST Extension
 *
 */
@Path(DebeziumSqlServerConnectorResource.BASE_PATH)
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class DebeziumSqlServerConnectorResource implements SchemaResource, ConnectionValidationResource, FilterValidationResource {

    public static final String BASE_PATH = "/debezium/sqlserver";
    public static final String VERSION_ENDPOINT = "/version";

    @GET
    @Path(VERSION_ENDPOINT)
    public String getConnectorVersion() {
        return Module.version();
    }

    @Override
    public Connector getConnector() {
        return new SqlServerConnector();
    }

    @Override
    public List<DataCollection> getMatchingCollections(Configuration configuration) {
        final SqlServerConnectorConfig sqlServerConfig = new SqlServerConnectorConfig(configuration);
        final List<String> databaseNames = sqlServerConfig.getDatabaseNames();

        try (SqlServerConnection connection = new SqlServerConnection(sqlServerConfig, null, Collections.emptySet(), sqlServerConfig.useSingleDatabase())) {
            Set<TableId> tables = new HashSet<>();
            databaseNames.forEach(databaseName -> {
                try {
                    tables.addAll(connection.readTableNames(databaseName, null, null, new String[]{ "TABLE" }));
                }
                catch (SQLException e) {
                    throw new DebeziumException(e);
                }
            });

            return tables.stream()
                    .filter(tableId -> sqlServerConfig.getTableFilters().dataCollectionFilter().isIncluded(tableId))
                    .map(tableId -> new DataCollection(tableId.catalog(), tableId.schema(), tableId.table()))
                    .collect(Collectors.toList());
        }
        catch (SQLException e) {
            throw new RuntimeException("Could not retrieve real database name", e);
        }
    }

    @Override
    public String getSchemaFilePath() {
        return "/META-INF/resources/sqlserver.json";
    }

}
