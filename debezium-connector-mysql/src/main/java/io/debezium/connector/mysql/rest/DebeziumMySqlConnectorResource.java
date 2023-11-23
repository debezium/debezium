/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.rest;

import java.sql.SQLException;
import java.util.ArrayList;
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
import io.debezium.connector.mysql.Module;
import io.debezium.connector.mysql.MySqlConnector;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.connector.mysql.strategy.AbstractConnectorConnection;
import io.debezium.relational.TableId;
import io.debezium.rest.ConnectionValidationResource;
import io.debezium.rest.FilterValidationResource;
import io.debezium.rest.SchemaResource;
import io.debezium.rest.model.DataCollection;

/**
 * A JAX-RS Resource class defining endpoints of the Debezium MySQL Connect REST Extension
 *
 */
@Path(DebeziumMySqlConnectorResource.BASE_PATH)
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class DebeziumMySqlConnectorResource implements SchemaResource, ConnectionValidationResource, FilterValidationResource {

    public static final String BASE_PATH = "/debezium/mysql";
    public static final String VERSION_ENDPOINT = "/version";

    @Override
    public String getSchemaFilePath() {
        return "/META-INF/resources/mysql.json";
    }

    @Override
    public Connector getConnector() {
        return new MySqlConnector();
    }

    @Override
    public List<DataCollection> getMatchingCollections(Configuration configuration) {
        final MySqlConnectorConfig config = new MySqlConnectorConfig(configuration);
        try (AbstractConnectorConnection connection = config.getConnectorAdapter().createConnection(configuration)) {
            Set<TableId> tables;

            final List<String> databaseNames = new ArrayList<>();
            connection.query("SHOW DATABASES", rs -> {
                while (rs.next()) {
                    databaseNames.add(rs.getString(1));
                }
            });

            List<DataCollection> allMatchingTables = new ArrayList<>();

            for (String databaseName : databaseNames) {
                if (!config.getTableFilters().databaseFilter().test(databaseName)) {
                    continue;
                }
                tables = connection.readTableNames(databaseName, null, null, new String[]{ "TABLE" });
                var matchingTables = tables.stream()
                        .filter(tableId -> config.getTableFilters().dataCollectionFilter().isIncluded(tableId))
                        .map(tableId -> new DataCollection(tableId.catalog(), tableId.table()))
                        .collect(Collectors.toList());
                allMatchingTables.addAll(matchingTables);
            }
            return allMatchingTables;
        }
        catch (SQLException e) {
            throw new DebeziumException(e);
        }
    }

    @GET
    @Path(VERSION_ENDPOINT)
    public String getConnectorVersion() {
        return Module.version();
    }

}
