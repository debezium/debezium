/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.rest;

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
import io.debezium.connector.oracle.Module;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnector;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.relational.TableId;
import io.debezium.rest.ConnectionValidationResource;
import io.debezium.rest.FilterValidationResource;
import io.debezium.rest.SchemaResource;
import io.debezium.rest.model.DataCollection;
import io.debezium.util.Strings;

/**
 * A JAX-RS Resource class defining endpoints of the Debezium Oracle Connect REST Extension
 *
 */
@Path(DebeziumOracleConnectorResource.BASE_PATH)
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class DebeziumOracleConnectorResource implements SchemaResource, ConnectionValidationResource, FilterValidationResource {

    public static final String BASE_PATH = "/debezium/oracle";
    public static final String VERSION_ENDPOINT = "/version";

    @Override
    public String getSchemaFilePath() {
        return "/META-INF/resources/oracle.json";
    }

    @Override
    public Connector getConnector() {
        return new OracleConnector();
    }

    @Override
    public List<DataCollection> getMatchingCollections(Configuration configuration) {
        final OracleConnectorConfig oracleConfig = new OracleConnectorConfig(configuration);
        final String databaseName = oracleConfig.getCatalogName();

        try (OracleConnection connection = new OracleConnection(oracleConfig.getJdbcConfig(), false)) {
            if (!Strings.isNullOrBlank(oracleConfig.getPdbName())) {
                connection.setSessionToPdb(oracleConfig.getPdbName());
            }
            Set<TableId> tables;
            // @TODO: we need to expose a better method from the connector, particularly getAllTableIds
            // the following's performance is acceptable when using PDBs but not as ideal with non-PDB
            tables = connection.readTableNames(databaseName, null, null, new String[]{ "TABLE" });

            return tables.stream()
                    .filter(tableId -> oracleConfig.getTableFilters().dataCollectionFilter().isIncluded(tableId))
                    .map(tableId -> new DataCollection(tableId.schema(), tableId.table()))
                    .collect(Collectors.toList());
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
