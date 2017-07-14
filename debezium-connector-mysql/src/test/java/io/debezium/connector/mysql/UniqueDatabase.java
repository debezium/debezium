/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static org.junit.Assert.assertNotNull;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class UniqueDatabase {
    private static final String DEFAULT_DATABASE = "mysql";
    private static final String[] CREATE_DATABASE_DDL = new String[] {
            "CREATE DATABASE $DBNAME$;",
            "USE $DBNAME$;"
    };

    private final String databaseName;
    private final String templateName;
    private final String serverName;

    public UniqueDatabase(final String databaseName, final String serverName) {
        this.databaseName = databaseName + "_" + Integer.toUnsignedString(new Random().nextInt(), 36);
        this.templateName = databaseName;
        this.serverName = serverName;
    }

    public String convertSQL(final String sql) {
        return sql.replace("$DBNAME$", databaseName);
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String topicForTable(final String topicName) {
        return String.format("%s.%s.%s", serverName, databaseName, topicName);
    }

    protected String getServerName() {
        return serverName;
    }

    public void createAndInitialize() {
        final String ddlFile = String.format("ddl/%s.sql", templateName);
        final URL ddlTestFile = UniqueDatabase.class.getClassLoader().getResource(ddlFile);
        assertNotNull("Cannot locate " + ddlFile, ddlTestFile);
        try {
            try (MySQLConnection connection = MySQLConnection.forTestDatabase(DEFAULT_DATABASE)) {
                connection.execute(
                           Stream.concat(
                                   Arrays.stream(CREATE_DATABASE_DDL),
                                   Files.readAllLines(Paths.get(ddlTestFile.toURI())).stream())
                               .map(String::trim)
                               .filter(x -> !x.trim().startsWith("--"))
                               .map(this::convertSQL)
                               .collect(Collectors.joining()).split(";")
                );
            }
        } catch (final Exception e) {
            throw new IllegalStateException(e);
        }
        
    }
}
