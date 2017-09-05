/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static org.junit.Assert.assertNotNull;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.debezium.config.Configuration;
import io.debezium.relational.history.FileDatabaseHistory;

public class UniqueDatabase {
    private static final String DEFAULT_DATABASE = "mysql";
    private static final String[] CREATE_DATABASE_DDL = new String[] {
            "CREATE DATABASE $DBNAME$;",
            "USE $DBNAME$;"
    };
    private static final Pattern COMMENT_PATTERN = Pattern.compile("^(.*)--.*$");

    private final String databaseName;
    private final String templateName;
    private final String serverName;
    private Path dbHistoryPath;
    private String identifier;

    public UniqueDatabase(final String serverName, final String databaseName) {
        this(serverName, databaseName, Integer.toUnsignedString(new Random().nextInt(), 36));
    }

    private UniqueDatabase(final String serverName, final String databaseName, final String identifier) {
        this.identifier = identifier;
        this.databaseName = databaseName + "_" + identifier;
        this.templateName = databaseName;
        this.serverName = serverName;
    }

    public UniqueDatabase(final String serverName, final String databaseName, final UniqueDatabase sibling) {
        this(serverName, databaseName, sibling.getIdentifier());
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

    public String qualifiedTableName(final String tableName) {
        return String.format("%s.%s", databaseName, tableName);
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
                final List<String> statements = Arrays.stream(
                        Stream.concat(
                                Arrays.stream(CREATE_DATABASE_DDL),
                                Files.readAllLines(Paths.get(ddlTestFile.toURI())).stream())
                            .map(String::trim)
                            .filter(x -> !x.startsWith("--") && !x.isEmpty())
                            .map(x -> {
                                final Matcher m = COMMENT_PATTERN.matcher(x);
                                return m.matches() ? m.group(1) : x;
                            })
                            .map(this::convertSQL)
                            .collect(Collectors.joining("\n")).split(";")
                        )
                       .map(x -> x.replace("$$", ";"))
                       .collect(Collectors.toList());
                connection.execute(statements.toArray(new String[statements.size()]));
            }
        } catch (final Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public UniqueDatabase withDbHistoryPath(final Path dbHistoryPath) {
        this.dbHistoryPath = dbHistoryPath;
        return this;
    }

    public Configuration.Builder defaultJdbcConfigBuilder() {
        return Configuration.create()
                .with(MySqlConnectorConfig.HOSTNAME, System.getProperty("database.hostname", "localhost"))
                .with(MySqlConnectorConfig.PORT, System.getProperty("database.port", "3306"))
                .with(MySqlConnectorConfig.USER, "snapper")
                .with(MySqlConnectorConfig.PASSWORD, "snapperpass");
    }

    public Configuration.Builder defaultConfig() {
        final Configuration.Builder builder = defaultJdbcConfigBuilder()
                .with(MySqlConnectorConfig.SSL_MODE, MySqlConnectorConfig.SecureConnectionMode.DISABLED)
                .with(MySqlConnectorConfig.SERVER_ID, 18765)
                .with(MySqlConnectorConfig.SERVER_NAME, getServerName())
                .with(MySqlConnectorConfig.POLL_INTERVAL_MS, 10)
                .with(MySqlConnectorConfig.DATABASE_WHITELIST, getDatabaseName())
                .with(MySqlConnectorConfig.DATABASE_HISTORY, FileDatabaseHistory.class);
        if (dbHistoryPath != null) {
            builder.with(FileDatabaseHistory.FILE_PATH, dbHistoryPath);
        }
        return builder;
    }

    public String getIdentifier() {
        return identifier;
    }
}
