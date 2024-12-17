/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog.util;

import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.Configuration.Builder;
import io.debezium.connector.binlog.BinlogConnectorConfig;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.storage.file.history.FileSchemaHistory;

/**
 * Create and populate a unique instance of a binlog-based connector database for each run of a Junit test.
 * A user of class needs to provide a logical name for Debezium and the database. It is expected that there
 * is an init file in {@code src/test/resources/ddl/<database_name>.sql}.<p></p>
 *
 * The database name is enriched with a unique suffix that guarantees complete isolation between runs:
 * {@code <database_name>_<suffix>}
 *
 * @author jpechane
 * @author Chris Cranford
 */
public abstract class UniqueDatabase {

    public static final ZoneId TIMEZONE = ZoneId.of("US/Samoa");

    private static final String DEFAULT_DATABASE = "mysql";
    private static final String[] CREATE_DATABASE_DDL = new String[]{
            "CREATE DATABASE `$DBNAME$`;",
            "USE `$DBNAME$`;"
    };
    private static final String[] CREATE_DATABASE_WITH_CHARSET_DDL = new String[]{
            "CREATE DATABASE `$DBNAME$` CHARSET $CHARSET$;",
            "USE `$DBNAME$`;"
    };
    private static final Pattern COMMENT_PATTERN = Pattern.compile("^(.*)--.*$");

    private final String databaseName;
    private final String charset;
    private final String templateName;
    private final String serverName;
    private Path dbHistoryPath;
    private final String identifier;

    public UniqueDatabase(final String serverName, final String databaseName, final String identifier, final String charset) {
        this.identifier = identifier;
        this.databaseName = (identifier != null) ? (databaseName + "_" + identifier) : databaseName;
        this.templateName = databaseName;
        this.serverName = serverName;
        this.charset = charset;
    }

    /**
     * Creates an instance with given Debezium logical name and database name
     *
     * @param serverName - logical Debezium server name
     * @param databaseName - the name of the database (prix)
     */
    public UniqueDatabase(final String serverName, final String databaseName) {
        this(serverName, databaseName, Integer.toUnsignedString(new Random().nextInt(), 36), null);
    }

    /**
     * Creates an instance with given Debezium logical name and database name and a set default charset
     *
     * @param serverName - logical Debezium server name
     * @param databaseName - the name of the database (prix)
     */
    public UniqueDatabase(final String serverName, final String databaseName, final String charset) {
        this(serverName, databaseName, Integer.toUnsignedString(new Random().nextInt(), 36), charset);
    }

    /**
     * Creates an instance with given Debezium logical name and database name and id suffix same
     * as another database. This is handy for tests that need multpli databases and can use regex
     * based whitelisting.

     * @param serverName - logical Debezium server name
     * @param databaseName - the name of the database (prix)
     * @param sibling - a database whose unique suffix will be used
     */
    public UniqueDatabase(final String serverName, final String databaseName, final UniqueDatabase sibling) {
        this(serverName, databaseName, sibling.getIdentifier(), null);
    }

    private String convertSQL(final String sql) {
        final String dbReplace = sql.replace("$DBNAME$", databaseName);
        return charset != null ? dbReplace.replace("$CHARSET$", charset) : dbReplace;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    /**
     * @param tableName
     * @return Fully qualified Kafka topic name for a given table <code>&lt;serverName&gt;.&lt;databaseName&gt;.&lt;tableName&gt;</code>
     */
    public String topicForTable(final String tableName) {
        return String.format("%s.%s.%s", serverName, databaseName, tableName);
    }

    /**
     * @param tableName
     * @return Fully qualified table name <code>&lt;databaseName&gt;.&lt;tableName&gt;</code>
     */
    public String qualifiedTableName(final String tableName) {
        return String.format("%s.%s", databaseName, tableName);
    }

    public String getServerName() {
        return serverName;
    }

    public String getTopicPrefix() {
        return serverName;
    }

    /**
     * Creates the database and populates it with initialization SQL script. To use multiline
     * statements for stored procedures definition use delimiter $$ to delimit statements in the procedure.
     * See fnDbz162 procedure in reqression_test.sql for example of usage.
     */
    public void createAndInitialize() {
        createAndInitialize(Collections.emptyMap());
    }

    /**
     * Creates the database and populates it with initialization SQL script. To use multiline
     * statements for stored procedures definition use delimiter $$ to delimit statements in the procedure.
     * See fnDbz162 procedure in reqression_test.sql for example of usage.
     *
     * @param urlProperties jdbc url properties
     */
    public void createAndInitialize(Map<String, Object> urlProperties) {
        final String ddlFile = String.format("ddl/%s.sql", templateName);
        final URL ddlTestFile = UniqueDatabase.class.getClassLoader().getResource(ddlFile);
        assertNotNull("Cannot locate " + ddlFile, ddlTestFile);
        try {
            try (JdbcConnection connection = forTestDatabase(DEFAULT_DATABASE, urlProperties)) {
                final List<String> statements = readFileContents(ddlTestFile.toURI(), (data) -> Arrays.stream(
                        Stream.concat(
                                Arrays.stream(charset != null ? CREATE_DATABASE_WITH_CHARSET_DDL : CREATE_DATABASE_DDL),
                                data)
                                .map(String::trim)
                                .filter(x -> !x.startsWith("--") && !x.isEmpty())
                                .map(x -> {
                                    final Matcher m = COMMENT_PATTERN.matcher(x);
                                    return m.matches() ? m.group(1) : x;
                                })
                                .map(this::convertSQL)
                                .collect(Collectors.joining("\n")).split(";"))
                        .map(x -> x.replace("$$", ";"))
                        .collect(Collectors.toList()));
                connection.execute(statements.toArray(new String[statements.size()]));
            }
        }
        catch (final Exception e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Supports reading the contents of the SQL file, regardless if its bundled in a jar or not.
     *
     * @param uri the file URI
     * @param handler the handler to receive the file contents
     * @return the contents
     * @throws IOException if there was an error reading the file contents
     */
    private List<String> readFileContents(URI uri, Function<Stream<String>, List<String>> handler) throws IOException {
        if ("jar".equals(uri.getScheme())) {
            // Open the JAR file
            String[] parts = uri.toString().split("!");
            URI jarUri = URI.create(parts[0]);
            try (FileSystem fs = FileSystems.newFileSystem(jarUri, Collections.emptyMap())) {
                try {
                    return handler.apply(Files.readAllLines(fs.getPath(parts[1])).stream());
                }
                catch (IOException e) {
                    throw new IOException("Failed to read contents", e);
                }
            }
            catch (IOException e) {
                throw new IOException("Failed to open file system", e);
            }
        }
        else {
            // Read from the file system
            Path path = Paths.get(uri);
            return handler.apply(Files.readAllLines(path).stream());
        }
    }

    /**
     * @param dbHistoryPath - directory where to store database schema history
     * @see io.debezium.storage.file.history.FileSchemaHistory
     */
    public UniqueDatabase withDbHistoryPath(final Path dbHistoryPath) {
        this.dbHistoryPath = dbHistoryPath;
        return this;
    }

    /**
     * @return Configuration builder initialized with JDBC connection parameters.
     */
    public Configuration.Builder defaultJdbcConfigBuilder() {
        Builder builder = Configuration.create()
                .with(BinlogConnectorConfig.HOSTNAME, System.getProperty("database.hostname", "localhost"))
                .with(BinlogConnectorConfig.PORT, System.getProperty("database.port", "3306"))
                .with(BinlogConnectorConfig.USER, "snapper")
                .with(BinlogConnectorConfig.PASSWORD, "snapperpass")
                .with("driver.allowPublicKeyRetrieval", "true");

        builder = applyConnectorDefaultJdbcConfiguration(builder);

        String sslMode = System.getProperty("database.ssl.mode", "preferred");

        if (sslMode.equals("disabled")) {
            builder.with(BinlogConnectorConfig.SSL_MODE, BinlogConnectorConfig.SecureConnectionMode.DISABLED);
        }
        else {
            URL trustStoreFile = UniqueDatabase.class.getClassLoader().getResource("ssl/truststore");
            URL keyStoreFile = UniqueDatabase.class.getClassLoader().getResource("ssl/keystore");

            builder.with(BinlogConnectorConfig.SSL_MODE, sslMode)
                    .with(BinlogConnectorConfig.SSL_TRUSTSTORE, System.getProperty("database.ssl.truststore", trustStoreFile.getPath()))
                    .with(BinlogConnectorConfig.SSL_TRUSTSTORE_PASSWORD, System.getProperty("database.ssl.truststore.password", "debezium"))
                    .with(BinlogConnectorConfig.SSL_KEYSTORE, System.getProperty("database.ssl.keystore", keyStoreFile.getPath()))
                    .with(BinlogConnectorConfig.SSL_KEYSTORE_PASSWORD, System.getProperty("database.ssl.keystore.password", "debezium"));
        }

        if (dbHistoryPath != null) {
            builder.with(FileSchemaHistory.FILE_PATH, dbHistoryPath);
        }

        // enable database DDL capture
        builder.with(BinlogConnectorConfig.STORE_ONLY_CAPTURED_DATABASES_DDL, true);

        return builder;
    }

    /**
     * @return Configuration builder initialized with JDBC connection parameters and most frequently used parameters
     */
    public Configuration.Builder defaultConfig() {
        return defaultConfigWithoutDatabaseFilter()
                .with(BinlogConnectorConfig.DATABASE_INCLUDE_LIST, getDatabaseName());
    }

    /**
     * @return Configuration builder initialized with JDBC connection parameters and most frequently used parameters,
     * database not filtered by default
     */
    public Configuration.Builder defaultConfigWithoutDatabaseFilter() {
        return defaultJdbcConfigBuilder()
                .with(BinlogConnectorConfig.SERVER_ID, 18765)
                .with(BinlogConnectorConfig.POLL_INTERVAL_MS, 10)
                .with(BinlogConnectorConfig.SCHEMA_HISTORY, FileSchemaHistory.class)
                .with(BinlogConnectorConfig.BUFFER_SIZE_FOR_BINLOG_READER, 10_000)
                .with(CommonConnectorConfig.TOPIC_PREFIX, getServerName());
    }

    /**
     * @return The unique database suffix
     */
    public String getIdentifier() {
        return identifier;
    }

    /**
     * @return timezone in which the database is located
     */
    public ZoneId timezone() {
        return TIMEZONE;
    }

    protected Configuration.Builder applyConnectorDefaultJdbcConfiguration(Configuration.Builder builder) {
        return builder;
    }

    protected abstract JdbcConnection forTestDatabase(String databaseName, Map<String, Object> urlProperties);

}
