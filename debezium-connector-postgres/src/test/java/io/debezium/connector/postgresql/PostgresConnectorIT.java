/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import static io.debezium.connector.postgresql.PostgresConnectorConfig.SnapshotMode.ALWAYS;
import static io.debezium.connector.postgresql.PostgresConnectorConfig.SnapshotMode.INITIAL;
import static io.debezium.connector.postgresql.PostgresConnectorConfig.SnapshotMode.INITIAL_ONLY;
import static io.debezium.connector.postgresql.PostgresConnectorConfig.SnapshotMode.NEVER;
import static io.debezium.connector.postgresql.TestHelper.PK_FIELD;
import static org.fest.assertions.Assertions.assertThat;
import static org.junit.Assert.fail;

import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.IntStream;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.postgresql.util.PSQLState;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.postgresql.connection.ReplicationConnection;
import io.debezium.data.VerifyRecord;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.embedded.EmbeddedEngine;
import io.debezium.jdbc.JdbcConnectionException;

/**
 * Integration test for {@link PostgresConnector} using an {@link io.debezium.embedded.EmbeddedEngine} 
 * 
 * @author Horia Chiorean (hchiorea@redhat.com)
 */
public class PostgresConnectorIT extends AbstractConnectorTest {
    
    private static final String INSERT_STMT = "INSERT INTO s1.a (aa) VALUES (1);" +
                                              "INSERT INTO s2.a (aa) VALUES (1);";
    private static final String SETUP_TABLES_STMT = "DROP SCHEMA IF EXISTS s1 CASCADE;" +
                                                    "DROP SCHEMA IF EXISTS s2 CASCADE;" +
                                                    "CREATE SCHEMA s1; " +
                                                    "CREATE SCHEMA s2; " +
                                                    "CREATE TABLE s1.a (pk SERIAL, aa integer, PRIMARY KEY(pk));" +
                                                    "CREATE TABLE s2.a (pk SERIAL, aa integer, PRIMARY KEY(pk));" +
                                                    INSERT_STMT;
    private PostgresConnector connector;
   
    @BeforeClass
    public static void beforeClass() throws SQLException {
        TestHelper.dropAllSchemas();        
    }
    
    @Before
    public void before() {
        initializeConnectorTestFramework();
    }
    
    @Test
    public void shouldValidateConnectorConfigDef() {
        connector = new PostgresConnector(); 
        ConfigDef configDef = connector.config();
        assertThat(configDef).isNotNull();
        PostgresConnectorConfig.ALL_FIELDS.forEach(this::validateFieldDef);
    }
    
    @Test
    public void shouldNotStartWithInvalidConfiguration() throws Exception {
        // use an empty configuration which should be invalid because of the lack of DB connection details
        Configuration config = Configuration.create().build();
    
        // we expect the engine will log at least one error, so preface it ...
        logger.info("Attempting to start the connector with an INVALID configuration, so MULTIPLE error messages & one exceptions will appear in the log");
        start(PostgresConnector.class, config, (success, msg, error) -> {
            assertThat(success).isFalse();
            assertThat(error).isNotNull();
        });
        assertConnectorNotRunning();            
    }
    
    @Test
    public void shouldValidateConfiguration() throws Exception {
        // use an empty configuration which should be invalid because of the lack of DB connection details
        Configuration config = Configuration.create().build();
        PostgresConnector connector = new PostgresConnector();
        Config validatedConfig = connector.validate(config.asMap());
        // validate that the required fields have errors
        assertConfigurationErrors(validatedConfig, PostgresConnectorConfig.HOSTNAME, 1);
        assertConfigurationErrors(validatedConfig, PostgresConnectorConfig.USER, 1);
        assertConfigurationErrors(validatedConfig, PostgresConnectorConfig.PASSWORD, 1);
        assertConfigurationErrors(validatedConfig, PostgresConnectorConfig.DATABASE_NAME, 1);
        
        // validate the non required fields
        validateField(validatedConfig, PostgresConnectorConfig.PLUGIN_NAME, ReplicationConnection.Builder.DEFAULT_PLUGIN_NAME);
        validateField(validatedConfig, PostgresConnectorConfig.SLOT_NAME, ReplicationConnection.Builder.DEFAULT_SLOT_NAME);
        validateField(validatedConfig, PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.FALSE);
        validateField(validatedConfig, PostgresConnectorConfig.PORT, PostgresConnectorConfig.DEFAULT_PORT);
        validateField(validatedConfig, PostgresConnectorConfig.SERVER_NAME, null);
        validateField(validatedConfig, PostgresConnectorConfig.TOPIC_SELECTION_STRATEGY, 
                      PostgresConnectorConfig.TopicSelectionStrategy.TOPIC_PER_TABLE.name().toLowerCase());
        validateField(validatedConfig, PostgresConnectorConfig.MAX_QUEUE_SIZE, PostgresConnectorConfig.DEFAULT_MAX_QUEUE_SIZE);
        validateField(validatedConfig, PostgresConnectorConfig.MAX_BATCH_SIZE, PostgresConnectorConfig.DEFAULT_MAX_BATCH_SIZE);
        validateField(validatedConfig, PostgresConnectorConfig.ROWS_FETCH_SIZE, PostgresConnectorConfig.DEFAULT_ROWS_FETCH_SIZE);
        validateField(validatedConfig, PostgresConnectorConfig.POLL_INTERVAL_MS, PostgresConnectorConfig.DEFAULT_POLL_INTERVAL_MILLIS);
        validateField(validatedConfig, PostgresConnectorConfig.SSL_MODE, 
                      PostgresConnectorConfig.SecureConnectionMode.DISABLED.name().toLowerCase());
        validateField(validatedConfig, PostgresConnectorConfig.SSL_CLIENT_CERT, null);
        validateField(validatedConfig, PostgresConnectorConfig.SSL_CLIENT_KEY, null);
        validateField(validatedConfig, PostgresConnectorConfig.SSL_CLIENT_KEY_PASSWORD, null);
        validateField(validatedConfig, PostgresConnectorConfig.SSL_ROOT_CERT, null);
        validateField(validatedConfig, PostgresConnectorConfig.SCHEMA_WHITELIST, null);
        validateField(validatedConfig, PostgresConnectorConfig.SCHEMA_BLACKLIST, null);
        validateField(validatedConfig, PostgresConnectorConfig.TABLE_WHITELIST, null);
        validateField(validatedConfig, PostgresConnectorConfig.TABLE_BLACKLIST, null);
        validateField(validatedConfig, PostgresConnectorConfig.COLUMN_BLACKLIST, null);
        validateField(validatedConfig, PostgresConnectorConfig.SNAPSHOT_MODE, 
                      INITIAL.name().toLowerCase());
        validateField(validatedConfig, PostgresConnectorConfig.SNAPSHOT_LOCK_TIMEOUT_MS, 
                      PostgresConnectorConfig.DEFAULT_SNAPSHOT_LOCK_TIMEOUT_MILLIS);
        validateField(validatedConfig, PostgresConnectorConfig.TIME_PRECISION_MODE, 
                      PostgresConnectorConfig.TemporalPrecisionMode.ADAPTIVE.name().toLowerCase());
    }
    
    @Test
    public void shouldSupportSSLParameters() throws Exception {
        // the default docker image we're testing against doesn't use SSL, so check that the connector fails to start when
        // SSL is enabled
        Configuration config = TestHelper.defaultConfig().with(PostgresConnectorConfig.SSL_MODE,  
                                                               PostgresConnectorConfig.SecureConnectionMode.REQUIRED).build();
        start(PostgresConnector.class, config, (success, msg, error) -> {
            assertThat(success).isFalse();
            assertThat(error).isInstanceOf(JdbcConnectionException.class);
            JdbcConnectionException jdbcException = (JdbcConnectionException)error;
            assertThat(PSQLState.CONNECTION_UNABLE_TO_CONNECT).isEqualTo(new PSQLState(jdbcException.getSqlState()));
        });
        assertConnectorNotRunning();
    }
    
    @Test
    public void shouldProduceEventsWithInitialSnapshot() throws Exception {
        TestHelper.execute(SETUP_TABLES_STMT);
        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                                               .with(PostgresConnectorConfig.SNAPSHOT_MODE, INITIAL.getValue())
                                               .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.FALSE);
        start(PostgresConnector.class, configBuilder.build());
        assertConnectorIsRunning();
    
        //check the records from the snapshot
        assertRecordsFromSnapshot(2, 1, 1);
    
        // insert 2 new records        
        TestHelper.execute(INSERT_STMT);
        assertRecordsAfterInsert(2, 2, 2);
        
        //now stop the connector
        stopConnector();
        assertNoRecordsToConsume();
        
        //insert some more records
        TestHelper.execute(INSERT_STMT);
       
        //start the connector back up and check that a new snapshot has not been performed (we're running initial only mode)
        //but the 2 records that we were inserted while we were down will be retrieved
        start(PostgresConnector.class, configBuilder.with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE).build());
        assertConnectorIsRunning();
    
        assertRecordsAfterInsert(2, 3, 3);
    }
    
    @Test
    public void shouldProduceEventsWhenSnapshotsAreNeverAllowed() throws InterruptedException {
        TestHelper.execute(SETUP_TABLES_STMT);
        Configuration config = TestHelper.defaultConfig()
                                         .with(PostgresConnectorConfig.SNAPSHOT_MODE, NEVER.getValue())
                                         .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE)
                                         .build();
        start(PostgresConnector.class, config);
        assertConnectorIsRunning();
        waitForAvailableRecords(100, TimeUnit.MILLISECONDS);
        // there shouldn't be any snapshot records
        assertNoRecordsToConsume();
    
        // insert and verify 2 new records
        TestHelper.execute(INSERT_STMT);
        assertRecordsAfterInsert(2, 2, 2);
    } 
    
    @Test
    public void shouldNotProduceEventsWithInitialOnlySnapshot() throws InterruptedException {
        TestHelper.execute(SETUP_TABLES_STMT);
        Configuration config = TestHelper.defaultConfig()
                                         .with(PostgresConnectorConfig.SNAPSHOT_MODE, INITIAL_ONLY.getValue())
                                         .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE)
                                         .build();
        start(PostgresConnector.class, config);
        assertConnectorIsRunning();

        //check the records from the snapshot
        assertRecordsFromSnapshot(2, 1, 1);
    
        // insert and verify that no events were received since the connector should not be streaming changes
        TestHelper.execute(INSERT_STMT);
        waitForAvailableRecords(100, TimeUnit.MILLISECONDS);
        // there shouldn't be any  records
        assertNoRecordsToConsume();
    }
    
    @Test
    public void shouldProduceEventsWhenAlwaysTakingSnapshots() throws InterruptedException {
        TestHelper.execute(SETUP_TABLES_STMT);
        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                                               .with(PostgresConnectorConfig.SNAPSHOT_MODE, ALWAYS.getValue())
                                               .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.FALSE);
        start(PostgresConnector.class, configBuilder.build());
        assertConnectorIsRunning();
        
        //check the records from the snapshot
        assertRecordsFromSnapshot(2, 1, 1);
    
        // insert and verify 2 new records
        TestHelper.execute(INSERT_STMT);
        assertRecordsAfterInsert(2, 2, 2);
    
        //now stop the connector
        stopConnector();
        assertNoRecordsToConsume();
    
        //start the connector back up and check that a new snapshot has been performed 
        start(PostgresConnector.class, configBuilder.with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE).build());
        assertConnectorIsRunning();
    
        assertRecordsFromSnapshot(4, 1, 2, 1, 2);   
    }
    
    @Test
    public void shouldResumeSnapshotIfFailingMidstream() throws Exception {
        // insert another set of rows so we can stop at certain point
        CountDownLatch latch = new CountDownLatch(1);
        String setupStmt = SETUP_TABLES_STMT + INSERT_STMT;
        TestHelper.execute(setupStmt);
        // use 1 ms polling interval to ensure we get the exception during the snapshot process
        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                                                        .with(PostgresConnectorConfig.SNAPSHOT_MODE, INITIAL.getValue())
                                                        .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.FALSE);
        EmbeddedEngine.CompletionCallback completionCallback = (success, message, error) -> {
            if (error != null) {
                latch.countDown();
            } else {
                fail("A controlled exception was expected....");
            }
        };
        start(PostgresConnector.class, configBuilder.build(), completionCallback, stopOnPKPredicate(2));
        // wait until we know we've raised the exception at startup AND the engine has been shutdown
        if (!latch.await(10, TimeUnit.SECONDS)) {
            fail("did not reach stop condition in time");
        }
        // wait until we know we've raised the exception at startup AND the engine has been shutdown
        assertConnectorNotRunning();
        // just drain all the records
        consumeAvailableRecords(record->{});
        // stop the engine altogether
        stopConnector();
        // make sure there are no records to consume
        assertNoRecordsToConsume();    
        // start the connector back up and check that it took another full snapshot since previously it was stopped midstream
        start(PostgresConnector.class, configBuilder.with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE).build());
        assertConnectorIsRunning();
    
        //check that the snapshot was recreated
        assertRecordsFromSnapshot(4, 1, 2, 1, 2);
        
        //and we can stream records
        // insert and verify 2 new records
        TestHelper.execute(INSERT_STMT);
        assertRecordsAfterInsert(2, 3, 3);
    }
    
    private Predicate<SourceRecord> stopOnPKPredicate(int pkValue) {
        return record -> {
            Struct key = (Struct) record.key();
            return ((Integer) key.get(PK_FIELD)) == pkValue;
        };
    }
    
    private void assertRecordsFromSnapshot(int expectedCount, int...pks) throws InterruptedException {
        SourceRecords actualRecords = consumeRecordsByTopic(expectedCount);
        assertThat(actualRecords.allRecordsInOrder().size()).isEqualTo(expectedCount);
        
        // we have 2 schemas/topics that we expect
        int expectedCountPerSchema = expectedCount / 2;
        
        List<SourceRecord> recordsForTopicS1 = actualRecords.recordsForTopic("s1.a");
        assertThat(recordsForTopicS1.size()).isEqualTo(expectedCountPerSchema);
        IntStream.range(0, expectedCountPerSchema)
                 .forEach(i -> VerifyRecord.isValidRead(recordsForTopicS1.remove(0), PK_FIELD, pks[i]));
    
        List<SourceRecord> recordsForTopicS2 = actualRecords.recordsForTopic("s2.a");
        assertThat(recordsForTopicS2.size()).isEqualTo(expectedCountPerSchema);
        IntStream.range(0, expectedCountPerSchema)
                 .forEach(i -> VerifyRecord.isValidRead(recordsForTopicS2.remove(0), PK_FIELD, pks[i + expectedCountPerSchema]));
    }
    
    private void assertRecordsAfterInsert(int expectedCount, int...pks) throws InterruptedException {
        SourceRecords actualRecords = consumeRecordsByTopic(expectedCount);
        assertThat(actualRecords.topics().size()).isEqualTo(expectedCount);
    
        // we have 2 schemas
        int expectedCountPerSchema = expectedCount / 2;
    
        List<SourceRecord> recordsForTopicS1 = actualRecords.recordsForTopic("s1.a");
        assertThat(recordsForTopicS1.size()).isEqualTo(expectedCountPerSchema);
        IntStream.range(0, expectedCountPerSchema).forEach(i -> VerifyRecord.isValidInsert(recordsForTopicS1.remove(0), PK_FIELD, pks[i]));
    
        List<SourceRecord> recordsForTopicS2 = actualRecords.recordsForTopic("s2.a");
        assertThat(recordsForTopicS2.size()).isEqualTo(expectedCountPerSchema);
        IntStream.range(0, expectedCountPerSchema).forEach(i -> VerifyRecord.isValidInsert(recordsForTopicS2.remove(0), PK_FIELD, pks[i]));
    }
    
    private <T> void validateField(Config config, Field field, T expectedValue) {
        assertNoConfigurationErrors(config, field);
        Object actualValue = configValue(config, field.name()).value();
        if (actualValue == null) {
            actualValue = field.defaultValue();
        }
        if (expectedValue == null) {
            assertThat(actualValue).isNull();
        } else {
            assertThat(expectedValue).isEqualTo(actualValue);
        }
    }
    
    private void validateFieldDef(Field expected) {
        ConfigDef configDef = connector.config();
        assertThat(configDef.names()).contains(expected.name());
        ConfigDef.ConfigKey key = configDef.configKeys().get(expected.name());
        assertThat(key).isNotNull();
        assertThat(key.name).isEqualTo(expected.name());
        assertThat(key.displayName).isEqualTo(expected.displayName());
        assertThat(key.importance).isEqualTo(expected.importance());
        assertThat(key.documentation).isEqualTo(expected.description());
        assertThat(key.type).isEqualTo(expected.type());
        assertThat(key.defaultValue).isEqualTo(expected.defaultValue());
        assertThat(key.dependents).isEqualTo(expected.dependents());
        assertThat(key.width).isNotNull();
        assertThat(key.group).isNotNull();
        assertThat(key.orderInGroup).isGreaterThan(0);
        assertThat(key.validator).isNull();
        assertThat(key.recommender).isNull();
    }
}
