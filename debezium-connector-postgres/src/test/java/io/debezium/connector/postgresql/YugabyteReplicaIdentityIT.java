package io.debezium.connector.postgresql;

import io.debezium.config.Configuration;
import io.debezium.data.Envelope;
import io.debezium.data.VerifyRecord;
import io.debezium.embedded.AbstractConnectorTest;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static io.debezium.connector.postgresql.TestHelper.PK_FIELD;
import static io.debezium.connector.postgresql.TestHelper.topicName;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests to validate the functionality of replica identities with YugabyteDB.
 *
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
public class YugabyteReplicaIdentityIT extends AbstractConnectorTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(YugabyteReplicaIdentityIT.class);

  private static final String CREATE_TABLES_STMT = "DROP SCHEMA IF EXISTS s1 CASCADE;" +
     "DROP SCHEMA IF EXISTS s2 CASCADE;" +
     "CREATE SCHEMA s1; " +
     "CREATE SCHEMA s2; " +
     "CREATE TABLE s1.a (pk SERIAL, aa integer, PRIMARY KEY(pk));" +
     "CREATE TABLE s2.a (pk SERIAL, aa integer, bb varchar(20), PRIMARY KEY(pk));";

  private static final String INSERT_STMT = "INSERT INTO s1.a (aa) VALUES (1);" +
     "INSERT INTO s2.a (aa) VALUES (1);";

  private YugabyteDBConnector connector;

  @BeforeClass
  public static void beforeClass() throws SQLException {
    TestHelper.dropAllSchemas();
  }

  @Before
  public void before() {
    initializeConnectorTestFramework();
    TestHelper.dropDefaultReplicationSlot();
    TestHelper.execute(CREATE_TABLES_STMT);
  }

  @After
  public void after() {
    stopConnector();
    TestHelper.dropDefaultReplicationSlot();
    TestHelper.dropPublication();
  }

  @Test
  public void oldValuesWithReplicaIdentityFullForPgOutput() throws Exception {
    shouldProduceOldValuesWithReplicaIdentityFull(PostgresConnectorConfig.LogicalDecoder.PGOUTPUT);
  }

  @Test
  public void oldValuesWithReplicaIdentityFullForYbOutput() throws Exception {
    shouldProduceOldValuesWithReplicaIdentityFull(PostgresConnectorConfig.LogicalDecoder.YBOUTPUT);
  }

  public void shouldProduceOldValuesWithReplicaIdentityFull(PostgresConnectorConfig.LogicalDecoder logicalDecoder) throws Exception {
    TestHelper.execute("ALTER TABLE s1.a REPLICA IDENTITY FULL;");
    TestHelper.execute("ALTER TABLE s2.a REPLICA IDENTITY FULL;");

    Configuration config = TestHelper.defaultConfig()
                             .with(PostgresConnectorConfig.SNAPSHOT_MODE, PostgresConnectorConfig.SnapshotMode.NEVER.getValue())
                             .with(PostgresConnectorConfig.PLUGIN_NAME, logicalDecoder.getPostgresPluginName())
                             .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE)
                             .build();
    start(YugabyteDBConnector.class, config);
    assertConnectorIsRunning();

    // YB Note: Added a wait for replication slot to be active.
    TestHelper.waitFor(Duration.ofSeconds(10));

    waitForAvailableRecords(10_000, TimeUnit.MILLISECONDS);
    // there shouldn't be any snapshot records
    assertNoRecordsToConsume();

    // insert and verify 2 new records
    TestHelper.execute(INSERT_STMT);
    TestHelper.execute("UPDATE s1.a SET aa = 12345 WHERE pk = 1;");

    SourceRecords actualRecords = consumeRecordsByTopic(3);
    List<SourceRecord> records = actualRecords.recordsForTopic(topicName("s1.a"));

    SourceRecord insertRecord = records.get(0);
    SourceRecord updateRecord = records.get(1);

    if (logicalDecoder.isYBOutput()) {
      YBVerifyRecord.isValidInsert(insertRecord, PK_FIELD, 1);
      YBVerifyRecord.isValidUpdate(updateRecord, PK_FIELD, 1);
    } else {
      VerifyRecord.isValidInsert(insertRecord, PK_FIELD, 1);
      VerifyRecord.isValidUpdate(updateRecord, PK_FIELD, 1);
    }

    Struct updateRecordValue = (Struct) updateRecord.value();
    assertThat(updateRecordValue.get(Envelope.FieldName.AFTER)).isNotNull();
    assertThat(updateRecordValue.get(Envelope.FieldName.BEFORE)).isNotNull();

    if (logicalDecoder.isYBOutput()) {
      assertThat(updateRecordValue.getStruct(Envelope.FieldName.BEFORE).getStruct("aa").getInt32("value")).isEqualTo(1);
      assertThat(updateRecordValue.getStruct(Envelope.FieldName.AFTER).getStruct("aa").getInt32("value")).isEqualTo(12345);
    } else {
      assertThat(updateRecordValue.getStruct(Envelope.FieldName.BEFORE).get("aa")).isEqualTo(1);
      assertThat(updateRecordValue.getStruct(Envelope.FieldName.AFTER).get("aa")).isEqualTo(12345);
    }
  }

  @Test
  public void replicaIdentityDefaultWithPgOutput() throws Exception {
    shouldProduceExpectedValuesWithReplicaIdentityDefault(PostgresConnectorConfig.LogicalDecoder.PGOUTPUT);
  }

  @Test
  public void replicaIdentityDefaultWithYbOutput() throws Exception {
    shouldProduceExpectedValuesWithReplicaIdentityDefault(PostgresConnectorConfig.LogicalDecoder.YBOUTPUT);
  }

  public void shouldProduceExpectedValuesWithReplicaIdentityDefault(PostgresConnectorConfig.LogicalDecoder logicalDecoder) throws Exception {
    TestHelper.execute("ALTER TABLE s1.a REPLICA IDENTITY DEFAULT;");
    TestHelper.execute("ALTER TABLE s2.a REPLICA IDENTITY DEFAULT;");

    Configuration config = TestHelper.defaultConfig()
                             .with(PostgresConnectorConfig.SNAPSHOT_MODE, PostgresConnectorConfig.SnapshotMode.NEVER.getValue())
                             .with(PostgresConnectorConfig.PLUGIN_NAME, logicalDecoder.getPostgresPluginName())
                             .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE)
                             .build();
    start(YugabyteDBConnector.class, config);
    assertConnectorIsRunning();

    // YB Note: Added a wait for replication slot to be active.
    TestHelper.waitFor(Duration.ofSeconds(10));

    waitForAvailableRecords(10_000, TimeUnit.MILLISECONDS);
    // there shouldn't be any snapshot records
    assertNoRecordsToConsume();

    // insert and verify 2 new records
    TestHelper.execute("INSERT INTO s2.a VALUES (1, 22, 'random text value');");
    TestHelper.execute("UPDATE s2.a SET aa = 12345 WHERE pk = 1;");

    SourceRecords actualRecords = consumeRecordsByTopic(2);
    List<SourceRecord> records = actualRecords.recordsForTopic(topicName("s2.a"));

    SourceRecord insertRecord = records.get(0);
    SourceRecord updateRecord = records.get(1);

    if (logicalDecoder.isYBOutput()) {
      YBVerifyRecord.isValidInsert(insertRecord, PK_FIELD, 1);
      YBVerifyRecord.isValidUpdate(updateRecord, PK_FIELD, 1);
    } else {
      VerifyRecord.isValidInsert(insertRecord, PK_FIELD, 1);
      VerifyRecord.isValidUpdate(updateRecord, PK_FIELD, 1);
    }

    Struct updateRecordValue = (Struct) updateRecord.value();
    assertThat(updateRecordValue.get(Envelope.FieldName.AFTER)).isNotNull();
    assertThat(updateRecordValue.get(Envelope.FieldName.BEFORE)).isNull();

    // After field will have entries for all the columns.
    if (logicalDecoder.isYBOutput()) {
      assertThat(updateRecordValue.getStruct(Envelope.FieldName.AFTER).getStruct("pk").getInt32("value")).isEqualTo(1);
      assertThat(updateRecordValue.getStruct(Envelope.FieldName.AFTER).getStruct("aa").getInt32("value")).isEqualTo(12345);
      assertThat(updateRecordValue.getStruct(Envelope.FieldName.AFTER).getStruct("bb").getString("value")).isEqualTo("random text value");
    } else {
      assertThat(updateRecordValue.getStruct(Envelope.FieldName.AFTER).get("pk")).isEqualTo(1);
      assertThat(updateRecordValue.getStruct(Envelope.FieldName.AFTER).get("aa")).isEqualTo(12345);
      assertThat(updateRecordValue.getStruct(Envelope.FieldName.AFTER).get("bb")).isEqualTo("random text value");
    }
  }

  @Test
  public void shouldProduceEventsWithValuesForChangedColumnWithReplicaIdentityChange() throws Exception {
    // YB Note: Note that even if we do not alter, the default replica identity on service is CHANGE.
    TestHelper.execute("ALTER TABLE s2.a REPLICA IDENTITY CHANGE;");

    Configuration config = TestHelper.defaultConfig()
                             .with(PostgresConnectorConfig.SNAPSHOT_MODE, PostgresConnectorConfig.SnapshotMode.NEVER.getValue())
                             .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE)
                             .build();
    start(YugabyteDBConnector.class, config);
    assertConnectorIsRunning();

    // YB Note: Added a wait for replication slot to be active.
    TestHelper.waitFor(Duration.ofSeconds(10));

    waitForAvailableRecords(10_000, TimeUnit.MILLISECONDS);
    // there shouldn't be any snapshot records
    assertNoRecordsToConsume();

    // insert and verify 3 new records
    TestHelper.execute("INSERT INTO s2.a VALUES (1, 22, 'random text value');");
    TestHelper.execute("UPDATE s2.a SET aa = 12345 WHERE pk = 1;");
    TestHelper.execute("UPDATE s2.a SET aa = null WHERE pk = 1;");

    SourceRecords actualRecords = consumeRecordsByTopic(3);
    List<SourceRecord> records = actualRecords.recordsForTopic(topicName("s2.a"));

    SourceRecord insertRecord = records.get(0);
    SourceRecord updateRecord = records.get(1);
    SourceRecord updateRecordWithNullCol = records.get(2);

    YBVerifyRecord.isValidInsert(insertRecord, PK_FIELD, 1);
    YBVerifyRecord.isValidUpdate(updateRecord, PK_FIELD, 1);
    YBVerifyRecord.isValidUpdate(updateRecordWithNullCol, PK_FIELD, 1);

    Struct updateRecordValue = (Struct) updateRecord.value();
    assertThat(updateRecordValue.get(Envelope.FieldName.AFTER)).isNotNull();
    assertThat(updateRecordValue.get(Envelope.FieldName.BEFORE)).isNull();

    // After field will have entries for all the changed columns.
    assertThat(updateRecordValue.getStruct(Envelope.FieldName.AFTER).getStruct("pk").getInt32("value")).isEqualTo(1);
    assertThat(updateRecordValue.getStruct(Envelope.FieldName.AFTER).getStruct("aa").getInt32("value")).isEqualTo(12345);
    assertThat(updateRecordValue.getStruct(Envelope.FieldName.AFTER).getStruct("bb")).isNull();

    // After field will have a null value in place of the column explicitly set as null.
    Struct updateRecordWithNullColValue = (Struct) updateRecordWithNullCol.value();
    assertThat(updateRecordWithNullColValue.getStruct(Envelope.FieldName.AFTER).getStruct("pk").getInt32("value")).isEqualTo(1);
    assertThat(updateRecordWithNullColValue.getStruct(Envelope.FieldName.AFTER).getStruct("aa").getInt32("value")).isNull();
    assertThat(updateRecordWithNullColValue.getStruct(Envelope.FieldName.AFTER).getStruct("bb")).isNull();
  }

  @Test
  public void shouldThrowExceptionWithReplicaIdentityNothingOnUpdatesAndDeletes() throws Exception {
    /*
      According to Postgres docs:
      If a table without a replica identity is added to a publication that replicates
      UPDATE or DELETE operations then subsequent UPDATE or DELETE operations will cause
      an error on the publisher.

      Details: https://www.postgresql.org/docs/current/logical-replication-publication.html
     */
    TestHelper.execute("ALTER TABLE s1.a REPLICA IDENTITY NOTHING;");
    TestHelper.execute("ALTER TABLE s2.a REPLICA IDENTITY NOTHING;");

    Configuration config = TestHelper.defaultConfig()
                             .with(PostgresConnectorConfig.SNAPSHOT_MODE, PostgresConnectorConfig.SnapshotMode.NEVER.getValue())
                             .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE)
                             .build();
    start(YugabyteDBConnector.class, config);
    assertConnectorIsRunning();

    // YB Note: Added a wait for replication slot to be active.
    TestHelper.waitFor(Duration.ofSeconds(10));

    waitForAvailableRecords(10_000, TimeUnit.MILLISECONDS);
    // there shouldn't be any snapshot records
    assertNoRecordsToConsume();

    // insert and verify 2 new records
    TestHelper.execute("INSERT INTO s2.a VALUES (1, 22, 'random text value');");

    try {
      TestHelper.execute("UPDATE s2.a SET aa = 12345 WHERE pk = 1;");
    } catch (Exception sqle) {
      assertThat(sqle.getMessage()).contains("ERROR: cannot update table \"a\" because it does "
                                              + "not have a replica identity and publishes updates");
    }

    try {
      TestHelper.execute("DELETE FROM s2.a WHERE pk = 1;");
    } catch (Exception sqle) {
      assertThat(sqle.getMessage()).contains("ERROR: cannot delete from table \"a\" because it "
                                              + "does not have a replica identity and publishes deletes");
    }
  }

  @Test
  public void beforeImageForDeleteWithReplicaIdentityFullAndPgOutput() throws Exception {
    shouldHaveBeforeImageForDeletesForReplicaIdentityFull(PostgresConnectorConfig.LogicalDecoder.PGOUTPUT);
  }

  @Test
  public void beforeImageForDeleteWithReplicaIdentityFullAndYbOutput() throws Exception {
    shouldHaveBeforeImageForDeletesForReplicaIdentityFull(PostgresConnectorConfig.LogicalDecoder.YBOUTPUT);
  }

  public void shouldHaveBeforeImageForDeletesForReplicaIdentityFull(PostgresConnectorConfig.LogicalDecoder logicalDecoder) throws Exception {
    TestHelper.execute("ALTER TABLE s1.a REPLICA IDENTITY FULL;");
    TestHelper.execute("ALTER TABLE s2.a REPLICA IDENTITY FULL;");
    Configuration config = TestHelper.defaultConfig()
                             .with(PostgresConnectorConfig.SNAPSHOT_MODE, PostgresConnectorConfig.SnapshotMode.NEVER.getValue())
                             .with(PostgresConnectorConfig.PLUGIN_NAME, logicalDecoder.getPostgresPluginName())
                             .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE)
                             .build();
    start(YugabyteDBConnector.class, config);
    assertConnectorIsRunning();

    // YB Note: Added a wait for replication slot to be active.
    TestHelper.waitFor(Duration.ofSeconds(10));

    waitForAvailableRecords(10_000, TimeUnit.MILLISECONDS);
    // there shouldn't be any snapshot records
    assertNoRecordsToConsume();

    // insert and verify 2 new records
    TestHelper.execute("INSERT INTO s2.a VALUES (1, 22, 'random text value');");
    TestHelper.execute("DELETE FROM s2.a WHERE pk = 1;");

    SourceRecords actualRecords = consumeRecordsByTopic(2);
    List<SourceRecord> records = actualRecords.recordsForTopic(topicName("s2.a"));

    SourceRecord insertRecord = records.get(0);
    SourceRecord deleteRecord = records.get(1);

    if (logicalDecoder.isYBOutput()) {
      YBVerifyRecord.isValidInsert(insertRecord, PK_FIELD, 1);
      YBVerifyRecord.isValidDelete(deleteRecord, PK_FIELD, 1);
    } else {
      VerifyRecord.isValidInsert(insertRecord, PK_FIELD, 1);
      VerifyRecord.isValidDelete(deleteRecord, PK_FIELD, 1);
    }

    Struct deleteRecordValue = (Struct) deleteRecord.value();
    assertThat(deleteRecordValue.get(Envelope.FieldName.AFTER)).isNull();
    assertThat(deleteRecordValue.get(Envelope.FieldName.BEFORE)).isNotNull();

    // Before field will have entries for all the columns.
    if (logicalDecoder.isYBOutput()) {
      assertThat(deleteRecordValue.getStruct(Envelope.FieldName.BEFORE).getStruct("pk").getInt32("value")).isEqualTo(1);
      assertThat(deleteRecordValue.getStruct(Envelope.FieldName.BEFORE).getStruct("aa").getInt32("value")).isEqualTo(22);
      assertThat(deleteRecordValue.getStruct(Envelope.FieldName.BEFORE).getStruct("bb").getString("value")).isEqualTo("random text value");
    } else {
      assertThat(deleteRecordValue.getStruct(Envelope.FieldName.BEFORE).get("pk")).isEqualTo(1);
      assertThat(deleteRecordValue.getStruct(Envelope.FieldName.BEFORE).get("aa")).isEqualTo(22);
      assertThat(deleteRecordValue.getStruct(Envelope.FieldName.BEFORE).get("bb")).isEqualTo("random text value");
    }
  }

  @Test
  public void beforeImageForDeleteWithReplicaIdentityDefaultAndPgOutput() throws Exception {
    shouldHaveBeforeImageForDeletesForReplicaIdentityDefault(PostgresConnectorConfig.LogicalDecoder.PGOUTPUT);
  }

  @Test
  public void beforeImageForDeleteWithReplicaIdentityDefaultAndYbOutput() throws Exception {
    shouldHaveBeforeImageForDeletesForReplicaIdentityDefault(PostgresConnectorConfig.LogicalDecoder.YBOUTPUT);
  }

  public void shouldHaveBeforeImageForDeletesForReplicaIdentityDefault(PostgresConnectorConfig.LogicalDecoder logicalDecoder) throws Exception {
    TestHelper.execute("ALTER TABLE s1.a REPLICA IDENTITY DEFAULT;");
    TestHelper.execute("ALTER TABLE s2.a REPLICA IDENTITY DEFAULT;");

    Configuration config = TestHelper.defaultConfig()
                             .with(PostgresConnectorConfig.SNAPSHOT_MODE, PostgresConnectorConfig.SnapshotMode.NEVER.getValue())
                             .with(PostgresConnectorConfig.PLUGIN_NAME, logicalDecoder.getPostgresPluginName())
                             .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE)
                             .build();
    start(YugabyteDBConnector.class, config);
    assertConnectorIsRunning();

    // YB Note: Added a wait for replication slot to be active.
    TestHelper.waitFor(Duration.ofSeconds(10));

    waitForAvailableRecords(10_000, TimeUnit.MILLISECONDS);
    // there shouldn't be any snapshot records
    assertNoRecordsToConsume();

    // insert and verify 2 new records
    TestHelper.execute("INSERT INTO s2.a VALUES (1, 22, 'random text value');");
    TestHelper.execute("DELETE FROM s2.a WHERE pk = 1;");

    SourceRecords actualRecords = consumeRecordsByTopic(2);
    List<SourceRecord> records = actualRecords.recordsForTopic(topicName("s2.a"));

    SourceRecord insertRecord = records.get(0);
    SourceRecord deleteRecord = records.get(1);

    if (logicalDecoder.isYBOutput()) {
      YBVerifyRecord.isValidInsert(insertRecord, PK_FIELD, 1);
      YBVerifyRecord.isValidDelete(deleteRecord, PK_FIELD, 1);
    } else {
      VerifyRecord.isValidInsert(insertRecord, PK_FIELD, 1);
      VerifyRecord.isValidDelete(deleteRecord, PK_FIELD, 1);
    }

    Struct deleteRecordValue = (Struct) deleteRecord.value();
    assertThat(deleteRecordValue.get(Envelope.FieldName.AFTER)).isNull();
    assertThat(deleteRecordValue.get(Envelope.FieldName.BEFORE)).isNotNull();

    // Before field will have entries only for the primary key columns.
    if (logicalDecoder.isYBOutput()) {
      assertThat(deleteRecordValue.getStruct(Envelope.FieldName.BEFORE).getStruct("pk").getInt32("value")).isEqualTo(1);
      assertThat(deleteRecordValue.getStruct(Envelope.FieldName.BEFORE).getStruct("aa").getInt32("value")).isNull();
      assertThat(deleteRecordValue.getStruct(Envelope.FieldName.BEFORE).getStruct("bb").getString("value")).isNull();
    } else {
      assertThat(deleteRecordValue.getStruct(Envelope.FieldName.BEFORE).get("pk")).isEqualTo(1);
      assertThat(deleteRecordValue.getStruct(Envelope.FieldName.BEFORE).get("aa")).isNull();
      assertThat(deleteRecordValue.getStruct(Envelope.FieldName.BEFORE).get("bb")).isNull();
    }
  }

  @Test
  public void shouldHaveBeforeImageForDeletesForReplicaIdentityChange() throws Exception {
    // YB Note: Note that even if we do not alter, the default replica identity on service is CHANGE.
    TestHelper.execute("ALTER TABLE s2.a REPLICA IDENTITY CHANGE;");

    Configuration config = TestHelper.defaultConfig()
                             .with(PostgresConnectorConfig.SNAPSHOT_MODE, PostgresConnectorConfig.SnapshotMode.NEVER.getValue())
                             .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE)
                             .build();
    start(YugabyteDBConnector.class, config);
    assertConnectorIsRunning();

    // YB Note: Added a wait for replication slot to be active.
    TestHelper.waitFor(Duration.ofSeconds(10));

    waitForAvailableRecords(10_000, TimeUnit.MILLISECONDS);
    // there shouldn't be any snapshot records
    assertNoRecordsToConsume();

    // insert and verify 2 new records
    TestHelper.execute("INSERT INTO s2.a VALUES (1, 22, 'random text value');");
    TestHelper.execute("DELETE FROM s2.a WHERE pk = 1;");

    SourceRecords actualRecords = consumeRecordsByTopic(2);
    List<SourceRecord> records = actualRecords.recordsForTopic(topicName("s2.a"));

    SourceRecord insertRecord = records.get(0);
    SourceRecord deleteRecord = records.get(1);

    YBVerifyRecord.isValidInsert(insertRecord, PK_FIELD, 1);
    YBVerifyRecord.isValidDelete(deleteRecord, PK_FIELD, 1);

    Struct deleteRecordValue = (Struct) deleteRecord.value();
    assertThat(deleteRecordValue.get(Envelope.FieldName.AFTER)).isNull();
    assertThat(deleteRecordValue.get(Envelope.FieldName.BEFORE)).isNotNull();

    // Before field will have entries only for the primary key columns.
    assertThat(deleteRecordValue.getStruct(Envelope.FieldName.BEFORE).getStruct("pk").getInt32("value")).isEqualTo(1);
    assertThat(deleteRecordValue.getStruct(Envelope.FieldName.BEFORE).getStruct("aa").getInt32("value")).isNull();
    assertThat(deleteRecordValue.getStruct(Envelope.FieldName.BEFORE).getStruct("bb").getString("value")).isNull();
  }
}
