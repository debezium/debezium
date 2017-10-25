/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import static org.fest.assertions.Assertions.assertThat;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.data.VerifyRecord;
import io.debezium.embedded.AbstractConnectorTest;

/**
 * Integration test for the Debezium Oracle connector.
 *
 * @author Gunnar Morling
 */
public class OracleConnectorIT extends AbstractConnectorTest {

    private static final long MICROS_PER_SECOND = TimeUnit.SECONDS.toMicros(1);
    private static OracleConnection connection;

    @BeforeClass
    public static void beforeClass() throws SQLException {
        connection = TestHelper.testConnection();
//        TestHelper.dropAllSchemas();
    }

    @AfterClass
    public static void closeConnection() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    @Before
    public void before() {
        initializeConnectorTestFramework();
    }

    @Test
    public void shouldReadChangeStreamForExistingTable() throws Exception {
        TestHelper.dropTable(connection, "debezium.customer");

        String ddl = "create table debezium.customer (" +
                "  id int not null, " +
                "  name varchar2(1000), " +
                "  score decimal(6, 2), " +
                "  registered timestamp, " +
                "  primary key (id)" +
                ")";

        connection.execute(ddl);
        connection.execute("GRANT SELECT ON debezium.customer to c##xstrmadmin");

        Configuration config = TestHelper.defaultConfig().build();

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        Thread.sleep(1000);

        connection.execute("INSERT INTO debezium.customer VALUES (1, 'Billie-Bob', 1234.56, TO_DATE('2018/02/22', 'yyyy-mm-dd'))");
        connection.execute("COMMIT");

        SourceRecords records = consumeRecordsByTopic(1);

        List<SourceRecord> testTableRecords = records.recordsForTopic("server1.ORCLPDB1.DEBEZIUM.CUSTOMER");
        assertThat(testTableRecords).hasSize(1);

        VerifyRecord.isValidInsert(testTableRecords.get(0));
        Struct after = (Struct) ((Struct)testTableRecords.get(0).value()).get("after");
        assertThat(after.get("ID")).isEqualTo(BigDecimal.valueOf(1));
        assertThat(after.get("NAME")).isEqualTo("Billie-Bob");
        assertThat(after.get("SCORE")).isEqualTo(BigDecimal.valueOf(1234.56));
        assertThat(after.get("REGISTERED")).isEqualTo(LocalDateTime.of(2018, 2, 22, 0, 0, 0).toEpochSecond(ZoneOffset.UTC) * MICROS_PER_SECOND);
    }

    @Test
    public void shouldReadChangeStreamForTableCreatedWhileStreaming() throws Exception {
        TestHelper.dropTable(connection, "debezium.customer");

        Configuration config = TestHelper.defaultConfig().build();

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        Thread.sleep(4000);

        String ddl = "create table debezium.customer (" +
                "  id int not null, " +
                "  name varchar2(1000), " +
                "  score decimal(6, 2), " +
                "  registered timestamp, " +
                "  primary key (id)" +
                ")";

        connection.execute(ddl);
        connection.execute("GRANT SELECT ON debezium.customer to c##xstrmadmin");

        connection.execute("INSERT INTO debezium.customer VALUES (2, 'Billie-Bob', 1234.56, TO_DATE('2018/02/22', 'yyyy-mm-dd'))");
        connection.execute("COMMIT");

        SourceRecords records = consumeRecordsByTopic(1);

        List<SourceRecord> testTableRecords = records.recordsForTopic("server1.ORCLPDB1.DEBEZIUM.CUSTOMER");
        assertThat(testTableRecords).hasSize(1);

        VerifyRecord.isValidInsert(testTableRecords.get(0));
        Struct after = (Struct) ((Struct)testTableRecords.get(0).value()).get("after");
        assertThat(after.get("ID")).isEqualTo(BigDecimal.valueOf(2));
        assertThat(after.get("NAME")).isEqualTo("Billie-Bob");
        assertThat(after.get("SCORE")).isEqualTo(BigDecimal.valueOf(1234.56));
        assertThat(after.get("REGISTERED")).isEqualTo(LocalDateTime.of(2018, 2, 22, 0, 0, 0).toEpochSecond(ZoneOffset.UTC) * MICROS_PER_SECOND);
    }
}
