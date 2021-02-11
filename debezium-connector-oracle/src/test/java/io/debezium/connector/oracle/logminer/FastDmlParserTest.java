/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import static org.fest.assertions.Assertions.assertThat;

import java.sql.Types;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.junit.Before;
import org.junit.Test;

import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.jsqlparser.SimpleDmlParser;
import io.debezium.connector.oracle.logminer.valueholder.LogMinerDmlEntry;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.data.Envelope.Operation;
import io.debezium.doc.FixFor;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.Table;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.util.Testing;

/**
 * @author Chris Cranford
 */
public class FastDmlParserTest {

    private static final String CATALOG_NAME = "ORCLCDB1";
    private static final String SCHEMA_NAME = "DEBEZIUM";
    private final List<Integer> iterations = Arrays.asList(1000, 5000, 10000, 20000, 50000, 100000, 500000, 1000000);

    private SimpleDmlParser simpleDmlParser;
    private FastDmlParser fastDmlParser;

    @Before
    public void beforeEach() throws Exception {
        // Create SimpleDmlParser
        OracleConnection jdbcConnection = TestHelper.testConnection();
        OracleConnectorConfig connectorConfig = new OracleConnectorConfig(TestHelper.defaultConfig().build());
        OracleChangeRecordValueConverter converters = new OracleChangeRecordValueConverter(connectorConfig, jdbcConnection);
        simpleDmlParser = new SimpleDmlParser(CATALOG_NAME, SCHEMA_NAME, converters);

        // Create FastDmlParser
        fastDmlParser = new FastDmlParser();
    }

    // Oracle's generated SQL avoids common spacing patterns such as spaces between column values or values
    // in an insert statement and is explicit about spacing and commas with SET and WHERE clauses. As of
    // now the parser expects this explicit spacing usage.

    @Test
    @FixFor("DBZ-3078")
    public void testParsingInsert() throws Exception {
        String sql = "insert into \"DEBEZIUM\".\"TEST\"(\"ID\",\"NAME\",\"TS\",\"DATE\") values " +
                "('1','Acme',TO_TIMESTAMP('2020-02-01 00:00:00.'),TO_DATE('2020-02-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS'));";

        LogMinerDmlEntry entry = fastDmlParser.parse(sql);
        assertThat(entry.getCommandType()).isEqualTo(Operation.CREATE);
        assertThat(entry.getOldValues()).isEmpty();
        assertThat(entry.getNewValues()).hasSize(4);
        assertThat(entry.getNewValues().get(0).getColumnName()).isEqualTo("ID");
        assertThat(entry.getNewValues().get(1).getColumnName()).isEqualTo("NAME");
        assertThat(entry.getNewValues().get(2).getColumnName()).isEqualTo("TS");
        assertThat(entry.getNewValues().get(3).getColumnName()).isEqualTo("DATE");
        assertThat(entry.getNewValues().get(0).getColumnData()).isEqualTo("1");
        assertThat(entry.getNewValues().get(1).getColumnData()).isEqualTo("Acme");
        assertThat(entry.getNewValues().get(2).getColumnData()).isEqualTo("TO_TIMESTAMP('2020-02-01 00:00:00.')");
        assertThat(entry.getNewValues().get(3).getColumnData()).isEqualTo("TO_DATE('2020-02-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')");
    }

    @Test
    @FixFor("DBZ-3078")
    public void testParsingUpdate() throws Exception {
        String sql = "update \"DEBEZIUM\".\"TEST\" " +
                "set \"NAME\" = 'Bob', \"TS\" = TO_TIMESTAMP('2020-02-02 00:00:00.'), \"DATE\" = TO_DATE('2020-02-02 00:00:00', 'YYYY-MM-DD HH24:MI:SS') " +
                "where \"ID\" = '1' and \"NAME\" = 'Acme' and \"TS\" = TO_TIMESTAMP('2020-02-01 00:00:00.') and \"DATE\" = TO_DATE('2020-02-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS');";

        LogMinerDmlEntry entry = fastDmlParser.parse(sql);
        assertThat(entry.getCommandType()).isEqualTo(Operation.UPDATE);
        assertThat(entry.getOldValues()).hasSize(4);
        assertThat(entry.getOldValues().get(0).getColumnName()).isEqualTo("ID");
        assertThat(entry.getOldValues().get(1).getColumnName()).isEqualTo("NAME");
        assertThat(entry.getOldValues().get(2).getColumnName()).isEqualTo("TS");
        assertThat(entry.getOldValues().get(3).getColumnName()).isEqualTo("DATE");
        assertThat(entry.getOldValues().get(0).getColumnData()).isEqualTo("1");
        assertThat(entry.getOldValues().get(1).getColumnData()).isEqualTo("Acme");
        assertThat(entry.getOldValues().get(2).getColumnData()).isEqualTo("TO_TIMESTAMP('2020-02-01 00:00:00.')");
        assertThat(entry.getOldValues().get(3).getColumnData()).isEqualTo("TO_DATE('2020-02-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')");
        assertThat(entry.getNewValues()).hasSize(4);
        assertThat(entry.getNewValues().get(0).getColumnName()).isEqualTo("ID");
        assertThat(entry.getNewValues().get(1).getColumnName()).isEqualTo("NAME");
        assertThat(entry.getNewValues().get(2).getColumnName()).isEqualTo("TS");
        assertThat(entry.getNewValues().get(3).getColumnName()).isEqualTo("DATE");
        assertThat(entry.getNewValues().get(0).getColumnData()).isEqualTo("1");
        assertThat(entry.getNewValues().get(1).getColumnData()).isEqualTo("Bob");
        assertThat(entry.getNewValues().get(2).getColumnData()).isEqualTo("TO_TIMESTAMP('2020-02-02 00:00:00.')");
        assertThat(entry.getNewValues().get(3).getColumnData()).isEqualTo("TO_DATE('2020-02-02 00:00:00', 'YYYY-MM-DD HH24:MI:SS')");
    }

    @Test
    @FixFor("DBZ-3078")
    public void testParsingDelete() throws Exception {
        String sql = "delete from \"DEBEZIUM\".\"TEST\" " +
                "where \"ID\" = '1' and \"NAME\" = 'Acme' and \"TS\" = TO_TIMESTAMP('2020-02-01 00:00:00.') and \"DATE\" = TO_DATE('2020-02-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS');";

        LogMinerDmlEntry entry = fastDmlParser.parse(sql);
        assertThat(entry.getCommandType()).isEqualTo(Operation.DELETE);
        assertThat(entry.getOldValues()).hasSize(4);
        assertThat(entry.getOldValues().get(0).getColumnName()).isEqualTo("ID");
        assertThat(entry.getOldValues().get(1).getColumnName()).isEqualTo("NAME");
        assertThat(entry.getOldValues().get(2).getColumnName()).isEqualTo("TS");
        assertThat(entry.getOldValues().get(3).getColumnName()).isEqualTo("DATE");
        assertThat(entry.getOldValues().get(0).getColumnData()).isEqualTo("1");
        assertThat(entry.getOldValues().get(1).getColumnData()).isEqualTo("Acme");
        assertThat(entry.getOldValues().get(2).getColumnData()).isEqualTo("TO_TIMESTAMP('2020-02-01 00:00:00.')");
        assertThat(entry.getOldValues().get(3).getColumnData()).isEqualTo("TO_DATE('2020-02-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')");
        assertThat(entry.getNewValues()).isEmpty();
    }

    @Test
    @FixFor("DBZ-3078")
    public void benchmarkInserts() throws Exception {
        Testing.Print.enable();

        final String STATEMENT = "insert into \"DEBEZIUM\".\"TEST\"(\"ID\",\"NAME\",\"CREATED\") values ('0','Test0',TO_TIMESTAMP('2020-02-01 00:00:00'));";

        TableEditor editor = Table.editor();
        editor.tableId(new TableId(CATALOG_NAME, SCHEMA_NAME, "TEST"));

        ColumnEditor columnEditor = Column.editor();
        Column c1 = columnEditor.name("ID").type("NUMERIC").jdbcType(Types.NUMERIC).create();
        Column c2 = columnEditor.name("NAME").type("VARCHAR2").jdbcType(Types.VARCHAR).create();
        Column c3 = columnEditor.name("CREATED").type("TIMESTAMP").jdbcType(Types.TIMESTAMP).create();
        Table table = editor.addColumns(c1, c2, c3).create();

        Tables tables = new Tables();
        tables.overwriteTable(table);

        benchmark(iterations,
                tables,
                entry -> {
                    assertThat(entry).isNotNull();
                    assertThat(entry.getCommandType()).isEqualTo(Operation.CREATE);
                    assertThat(entry.getOldValues()).isEmpty();
                    assertThat(entry.getNewValues()).isNotEmpty();
                    assertThat(entry.getNewValues()).hasSize(3);
                    assertThat(entry.getNewValues().get(0).getColumnName()).isEqualTo("ID");
                    assertThat(entry.getNewValues().get(1).getColumnName()).isEqualTo("NAME");
                    assertThat(entry.getNewValues().get(2).getColumnName()).isEqualTo("CREATED");
                    assertThat(entry.getNewValues().get(1).getColumnData()).isEqualTo("Test0");
                    assertThat(entry.getNewValues().get(2).getColumnData()).isEqualTo(1580515200000L);
                },
                entry -> {
                    assertThat(entry).isNotNull();
                    assertThat(entry.getCommandType()).isEqualTo(Operation.CREATE);
                    assertThat(entry.getOldValues()).isEmpty();
                    assertThat(entry.getNewValues()).isNotEmpty();
                    assertThat(entry.getNewValues()).hasSize(3);
                    assertThat(entry.getNewValues().get(0).getColumnName()).isEqualTo("ID");
                    assertThat(entry.getNewValues().get(1).getColumnName()).isEqualTo("NAME");
                    assertThat(entry.getNewValues().get(2).getColumnName()).isEqualTo("CREATED");
                    assertThat(entry.getNewValues().get(0).getColumnData()).isEqualTo("0");
                    assertThat(entry.getNewValues().get(1).getColumnData()).isEqualTo("Test0");
                    assertThat(entry.getNewValues().get(2).getColumnData()).isEqualTo("TO_TIMESTAMP('2020-02-01 00:00:00')");
                },
                STATEMENT);
    }

    @Test
    @FixFor("DBZ-3078")
    public void benchmarkUpdates() throws Exception {
        Testing.Print.enable();

        final String STATEMENT = "update \"DEBEZIUM\".\"TEST\" set \"NAME\" = 'TEST0', \"SCORE\" = '1234.56' " +
                "where \"ID\" = '0' and \"NAME\" = 'Test0' and \"SCORE\" = '2345.67' and \"CREATED\" = TO_TIMESTAMP('2020-02-01 00:00:00.');";

        TableEditor editor = Table.editor();
        editor.tableId(new TableId(CATALOG_NAME, SCHEMA_NAME, "TEST"));

        ColumnEditor columnEditor = Column.editor();
        Column c1 = columnEditor.name("ID").type("NUMERIC").jdbcType(Types.NUMERIC).create();
        Column c2 = columnEditor.name("NAME").type("VARCHAR2").jdbcType(Types.VARCHAR).create();
        Column c3 = columnEditor.name("SCORE").type("FLOAT").jdbcType(Types.FLOAT).create();
        Column c4 = columnEditor.name("CREATED").type("TIMESTAMP").jdbcType(Types.TIMESTAMP).create();
        Table table = editor.addColumns(c1, c2, c3, c4).create();

        Tables tables = new Tables();
        tables.overwriteTable(table);

        benchmark(iterations,
                tables,
                entry -> {
                    assertThat(entry).isNotNull();
                    assertThat(entry.getCommandType()).isEqualTo(Operation.UPDATE);
                    assertThat(entry.getOldValues()).hasSize(4);
                    assertThat(entry.getNewValues()).hasSize(4);

                    assertThat(entry.getOldValues().get(0).getColumnName()).isEqualTo("ID");
                    assertThat(entry.getOldValues().get(1).getColumnName()).isEqualTo("NAME");
                    assertThat(entry.getOldValues().get(2).getColumnName()).isEqualTo("SCORE");
                    assertThat(entry.getOldValues().get(3).getColumnName()).isEqualTo("CREATED");
                    assertThat(entry.getNewValues().get(0).getColumnName()).isEqualTo("ID");
                    assertThat(entry.getNewValues().get(1).getColumnName()).isEqualTo("NAME");
                    assertThat(entry.getNewValues().get(2).getColumnName()).isEqualTo("SCORE");
                    assertThat(entry.getNewValues().get(3).getColumnName()).isEqualTo("CREATED");

                    assertThat(entry.getOldValues().get(1).getColumnData()).isEqualTo("Test0");
                    assertThat(entry.getOldValues().get(2).getColumnData()).isEqualTo(2345.67f);
                    assertThat(entry.getNewValues().get(3).getColumnData()).isEqualTo(1580515200000L);
                    assertThat(entry.getNewValues().get(1).getColumnData()).isEqualTo("TEST0");
                    assertThat(entry.getNewValues().get(2).getColumnData()).isEqualTo(1234.56f);
                    assertThat(entry.getNewValues().get(3).getColumnData()).isEqualTo(1580515200000L);
                },
                entry -> {
                    assertThat(entry).isNotNull();
                    assertThat(entry.getCommandType()).isEqualTo(Operation.UPDATE);
                    assertThat(entry.getOldValues()).hasSize(4);
                    assertThat(entry.getNewValues()).hasSize(4);

                    assertThat(entry.getOldValues().get(0).getColumnName()).isEqualTo("ID");
                    assertThat(entry.getOldValues().get(1).getColumnName()).isEqualTo("NAME");
                    assertThat(entry.getOldValues().get(2).getColumnName()).isEqualTo("SCORE");
                    assertThat(entry.getOldValues().get(3).getColumnName()).isEqualTo("CREATED");
                    assertThat(entry.getNewValues().get(0).getColumnName()).isEqualTo("ID");
                    assertThat(entry.getNewValues().get(1).getColumnName()).isEqualTo("NAME");
                    assertThat(entry.getNewValues().get(2).getColumnName()).isEqualTo("SCORE");
                    assertThat(entry.getNewValues().get(3).getColumnName()).isEqualTo("CREATED");

                    assertThat(entry.getOldValues().get(0).getColumnData()).isEqualTo("0");
                    assertThat(entry.getOldValues().get(1).getColumnData()).isEqualTo("Test0");
                    assertThat(entry.getOldValues().get(2).getColumnData()).isEqualTo("2345.67");
                    assertThat(entry.getNewValues().get(3).getColumnData()).isEqualTo("TO_TIMESTAMP('2020-02-01 00:00:00.')");
                    assertThat(entry.getNewValues().get(0).getColumnData()).isEqualTo("0");
                    assertThat(entry.getNewValues().get(1).getColumnData()).isEqualTo("TEST0");
                    assertThat(entry.getNewValues().get(2).getColumnData()).isEqualTo("1234.56");
                    assertThat(entry.getNewValues().get(3).getColumnData()).isEqualTo("TO_TIMESTAMP('2020-02-01 00:00:00.')");
                },
                STATEMENT);
    }

    @Test
    @FixFor("DBZ-3078")
    public void benchmarkDeletes() throws Exception {
        Testing.Print.enable();

        final String STATEMENT = "delete from \"DEBEZIUM\".\"TEST\" where \"ID\" = '2' and \"NAME\" = 'TEST2' and \"CREATED\" = TO_TIMESTAMP('2020-02-01 00:00:00.');";

        TableEditor editor = Table.editor();
        editor.tableId(new TableId(CATALOG_NAME, SCHEMA_NAME, "TEST"));

        ColumnEditor columnEditor = Column.editor();
        Column c1 = columnEditor.name("ID").type("NUMERIC").jdbcType(Types.NUMERIC).create();
        Column c2 = columnEditor.name("NAME").type("VARCHAR2").jdbcType(Types.VARCHAR).create();
        Column c3 = columnEditor.name("CREATED").type("TIMESTAMP").jdbcType(Types.TIMESTAMP).create();
        Table table = editor.addColumns(c1, c2, c3).create();

        Tables tables = new Tables();
        tables.overwriteTable(table);

        benchmark(iterations,
                tables,
                entry -> {
                    assertThat(entry).isNotNull();
                    assertThat(entry.getCommandType()).isEqualTo(Operation.DELETE);
                    assertThat(entry.getOldValues()).hasSize(3);
                    assertThat(entry.getNewValues()).isEmpty();

                    assertThat(entry.getOldValues().get(0).getColumnName()).isEqualTo("ID");
                    assertThat(entry.getOldValues().get(1).getColumnName()).isEqualTo("NAME");
                    assertThat(entry.getOldValues().get(2).getColumnName()).isEqualTo("CREATED");

                    assertThat(entry.getOldValues().get(1).getColumnData()).isEqualTo("TEST2");
                    assertThat(entry.getOldValues().get(2).getColumnData()).isEqualTo(1580515200000L);
                },
                entry -> {
                    assertThat(entry).isNotNull();
                    assertThat(entry.getCommandType()).isEqualTo(Operation.DELETE);
                    assertThat(entry.getOldValues()).hasSize(3);
                    assertThat(entry.getNewValues()).isEmpty();
                    assertThat(entry.getOldValues().get(0).getColumnName()).isEqualTo("ID");
                    assertThat(entry.getOldValues().get(1).getColumnName()).isEqualTo("NAME");
                    assertThat(entry.getOldValues().get(2).getColumnName()).isEqualTo("CREATED");

                    assertThat(entry.getOldValues().get(0).getColumnData()).isEqualTo("2");
                    assertThat(entry.getOldValues().get(1).getColumnData()).isEqualTo("TEST2");
                    assertThat(entry.getOldValues().get(2).getColumnData()).isEqualTo("TO_TIMESTAMP('2020-02-01 00:00:00.')");
                },
                STATEMENT);
    }

    private void benchmark(List<Integer> iterationList, Tables tables, Consumer<LogMinerDmlEntry> oldValidator, Consumer<LogMinerDmlEntry> newValidator, String... sql) {
        Map<String, List<String>> metrics = new LinkedHashMap<>();
        for (Integer iterations : iterationList) {
            List<String> parses = metrics.computeIfAbsent(iterations.toString(), e -> new ArrayList<>());
            Duration time = Duration.ZERO;
            for (int i = 0; i < iterations; ++i) {
                for (int j = 0; j < sql.length; ++j) {
                    Instant s = Instant.now();
                    LogMinerDmlEntry entry = simpleDmlParser.parse(sql[j], tables, "1234567890");
                    time = time.plus(Duration.between(s, Instant.now()));
                    oldValidator.accept(entry);
                }
            }
            double pps = (iterations / (time.toMillis() / 1000.f));
            parses.add(Double.isInfinite(pps) ? "Infinite" : String.format("%.0f", pps));

            time = Duration.ZERO;
            for (int i = 0; i < iterations; ++i) {
                for (int j = 0; j < sql.length; ++j) {
                    Instant s = Instant.now();
                    LogMinerDmlEntry entry = fastDmlParser.parse(sql[j]);
                    time = time.plus(Duration.between(s, Instant.now()));
                    newValidator.accept(entry);
                }
            }
            pps = (iterations / (time.toMillis() / 1000.f));
            parses.add(Double.isInfinite(pps) ? "Infinity" : String.format("%.0f", pps));
        }

        Testing.print("||Iterations||Old Parses/Sec||New Parses/Sec||");
        for (Map.Entry<String, List<String>> entry : metrics.entrySet()) {
            Testing.print(entry.getKey() + "|" + entry.getValue().get(0) + "|" + entry.getValue().get(1));
        }
    }
}
