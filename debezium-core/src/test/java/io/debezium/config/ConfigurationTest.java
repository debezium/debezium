/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.config;

import static io.debezium.relational.RelationalDatabaseConnectorConfig.COLUMN_BLACKLIST;
import static io.debezium.relational.RelationalDatabaseConnectorConfig.COLUMN_EXCLUDE_LIST;
import static io.debezium.relational.RelationalDatabaseConnectorConfig.COLUMN_INCLUDE_LIST;
import static io.debezium.relational.RelationalDatabaseConnectorConfig.COLUMN_WHITELIST;
import static io.debezium.relational.RelationalDatabaseConnectorConfig.MSG_KEY_COLUMNS;
import static org.fest.assertions.Assertions.assertThat;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import org.junit.Before;
import org.junit.Test;

import io.debezium.doc.FixFor;
import io.debezium.function.Predicates;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.history.DatabaseHistory;

/**
 * @author Randall Hauch
 *
 */
public class ConfigurationTest {

    private Configuration config;

    @Before
    public void beforeEach() {
        config = Configuration.create().with("A", "a")
                .with("B", "b")
                .with("1", 1)
                .build();
    }

    @Test
    public void shouldConvertFromProperties() {
        Properties props = new Properties();
        props.setProperty("A", "a");
        props.setProperty("B", "b");
        props.setProperty("1", "1");
        config = Configuration.from(props);
        assertThat(config.getString("A")).isEqualTo("a");
        assertThat(config.getString("B")).isEqualTo("b");
        assertThat(config.getString("1")).isEqualTo("1");
        assertThat(config.getInteger("1")).isEqualTo(1); // converts
        assertThat(config.getBoolean("1")).isNull(); // not a boolean
    }

    @Test
    public void shouldCreateInternalFields() {
        config = Configuration.create().with(Field.createInternal("a"), "a1").build();
        assertThat(config.getString("internal.a")).isEqualTo("a1");
    }

    @Test
    @FixFor("DBZ-1962")
    public void shouldThrowValidationOnDuplicateOldColumnFilterConfigurationOld() {
        config = Configuration.create()
                .with(COLUMN_WHITELIST, ".+aa")
                .with(COLUMN_BLACKLIST, ".+bb")
                .build();

        List<String> errorMessages = config.validate(Field.setOf(COLUMN_EXCLUDE_LIST)).get(COLUMN_EXCLUDE_LIST.name()).errorMessages();
        assertThat(errorMessages).isNotEmpty();
        assertThat(errorMessages.get(0)).isEqualTo(RelationalDatabaseConnectorConfig.COLUMN_INCLUDE_LIST_ALREADY_SPECIFIED_ERROR_MSG);
    }

    @Test
    @FixFor("DBZ-1962")
    public void shouldThrowValidationOnDuplicateOldColumnFilterConfiguration() {
        config = Configuration.create()
                .with(COLUMN_INCLUDE_LIST, ".+aa")
                .with(COLUMN_EXCLUDE_LIST, ".+bb")
                .build();

        List<String> errorMessages = config.validate(Field.setOf(COLUMN_EXCLUDE_LIST)).get(COLUMN_EXCLUDE_LIST.name()).errorMessages();
        assertThat(errorMessages).isNotEmpty();
        assertThat(errorMessages.get(0)).isEqualTo(RelationalDatabaseConnectorConfig.COLUMN_INCLUDE_LIST_ALREADY_SPECIFIED_ERROR_MSG);
    }

    @Test
    @FixFor("DBZ-1962")
    public void shouldThrowValidationOnDuplicateColumnFilterConfiguration() {
        config = Configuration.create()
                .with("column.include.list", ".+aa")
                .with("column.exclude.list", ".+bb")
                .build();

        List<String> errorMessages = config.validate(Field.setOf(COLUMN_EXCLUDE_LIST)).get(COLUMN_EXCLUDE_LIST.name()).errorMessages();
        assertThat(errorMessages).isNotEmpty();
        assertThat(errorMessages.get(0)).isEqualTo(RelationalDatabaseConnectorConfig.COLUMN_INCLUDE_LIST_ALREADY_SPECIFIED_ERROR_MSG);
    }

    @Test
    public void shouldAllowNewColumnFilterIncludeListConfiguration() {
        config = Configuration.create()
                .with("column.include.list", ".+aa")
                .build();

        List<String> errorMessages = config.validate(Field.setOf(COLUMN_EXCLUDE_LIST)).get(COLUMN_EXCLUDE_LIST.name()).errorMessages();
        assertThat(errorMessages).isEmpty();
        errorMessages = config.validate(Field.setOf(COLUMN_INCLUDE_LIST)).get(COLUMN_INCLUDE_LIST.name()).errorMessages();
        assertThat(errorMessages).isEmpty();
    }

    @Test
    public void shouldAllowNewColumnFilterExcludeListConfiguration() {
        config = Configuration.create()
                .with("column.exclude.list", ".+bb")
                .build();

        List<String> errorMessages = config.validate(Field.setOf(COLUMN_EXCLUDE_LIST)).get(COLUMN_EXCLUDE_LIST.name()).errorMessages();
        assertThat(errorMessages).isEmpty();
    }

    @Test
    public void shouldCallFunctionOnEachMatchingFieldUsingRegex() {
        config = Configuration.create()
                .with("column.truncate.to.-10.chars", "should-not-be-matched")
                .with("column.truncate.to.10.chars", "10-chars")
                .with("column.truncate.to.20.chars", "20-chars")
                .with("column.mask.with.20.chars", "20-mask")
                .with("column.mask.with.0.chars", "0-mask")
                .with("column.mask.with.chars", "should-not-be-matched")
                .build();

        // Use a regex that captures an integer using a regex group ...
        AtomicInteger counter = new AtomicInteger();
        config.forEachMatchingFieldNameWithInteger("column\\.truncate\\.to\\.(\\d+)\\.chars", (value, n) -> {
            counter.incrementAndGet();
            assertThat(value).isEqualTo(Integer.toString(n) + "-chars");
        });
        assertThat(counter.get()).isEqualTo(2);

        // Use a regex that captures an integer using a regex group ...
        counter.set(0);
        config.forEachMatchingFieldNameWithInteger("column.mask.with.(\\d+).chars", (value, n) -> {
            counter.incrementAndGet();
            assertThat(value).isEqualTo(Integer.toString(n) + "-mask");
        });
        assertThat(counter.get()).isEqualTo(2);

        // Use a regex that matches the name but also uses a regex group ...
        counter.set(0);
        config.forEachMatchingFieldName("column.mask.with.(\\d+).chars", (name, value) -> {
            counter.incrementAndGet();
            assertThat(name).startsWith("column.mask.with.");
            assertThat(name).endsWith(".chars");
            assertThat(value).endsWith("-mask");
        });
        assertThat(counter.get()).isEqualTo(2);

        // Use a regex that matches all of our fields ...
        counter.set(0);
        config.forEachMatchingFieldName("column.*", (name, value) -> {
            counter.incrementAndGet();
            assertThat(name).startsWith("column.");
            assertThat(name).endsWith(".chars");
            assertThat(value).isNotNull();
        });
        assertThat(counter.get()).isEqualTo(6);
    }

    @Test
    public void shouldMaskPasswords() {
        Pattern p = Pattern.compile(".*password$", Pattern.CASE_INSENSITIVE);
        assertThat(p.matcher("password").matches()).isTrue();
        assertThat(p.matcher("otherpassword").matches()).isTrue();

        config = Configuration.create()
                .with("column.password", "warning")
                .with("column.Password.this.is.not", "value")
                .with("column.truncate.to.20.chars", "20-chars")
                .with("column.mask.with.20.chars", "20-mask")
                .with("column.mask.with.0.chars", "0-mask")
                .with("column.mask.with.chars", "should-not-be-matched")
                .build();
        assertThat(config.withMaskedPasswords().toString().contains("warning")).isFalse();
        assertThat(config.toString().contains("warning")).isFalse();
        assertThat(config.withMaskedPasswords().getString("column.password")).isEqualTo("********");
        assertThat(config.getString("column.password")).isEqualTo("warning");
    }

    /**
     * On Amazon RDS we'll see INSERT (sic!) statements for a heartbeat table in the DDL even stream, they should
     * be filtered out by default (the reason being that those statements are sent using STATEMENT binlog format,
     * which is just applied for that session emitting those statements).
     */
    @Test
    @FixFor("DBZ-469")
    public void defaultDdlFilterShouldFilterOutRdsHeartbeatInsert() {
        String defaultDdlFilter = Configuration.create().build().getString(DatabaseHistory.DDL_FILTER);
        Predicate<String> ddlFilter = Predicates.includes(defaultDdlFilter);
        assertThat(ddlFilter.test("INSERT INTO mysql.rds_heartbeat2(id, value) values (1,1510678117058) ON DUPLICATE KEY UPDATE value = 1510678117058")).isTrue();
    }

    @Test
    @FixFor("DBZ-661")
    public void defaultDdlFilterShouldFilterOutFlushRelayLogs() {
        String defaultDdlFilter = Configuration.create().build().getString(DatabaseHistory.DDL_FILTER);
        Predicate<String> ddlFilter = Predicates.includes(defaultDdlFilter);
        assertThat(ddlFilter.test("FLUSH RELAY LOGS")).isTrue();
    }

    @Test
    @FixFor("DBZ-1492")
    public void defaultDdlFilterShouldFilterOutRdsSysinfoStatements() {
        String defaultDdlFilter = Configuration.create().build().getString(DatabaseHistory.DDL_FILTER);
        Predicate<String> ddlFilter = Predicates.includes(defaultDdlFilter);
        assertThat(ddlFilter.test("DELETE FROM mysql.rds_sysinfo where name = 'innodb_txn_key'")).isTrue();
        assertThat(ddlFilter.test("INSERT INTO mysql.rds_sysinfo(name, value) values ('innodb_txn_key','Thu Sep 19 19:38:23 UTC 2019')")).isTrue();
    }

    @Test
    @FixFor("DBZ-1775")
    public void defaultDdlFilterShouldFilterOutRdsMonitorStatements() {
        String defaultDdlFilter = Configuration.create().build().getString(DatabaseHistory.DDL_FILTER);
        Predicate<String> ddlFilter = Predicates.includes(defaultDdlFilter);
        assertThat(ddlFilter.test("DELETE FROM mysql.rds_monitor")).isTrue();
    }

    @Test
    @FixFor("DBZ-1015")
    public void testMsgKeyColumnsField() {
        // null : ok
        config = Configuration.create().build();
        assertThat(config.validate(Field.setOf(MSG_KEY_COLUMNS)).get(MSG_KEY_COLUMNS.name()).errorMessages()).isEmpty();
        // empty field: error
        config = Configuration.create().with(MSG_KEY_COLUMNS, "").build();
        assertThat(config.validate(Field.setOf(MSG_KEY_COLUMNS)).get(MSG_KEY_COLUMNS.name()).errorMessages()).isNotEmpty();
        // field: ok
        config = Configuration.create().with(MSG_KEY_COLUMNS, "t1:C1").build();
        assertThat(config.validate(Field.setOf(MSG_KEY_COLUMNS)).get(MSG_KEY_COLUMNS.name()).errorMessages()).isEmpty();
        // field: ok
        config = Configuration.create().with(MSG_KEY_COLUMNS, "t1:C1,C2").build();
        assertThat(config.validate(Field.setOf(MSG_KEY_COLUMNS)).get(MSG_KEY_COLUMNS.name()).errorMessages()).isEmpty();
        // field: ok
        config = Configuration.create().with(MSG_KEY_COLUMNS, "t1:C1,C2;t2:C1,C2").build();
        assertThat(config.validate(Field.setOf(MSG_KEY_COLUMNS)).get(MSG_KEY_COLUMNS.name()).errorMessages()).isEmpty();
        // field: ok
        config = Configuration.create().with(MSG_KEY_COLUMNS, "t1:C1;(.*).t2:C1,C2").build();
        assertThat(config.validate(Field.setOf(MSG_KEY_COLUMNS)).get(MSG_KEY_COLUMNS.name()).errorMessages()).isEmpty();
        // field: invalid format
        config = Configuration.create().with(MSG_KEY_COLUMNS, "t1,t2").build();
        assertThat(config.validate(Field.setOf(MSG_KEY_COLUMNS)).get(MSG_KEY_COLUMNS.name()).errorMessages()).isNotEmpty();
    }
}
