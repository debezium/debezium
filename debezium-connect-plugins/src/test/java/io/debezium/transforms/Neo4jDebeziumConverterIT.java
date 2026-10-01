/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.SessionConfig;
import org.testcontainers.containers.Neo4jContainer;
import org.testcontainers.utility.DockerImageName;

import io.debezium.config.Configuration;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;

/**
 * End-to-end integration tests for {@link Neo4jDebeziumConverter} against a <em>real</em> Neo4j source: a Neo4j
 * Enterprise container with CDC enabled, streamed through the official Neo4j Kafka source connector via the Debezium
 * embedded engine. Each consumed CDC {@link Struct} is fed to the SMT and the emitted Debezium envelope is asserted.
 * This IT is <strong>disabled by default</strong>: it is not compiled or run unless the {@code neo4j-source-it}
 * Maven profile is active (that profile adds the Neo4j connector + testcontainers dependencies and re-includes
 * this class in test compilation, and runs it).
 * The main reason to disable it by default is that <strong>the connector isn't on Maven Central</strong>, org.neo4j.connectors.kafka:neo4j-kafka-connect-neo4j:5.5.4 is installed by hand into local .m2 from a GitHub release (that's why the profile exists and the IT is testExcluded).
 * A default build would simply fail to resolve the dependency on any machine or CI runner that hasn't run the manual install:install-file step.
 * To run manually:
 *
 * <pre>
 *   # one-time: install the connector fat jar (download from github.com/neo4j/neo4j-kafka-connector/releases)
 *   ./mvnw install:install-file -Dfile=neo4j-kafka-connect-neo4j-5.5.4.jar \
 *       -DgroupId=org.neo4j.connectors.kafka -DartifactId=neo4j-kafka-connect-neo4j -Dversion=5.5.4 -Dpackaging=jar
 *
 *   ./mvnw -pl debezium-connect-plugins verify -Pneo4j-source-it -Dit.test=Neo4jDebeziumConverterIT
 * </pre>
 */
public class Neo4jDebeziumConverterIT extends AbstractAsyncEngineConnectorTest {

    private static final String NEO4J_IMAGE = "neo4j:5.26.0-enterprise";
    private static final String PASSWORD = "neo4jpass";
    private static final String CONNECTOR_CLASS = "org.neo4j.connectors.kafka.source.Neo4jConnector";

    private static final List<String> KEY_LABELS = List.of(
            "Person", "ZeroCfgNode", "UpdNode", "DelNode", "EmployeeNode", "NamedNode",
            "InclNode", "ExclNode", "KeyChangeNode", "Order", "Product", "Customer", "Friend");

    @SuppressWarnings("resource")
    private static final Neo4jContainer<?> NEO4J = new Neo4jContainer<>(DockerImageName.parse(NEO4J_IMAGE))
            .withEnv("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes")
            .withAdminPassword(PASSWORD);

    private static Driver driver;

    /** Sequence for generating a unique, isolated database name per test. */
    private static final AtomicInteger DB_SEQ = new AtomicInteger();

    @BeforeAll
    static void startContainer() {
        NEO4J.start();
        driver = GraphDatabase.driver(NEO4J.getBoltUrl(), AuthTokens.basic("neo4j", PASSWORD));
    }

    @AfterAll
    static void stopContainer() {
        if (driver != null) {
            driver.close();
        }
        NEO4J.stop();
    }

    @Test
    @DisplayName("node create with temporal/spatial/array properties becomes an op=c envelope")
    void nodeCreateAllTypes() throws Exception {
        final var records = capture("persons", "(:Person)", 1,
                "CREATE (p:Person {"
                        + "  id: 1, name: 'Alice', age: 30, active: true, score: 9.5,"
                        + "  born: date('1990-01-15'),"
                        + "  wakeup: localtime('08:30:00'),"
                        + "  meeting: time('12:30:00+01:00'),"
                        + "  created: localdatetime('2021-06-15T10:15:30'),"
                        + "  lastSeen: datetime('2021-06-15T10:15:30+01:00'),"
                        + "  span: duration('P1Y2M3DT4H5M6S'),"
                        + "  location: point({latitude: 12.34, longitude: 56.78}),"
                        + "  tags: ['a', 'b', 'c']"
                        + "})");

        final var out = applyOne(records, Map.of("label.Person.key.properties", "id"));
        assertThat(out.topic()).isEqualTo("Person");

        final var envelope = (Struct) out.value();
        assertThat(envelope.getString("op")).isEqualTo("c");

        final var after = envelope.getStruct("after");
        assertThat(after.get("id")).isEqualTo(1L);
        assertThat(after.get("name")).isEqualTo("Alice");
        assertThat(after.get("age")).isEqualTo(30L);
        assertThat(after.get("active")).isEqualTo(true);
        assertThat(after.get("score")).isEqualTo(9.5d);

        // Neo4j date -> io.debezium.time.Date (epoch days, INT32).
        assertThat(after.schema().field("born").schema().name()).isEqualTo("io.debezium.time.Date");
        assertThat(after.get("born")).isEqualTo((int) LocalDate.of(1990, 1, 15).toEpochDay());
        // Neo4j localtime -> io.debezium.time.MicroTime (micros of day, INT64).
        assertThat(after.schema().field("wakeup").schema().name()).isEqualTo("io.debezium.time.MicroTime");
        assertThat(after.get("wakeup")).isEqualTo(LocalTime.of(8, 30, 0).toNanoOfDay() / 1_000L);
        // Neo4j localdatetime -> io.debezium.time.Timestamp (epoch millis, UTC).
        assertThat(after.schema().field("created").schema().name()).isEqualTo("io.debezium.time.Timestamp");
        assertThat(after.get("created"))
                .isEqualTo(LocalDateTime.of(2021, 6, 15, 10, 15, 30).toInstant(ZoneOffset.UTC).toEpochMilli());
        // Neo4j zoned datetime / offset time -> ISO strings under the Debezium logical-type names.
        assertThat(after.schema().field("lastSeen").schema().name()).isEqualTo("io.debezium.time.ZonedTimestamp");
        assertThat(after.get("lastSeen")).isEqualTo("2021-06-15T10:15:30+01:00");
        assertThat(after.schema().field("meeting").schema().name()).isEqualTo("io.debezium.time.ZonedTime");
        assertThat(after.get("meeting")).isEqualTo("12:30:00+01:00");
        // Neo4j duration -> ISO-8601 STRING (years folded into months by Neo4j: 1Y2M -> 14M).
        assertThat(after.schema().field("span").schema().type()).isEqualTo(Schema.Type.STRING);
        assertThat(after.get("span")).isEqualTo("P14M3DT4H5M6S");
        // Neo4j point -> JSON STRING (x = longitude, y = latitude).
        assertThat(after.schema().field("location").schema().type()).isEqualTo(Schema.Type.STRING);
        assertThat((String) after.get("location")).isEqualTo("{\"srid\":4326,\"x\":56.78,\"y\":12.34}");
        // Homogeneous primitive list -> ARRAY column.
        assertThat(after.schema().field("tags").schema().type()).isEqualTo(Schema.Type.ARRAY);
        assertThat(after.get("tags")).isEqualTo(List.of("a", "b", "c"));

        // The primary key is projected into the record key.
        assertThat(((Struct) out.key()).get("id")).isEqualTo(1L);
    }

    @Test
    @DisplayName("zero-config node uses the NODE KEY constraint as its primary key")
    void zeroConfigConstraintKey() throws Exception {
        final var records = capture("zerocfg", "(:ZeroCfgNode)", 1,
                "CREATE (:ZeroCfgNode {id: 10, name: 'Zed'})");

        final var out = applyOne(records, Map.of());
        assertThat(out.topic()).isEqualTo("ZeroCfgNode");
        assertThat(((Struct) out.value()).getStruct("after").get("name")).isEqualTo("Zed");
        assertThat(((Struct) out.key()).get("id")).isEqualTo(10L);
    }

    @Test
    @DisplayName("node update becomes an op=u envelope with the after image")
    void nodeUpdate() throws Exception {
        final var records = capture("upd", "(:UpdNode)", 2,
                "CREATE (:UpdNode {id: 11, email: 'old@x.com'})",
                "MATCH (n:UpdNode {id: 11}) SET n.email = 'new@x.com'");

        final var update = applyByOp(records, Map.of(), "u");
        assertThat(update.getStruct("after").get("email")).isEqualTo("new@x.com");
    }

    @Test
    @DisplayName("node delete becomes an op=d envelope with the before image and record key")
    void nodeDelete() throws Exception {
        final var records = capture("del", "(:DelNode)", 2,
                "CREATE (:DelNode {id: 12, name: 'Doomed'})",
                "MATCH (n:DelNode {id: 12}) DELETE n");

        final var out = applyByOpRecord(records, Map.of(), "d");
        assertThat(((Struct) out.value()).getStruct("before").get("name")).isEqualTo("Doomed");
        assertThat(((Struct) out.key()).get("id")).isEqualTo(12L);
    }

    @Test
    @DisplayName("multi-label node is owned by its single mapped label")
    void multiLabelMappedOwner() throws Exception {
        final var records = capture("emp", "(:EmployeeNode)", 1,
                "CREATE (:EmployeeNode:PersonRole {id: 13, name: 'Eve'})");

        final var out = applyOne(records, Map.of("label.EmployeeNode.table", "employees"));
        assertThat(out.topic()).isEqualTo("employees");
        assertThat(((Struct) out.value()).getStruct("after").get("name")).isEqualTo("Eve");
    }

    @Test
    @DisplayName("snake_case naming and a table override route to the configured table")
    void namingAndTableOverride() throws Exception {
        final var records = capture("named", "(:NamedNode)", 1,
                "CREATE (:NamedNode {id: 14, unitPrice: 9.99})");

        final var out = applyOne(records, Map.of(
                "table.naming", "snake_case",
                "column.naming", "snake_case",
                "label.NamedNode.table", "renamed_items"));
        assertThat(out.topic()).isEqualTo("renamed_items");
        assertThat(((Struct) out.value()).getStruct("after").get("unit_price")).isEqualTo(9.99d);
    }

    @Test
    @DisplayName("properties.include keeps only listed columns while retaining the key")
    void propertiesInclude() throws Exception {
        final var records = capture("incl", "(:InclNode)", 1,
                "CREATE (:InclNode {id: 15, name: 'Ina', secret: 'x', note: 'hi'})");

        final var after = ((Struct) applyOne(records, Map.of("label.InclNode.properties.include", "name")).value())
                .getStruct("after");
        assertThat(after.get("name")).isEqualTo("Ina");
        assertThat(after.get("id")).isEqualTo(15L);
        assertThat(after.schema().field("secret")).isNull();
        assertThat(after.schema().field("note")).isNull();
    }

    @Test
    @DisplayName("properties.exclude drops the column but keeps the key")
    void propertiesExclude() throws Exception {
        final var records = capture("excl", "(:ExclNode)", 1,
                "CREATE (:ExclNode {id: 16, name: 'Xan', secret: 'x'})");

        final var after = ((Struct) applyOne(records, Map.of("label.ExclNode.properties.exclude", "secret")).value())
                .getStruct("after");
        assertThat(after.get("name")).isEqualTo("Xan");
        assertThat(after.get("id")).isEqualTo(16L);
        assertThat(after.schema().field("secret")).isNull();
    }

    @Test
    @DisplayName("label.key.properties overrides the primary key on a label with no constraint")
    void keyPropertiesOverride() throws Exception {
        final var records = capture("overridekey", "(:OverrideKeyNode)", 1,
                "CREATE (:OverrideKeyNode {id: 17, email: 'o@x.com'})");

        final var out = applyOne(records, Map.of("label.OverrideKeyNode.key.properties", "email"));
        final var key = (Struct) out.key();
        assertThat(key.get("email")).isEqualTo("o@x.com");
        assertThat(key.schema().field("id")).isNull();
    }

    // ----- relationship join-table scenarios -----

    @Test
    @DisplayName("relationship becomes a join-table row with a composite key")
    void joinCreate() throws Exception {
        final var records = capture("jcontains", "(:Order)-[:JCONTAINS]->(:Product)", 1,
                "CREATE (o:Order {id: 5001})-[:JCONTAINS {quantity: 3}]->(p:Product {id: 200})");

        final var out = applyOne(records, Map.of(
                "column.naming", "snake_case",
                "relationship.JCONTAINS.table", "order_items"));
        assertThat(out.topic()).isEqualTo("order_items");

        final var after = ((Struct) out.value()).getStruct("after");
        assertThat(after.get("order_id")).isEqualTo(5001L);
        assertThat(after.get("product_id")).isEqualTo(200L);
        assertThat(after.get("quantity")).isEqualTo(3L);

        final var key = (Struct) out.key();
        assertThat(key.get("order_id")).isEqualTo(5001L);
        assertThat(key.get("product_id")).isEqualTo(200L);
    }

    @Test
    @DisplayName("relationship delete emits op=d keyed by both endpoints")
    void joinDelete() throws Exception {
        final var records = capture("jdcontains", "(:Order)-[:JDCONTAINS]->(:Product)", 2,
                "CREATE (o:Order {id: 5002})-[:JDCONTAINS {quantity: 1}]->(p:Product {id: 201})",
                "MATCH (:Order {id: 5002})-[r:JDCONTAINS]->(:Product {id: 201}) DELETE r");

        final var out = applyByOpRecord(records, Map.of("column.naming", "snake_case"), "d");
        final var before = ((Struct) out.value()).getStruct("before");
        assertThat(before.get("order_id")).isEqualTo(5002L);
        assertThat(before.get("product_id")).isEqualTo(201L);
    }

    @Test
    @DisplayName("start.column and end.column override the join-table foreign-key column names")
    void startEndColumnOverride() throws Exception {
        final var records = capture("secontains", "(:Order)-[:SECONTAINS]->(:Product)", 1,
                "CREATE (o:Order {id: 5003})-[:SECONTAINS {quantity: 2}]->(p:Product {id: 202})");

        final var after = ((Struct) applyOne(records, Map.of(
                "relationship.SECONTAINS.start.column", "order_ref",
                "relationship.SECONTAINS.end.column", "product_ref")).value()).getStruct("after");
        assertThat(after.get("order_ref")).isEqualTo(5003L);
        assertThat(after.get("product_ref")).isEqualTo(202L);
        assertThat(after.get("quantity")).isEqualTo(2L);
    }

    @Test
    @DisplayName("relationship properties selection keeps only listed properties")
    void relationshipPropertiesSelection() throws Exception {
        final var records = capture("pscontains", "(:Order)-[:PSCONTAINS]->(:Product)", 1,
                "CREATE (o:Order {id: 5004})-[:PSCONTAINS {quantity: 3, discount: 5, note: 'gift'}]->(p:Product {id: 203})");

        final var after = ((Struct) applyOne(records, Map.of("relationship.PSCONTAINS.properties", "quantity")).value())
                .getStruct("after");
        assertThat(after.get("quantity")).isEqualTo(3L);
        assertThat(after.schema().field("discount")).isNull();
        assertThat(after.schema().field("note")).isNull();
    }

    // ----- relationship foreign-key scenarios -----

    @Test
    @DisplayName("foreign-key relationship becomes a partial update of the start-owned table")
    void fkStartOwner() throws Exception {
        final var records = capture("fkplaced1", "(:Order)-[:FK1_PLACED_BY]->(:Customer)", 1,
                "CREATE (o:Order {id: 6001})-[:FK1_PLACED_BY]->(c:Customer {id: 1004})");

        final var out = applyOne(records, Map.of(
                "column.naming", "snake_case",
                "relationship.FK1_PLACED_BY.mode", "foreign_key",
                "relationship.FK1_PLACED_BY.table", "orders"));
        assertThat(out.topic()).isEqualTo("orders");

        final var payload = (Struct) out.value();
        assertThat(payload.getString("op")).isEqualTo("u");
        final var after = payload.getStruct("after");
        assertThat(after.get("id")).isEqualTo(6001L);
        assertThat(after.get("customer_id")).isEqualTo(1004L);
        assertThat(((Struct) out.key()).get("id")).isEqualTo(6001L);
    }

    @Test
    @DisplayName("owner=end puts the foreign key on the end table")
    void fkOwnerEnd() throws Exception {
        final var records = capture("fkplaced2", "(:Customer)-[:FK2_PLACED]->(:Order)", 1,
                "CREATE (c:Customer {id: 1005})-[:FK2_PLACED]->(o:Order {id: 6002})");

        final var after = ((Struct) applyOne(records, Map.of(
                "column.naming", "snake_case",
                "relationship.FK2_PLACED.mode", "foreign_key",
                "relationship.FK2_PLACED.owner", "end")).value()).getStruct("after");
        assertThat(after.get("id")).isEqualTo(6002L);
        assertThat(after.get("customer_id")).isEqualTo(1005L);
    }

    @Test
    @DisplayName("foreign-key relationship delete nulls the foreign-key column as an update")
    void fkDelete() throws Exception {
        final var records = capture("fkplaced3", "(:Order)-[:FK3_PLACED_BY]->(:Customer)", 2,
                "CREATE (o:Order {id: 6003})-[:FK3_PLACED_BY]->(c:Customer {id: 1006})",
                "MATCH (:Order {id: 6003})-[r:FK3_PLACED_BY]->(:Customer {id: 1006}) DELETE r");

        // Both create and delete map to op=u; the delete is the last emitted and carries a null foreign key.
        final var applied = applySmt(records, Map.of(
                "column.naming", "snake_case",
                "relationship.FK3_PLACED_BY.mode", "foreign_key"));
        final var after = ((Struct) applied.get(applied.size() - 1).value()).getStruct("after");
        assertThat(after.get("id")).isEqualTo(6003L);
        assertThat(after.get("customer_id")).isNull();
    }

    @Test
    @DisplayName("fk.column overrides the derived foreign-key column name")
    void fkColumnOverride() throws Exception {
        final var records = capture("fkplaced4", "(:Order)-[:FK4_PLACED_BY]->(:Customer)", 1,
                "CREATE (o:Order {id: 6004})-[:FK4_PLACED_BY]->(c:Customer {id: 1007})");

        final var after = ((Struct) applyOne(records, Map.of(
                "relationship.FK4_PLACED_BY.mode", "foreign_key",
                "relationship.FK4_PLACED_BY.fk.column", "cust_ref")).value()).getStruct("after");
        assertThat(after.get("cust_ref")).isEqualTo(1007L);
        assertThat(after.schema().field("Customer_id")).isNull();
    }

    @Test
    @DisplayName("an endpoint-qualified relationship mapping wins over the unqualified type")
    void qualifiedRelationship() throws Exception {
        final var records = capture("qknows", "(:Friend)-[:QKNOWS]->(:Friend)", 1,
                "CREATE (a:Friend {id: 7001})-[:QKNOWS]->(b:Friend {id: 7002})");

        final var out = applyOne(records, Map.of(
                "relationship.QKNOWS.table", "generic_knows",
                "relationship.QKNOWS@Friend-Friend.table", "friendships"));
        assertThat(out.topic()).isEqualTo("friendships");
    }

    // ----- behavior scenarios -----

    @Test
    @DisplayName("a node with no key and no override is dropped")
    void missingKeyDropped() throws Exception {
        final var records = capture("nokey", "(:NoKeyNode)", 1,
                "CREATE (:NoKeyNode {id: 18, name: 'Nk'})");
        assertThat(applySmt(records, Map.of())).isEmpty();
    }

    @Test
    @DisplayName("a primary-key value change is rejected when field.missing.behavior=fail")
    void keyValueChangeRejected() throws Exception {
        final var records = capture("keychange", "(:KeyChangeNode)", 2,
                "CREATE (:KeyChangeNode {id: 19, name: 'K'})",
                "MATCH (n:KeyChangeNode {id: 19}) SET n.id = 29");

        final var update = records.get(records.size() - 1);
        try (var smt = new Neo4jDebeziumConverter<SourceRecord>()) {
            smt.configure(Map.of("field.missing.behavior", "fail"));
            assertThatThrownBy(() -> smt.apply(update))
                    .isInstanceOf(DataException.class)
                    .hasMessageContaining("primary-key value");
        }
    }

    @Test
    @DisplayName("field.missing.behavior=ignore drops a keyless record silently")
    void ignoreDropsSilently() throws Exception {
        final var records = capture("ignorenode", "(:IgnoreNode)", 1,
                "CREATE (:IgnoreNode {id: 20, name: 'Ig'})");
        assertThat(applySmt(records, Map.of("field.missing.behavior", "ignore"))).isEmpty();
    }

    /**
     * Creates a fresh, isolated Neo4j database with CDC enrichment enabled and the standard NODE KEY constraints,
     * so every test replays only its own CDC log regardless of the labels it reuses.
     */
    private String createIsolatedDatabase() {
        final String database = "itdb" + DB_SEQ.incrementAndGet();
        try (var session = driver.session(SessionConfig.forDatabase("system"))) {
            session.run("CREATE DATABASE " + database + " WAIT").consume();
            session.run("ALTER DATABASE " + database + " SET OPTION txLogEnrichment 'FULL'").consume();
        }
        // The enrichment setting is applied asynchronously to subsequent transactions; wait until the database
        // actually reports txLogEnrichment=FULL
        awaitEnrichmentFull(database);
        createConstraints(database);
        return database;
    }

    /** Polls {@code SHOW DATABASE} until the asynchronously-applied {@code txLogEnrichment} option reads {@code FULL}. */
    private static void awaitEnrichmentFull(String database) {
        Awaitility.await("txLogEnrichment=FULL for database " + database)
                .atMost(30, TimeUnit.SECONDS)
                .pollInterval(250, TimeUnit.MILLISECONDS)
                .until(() -> isEnrichmentFull(database));
    }

    private static boolean isEnrichmentFull(String database) {
        try (var session = driver.session(SessionConfig.forDatabase("system"))) {
            return session.run("SHOW DATABASE " + database + " YIELD options").list().stream()
                    .anyMatch(record -> "FULL".equals(
                            String.valueOf(record.get("options").asMap().get("txLogEnrichment"))));
        }
    }

    private static void createConstraints(String database) {
        try (var session = driver.session(SessionConfig.forDatabase(database))) {
            for (final var label : KEY_LABELS) {
                session.run(String.format(
                        "CREATE CONSTRAINT %s_id IF NOT EXISTS FOR (n:%s) REQUIRE n.id IS NODE KEY",
                        label.toLowerCase(Locale.ROOT), label)).consume();
            }
        }
    }

    private void run(String database, String cypher) {
        try (var session = driver.session(SessionConfig.forDatabase(database))) {
            session.run(cypher).consume();
        }
    }

    /**
     * Creates a fresh isolated database, applies the given graph mutations (committed), then starts a connector
     * scoped to {@code pattern} on {@code topic} and consumes exactly {@code count} CDC records from it. Offsets
     * are cleared per test and {@code start-from=EARLIEST}, so the pre-created changes are replayed deterministically.
     */
    private List<SourceRecord> capture(String topic, String pattern, int count, String... cyphers) throws Exception {
        final String database = createIsolatedDatabase();
        for (final var cypher : cyphers) {
            run(database, cypher);
        }
        start(connectorClass(), connectorConfig(topic, pattern, database));
        assertConnectorIsRunning();
        setConsumeTimeout(60, TimeUnit.SECONDS);

        final var records = consumeRecordsByTopic(count).recordsForTopic(topic);
        assertThat(records).as("CDC records on topic '%s'", topic).hasSize(count);
        return records;
    }

    /** Applies the SMT to every record and returns the emitted (non-dropped) records, in order. */
    private static List<SourceRecord> applySmt(List<SourceRecord> in, Map<String, String> smtConfig) {
        try (var smt = new Neo4jDebeziumConverter<SourceRecord>()) {
            smt.configure(smtConfig);
            final var out = new ArrayList<SourceRecord>();
            for (final var record : in) {
                final var emitted = smt.apply(record);
                if (emitted != null) {
                    out.add(emitted);
                }
            }
            return out;
        }
    }

    /** Applies the SMT and asserts exactly one record was emitted, returning it. */
    private static SourceRecord applyOne(List<SourceRecord> in, Map<String, String> smtConfig) {
        final var out = applySmt(in, smtConfig);
        assertThat(out).as("exactly one emitted record").hasSize(1);
        return out.get(0);
    }

    /** Applies the SMT and returns the envelope value of the single record with the given op. */
    private static Struct applyByOp(List<SourceRecord> in, Map<String, String> smtConfig, String op) {
        return (Struct) applyByOpRecord(in, smtConfig, op).value();
    }

    /** Applies the SMT and returns the single emitted record whose envelope op matches. */
    private static SourceRecord applyByOpRecord(List<SourceRecord> in, Map<String, String> smtConfig, String op) {
        final var matches = applySmt(in, smtConfig).stream()
                .filter(r -> op.equals(((Struct) r.value()).getString("op")))
                .toList();
        assertThat(matches).as("exactly one emitted record with op=%s", op).hasSize(1);
        return matches.get(0);
    }

    private Configuration connectorConfig(String topic, String pattern, String database) {
        return Configuration.create()
                .with("neo4j.uri", NEO4J.getBoltUrl())
                .with("neo4j.authentication.type", "BASIC")
                .with("neo4j.authentication.basic.username", "neo4j")
                .with("neo4j.authentication.basic.password", PASSWORD)
                .with("neo4j.database", database)
                .with("neo4j.source-strategy", "CDC")
                .with("neo4j.start-from", "EARLIEST")
                .with("neo4j.payload-mode", "EXTENDED")
                .with("neo4j.cdc.poll-interval", "500ms")
                .with("neo4j.cdc.poll-duration", "1s")
                .with("neo4j.cdc.topic." + topic + ".patterns", pattern)
                .with("neo4j.cdc.topic." + topic + ".key-strategy", "SKIP")
                .with("key.converter", "org.apache.kafka.connect.json.JsonConverter")
                .with("value.converter", "org.apache.kafka.connect.json.JsonConverter")
                .build();
    }

    @SuppressWarnings("unchecked")
    private static Class<? extends SourceConnector> connectorClass() throws ClassNotFoundException {
        return (Class<? extends SourceConnector>) Class.forName(CONNECTOR_CLASS);
    }

}
