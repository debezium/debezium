/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.util.Interner;
import io.debezium.util.Interner.Mode;

/**
 * Verifies via reflection that all internable fields and instances of the schema object
 * classes are properly routed through {@link Interner}.
 *
 * <p>Three layers of coverage:</p>
 * <ol>
 *   <li><b>String fields</b>: single instance created with {@code new String(...)}.
 *       {@code value == value.intern()} is true if the field was routed through
 *       {@link Interner#intern} (which delegates to {@link String#intern}).</li>
 *   <li><b>Class instances</b>: two independently created equal instances must be the
 *       same object reference, proving the whole object is interned.</li>
 *   <li><b>Reference fields across unequal instances</b>: two unequal instances that
 *       share some field values; those equal-valued fields must be the same reference.
 *       Checked with reflection so any newly added field that bypasses {@link Interner}
 *       is caught automatically.</li>
 * </ol>
 */
public class SchemaObjectInterningReflectionTest {

    private static final Set<Class<?>> BOXED_PRIMITIVES = Set.of(
            Boolean.class, Byte.class, Short.class, Integer.class, Long.class,
            Float.class, Double.class, Character.class);

    @BeforeEach
    void enableInterner() {
        Interner.activate(Mode.SHARED);
    }

    @AfterEach
    void resetInterner() {
        Interner.clear();
        Interner.activate(Mode.OFF);
    }

    // ---- Layer 1: String fields are JVM-interned ----

    @Test
    void columnStringFieldsShouldBeJvmInterned() {
        Column col = Column.editor()
                .name(new String("col_id"))
                .type(new String("BIGINT"), new String("BIGINT UNSIGNED"))
                .jdbcType(Types.BIGINT).length(19).optional(false)
                .charsetName(new String("utf8"))
                .defaultValueExpression(new String("0"))
                .comment(new String("primary key"))
                .create();

        assertAllStringFieldsInterned(col);
    }

    @Test
    void attributeStringFieldsShouldBeJvmInterned() {
        Attribute attr = Attribute.editor()
                .name(new String("engine"))
                .value(new String("InnoDB"))
                .create();

        assertAllStringFieldsInterned(attr);
    }

    @Test
    void tableIdStringFieldsShouldBeJvmInterned() {
        TableId id = new TableId(new String("mydb"), new String("myschema"), new String("orders"));

        assertAllStringFieldsInterned(id);
    }

    @Test
    void tableStringFieldsShouldBeJvmInterned() {
        Table table = Table.editor()
                .tableId(new TableId("cat", "sch", "t"))
                .addColumns(Column.editor().name("id").type("BIGINT").jdbcType(Types.BIGINT)
                        .length(19).optional(false).create())
                .setPrimaryKeyNames("id")
                .setDefaultCharsetName(new String("utf8mb4"))
                .setComment(new String("the main table"))
                .create();

        assertAllStringFieldsInterned(table);
    }

    // ---- Layer 2: Class instances are pool-interned ----

    @Test
    void twoEqualColumnInstancesShouldBeSameObject() {
        Column col1 = Column.editor()
                .name(new String("status"))
                .type(new String("VARCHAR"), new String("VARCHAR(64)"))
                .jdbcType(Types.VARCHAR).length(64).optional(true).create();
        Column col2 = Column.editor()
                .name(new String("status"))
                .type(new String("VARCHAR"), new String("VARCHAR(64)"))
                .jdbcType(Types.VARCHAR).length(64).optional(true).create();

        assertThat(col1).isSameAs(col2);
    }

    @Test
    void twoEqualAttributeInstancesShouldBeSameObject() {
        Attribute attr1 = Attribute.editor().name(new String("engine")).value(new String("InnoDB")).create();
        Attribute attr2 = Attribute.editor().name(new String("engine")).value(new String("InnoDB")).create();

        assertThat(attr1).isSameAs(attr2);
    }

    @Test
    void twoEqualTableInstancesShouldBeSameObject() {
        Table table1 = buildTable("db1", "s1", "orders");
        Table table2 = buildTable("db1", "s1", "orders");

        assertThat(table1).isSameAs(table2);
    }

    @Test
    void twoEqualTableIdInstancesShouldBeSameObjectWhenStoredInTable() {
        // TableId objects are interned as fields of TableImpl: two tables sharing the same
        // logical identifier (same catalog/schema/table) must reuse the same TableId instance.
        Table table1 = buildTable("cat", "sch", "users");
        Table table2 = buildTable("cat", "sch", "users");

        // Same Table instance because the whole Table is interned
        assertThat(table1).isSameAs(table2);
        assertThat(table1.id()).isSameAs(table2.id());
    }

    // ---- Layer 3: Reference fields shared across unequal instances ----

    @Test
    void unequalColumnsWithSameTypeShouldShareTypeStringFields() {
        // Two columns with different names: different Column instances, but common type
        // fields must resolve to the same references.
        Column col1 = Column.editor()
                .name(new String("col_a"))
                .type(new String("VARCHAR"), new String("VARCHAR(255)"))
                .jdbcType(Types.VARCHAR).length(255).optional(true)
                .comment(new String("a note"))
                .create();
        Column col2 = Column.editor()
                .name(new String("col_b"))
                .type(new String("VARCHAR"), new String("VARCHAR(255)"))
                .jdbcType(Types.VARCHAR).length(255).optional(true)
                .comment(new String("a note"))
                .create();

        assertThat(col1).isNotSameAs(col2);
        assertEqualFieldsShareReference(col1, col2);
    }

    @Test
    void unequalColumnsWithSameEnumValuesShouldShareEnumList() {
        // Different Column instances; enumValues list must be shared.
        Column col1 = Column.editor()
                .name(new String("status_a"))
                .type(new String("ENUM"), new String("ENUM('A','B')"))
                .jdbcType(Types.VARCHAR)
                .enumValues(Arrays.asList(new String("A"), new String("B")))
                .create();
        Column col2 = Column.editor()
                .name(new String("status_b"))
                .type(new String("ENUM"), new String("ENUM('A','B')"))
                .jdbcType(Types.VARCHAR)
                .enumValues(Arrays.asList(new String("A"), new String("B")))
                .create();

        assertThat(col1).isNotSameAs(col2);
        assertEqualFieldsShareReference(col1, col2);
    }

    @Test
    void unequalTablesWithSameColumnsShouldShareCollectionFields() {
        // Two tables with different IDs but identical structure; column lists and maps must
        // resolve to the same references.
        Table table1 = buildTable("db1", "s1", "orders");
        Table table2 = buildTable("db2", "s2", "orders");

        assertThat(table1).isNotSameAs(table2);
        assertEqualFieldsShareReference(table1, table2);
    }

    @Test
    void tablesWithSameIdButDifferentStructureShouldShareTableIdInstance() {
        // Simulates a schema change: same table identifier, different column set.
        // Even though the two Table instances are different, they must share the same
        // interned TableId object.
        Table before = Table.editor()
                .tableId(new TableId(new String("cat"), new String("sch"), new String("users")))
                .addColumns(Column.editor().name("id").type("BIGINT").jdbcType(Types.BIGINT)
                        .length(19).optional(false).create())
                .setPrimaryKeyNames("id")
                .setDefaultCharsetName(new String("utf8mb4"))
                .create();
        Table after = Table.editor()
                .tableId(new TableId(new String("cat"), new String("sch"), new String("users")))
                .addColumns(
                        Column.editor().name("id").type("BIGINT").jdbcType(Types.BIGINT)
                                .length(19).optional(false).create(),
                        Column.editor().name("name").type("VARCHAR").jdbcType(Types.VARCHAR)
                                .length(255).optional(true).create())
                .setPrimaryKeyNames("id")
                .setDefaultCharsetName(new String("utf8mb4"))
                .create();

        assertThat(before).isNotSameAs(after);
        assertThat(before.id()).isSameAs(after.id());
        // Reflection confirms all equal-valued fields (including the TableId field) share references
        assertEqualFieldsShareReference(before, after);
    }

    // ---- Reflection helpers ----

    /**
     * Asserts that every non-null String field in the class hierarchy of {@code obj}
     * satisfies {@code value == value.intern()}.
     *
     * <p>Constructing strings with {@code new String(...)} ensures non-literal references,
     * so any field that bypasses {@link Interner#intern} (which delegates to
     * {@link String#intern}) will hold a non-canonical instance and fail.</p>
     */
    private void assertAllStringFieldsInterned(Object obj) {
        List<String> failures = new ArrayList<>();
        walkDeclaredFields(obj.getClass(), field -> {
            if (!String.class.equals(field.getType())) {
                return;
            }
            String value = readField(field, obj);
            if (value != null && value != value.intern()) {
                failures.add(obj.getClass().getSimpleName() + "#" + field.getName() + ": String not interned");
            }
        });
        assertThat(failures).as("String fields not interned").isEmpty();
    }

    /**
     * For every non-primitive, non-boxed reference field declared on the class hierarchy,
     * if both {@code obj1} and {@code obj2} hold an equal (non-null, non-empty) value for
     * that field, asserts the two values are the same object reference.
     *
     * <p>This catches any field (String, Collection, Map, or a custom type like
     * {@link TableId}) that was added to a class without being wired through
     * {@link Interner#intern}: two equal instances would hold distinct references instead
     * of sharing the canonical pool entry.</p>
     *
     * <p>Empty collections are excluded: we do not require them to be interned
     * (no memory benefit).</p>
     */
    private void assertEqualFieldsShareReference(Object obj1, Object obj2) {
        assertThat(obj1.getClass()).isEqualTo(obj2.getClass());
        List<String> failures = new ArrayList<>();
        walkDeclaredFields(obj1.getClass(), field -> {
            Class<?> type = field.getType();
            if (type.isPrimitive() || BOXED_PRIMITIVES.contains(type)) {
                return;
            }
            Object v1 = readField(field, obj1);
            Object v2 = readField(field, obj2);
            if (v1 == null || v2 == null) {
                return;
            }
            if (v1 instanceof Collection && ((Collection<?>) v1).isEmpty()) {
                return;
            }
            if (v1 instanceof Map && ((Map<?, ?>) v1).isEmpty()) {
                return;
            }
            if (!v1.equals(v2)) {
                return; // genuinely different values: not expected to share a reference
            }
            if (v1 != v2) {
                failures.add(obj1.getClass().getSimpleName() + "#" + field.getName()
                        + " (" + type.getSimpleName() + "): equal values are different references");
            }
        });
        assertThat(failures).as("Fields with equal values that should share references").isEmpty();
    }

    private void walkDeclaredFields(Class<?> clazz, Consumer<Field> consumer) {
        for (Class<?> c = clazz; c != null && c != Object.class; c = c.getSuperclass()) {
            for (Field field : c.getDeclaredFields()) {
                if (Modifier.isStatic(field.getModifiers())) {
                    continue;
                }
                field.setAccessible(true);
                consumer.accept(field);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private <T> T readField(Field field, Object obj) {
        try {
            return (T) field.get(obj);
        }
        catch (IllegalAccessException e) {
            throw new RuntimeException("Cannot read field " + field, e);
        }
    }

    private Table buildTable(String catalog, String schema, String table) {
        return Table.editor()
                .tableId(new TableId(catalog, schema, new String(table)))
                .addColumns(
                        Column.editor().name(new String("id")).type(new String("BIGINT"))
                                .jdbcType(Types.BIGINT).length(19).optional(false).create(),
                        Column.editor().name(new String("amount")).type(new String("DECIMAL"))
                                .jdbcType(Types.DECIMAL).length(10).create())
                .setPrimaryKeyNames(new String("id"))
                .setDefaultCharsetName(new String("utf8mb4"))
                .addAttribute(Attribute.editor().name(new String("engine")).value(new String("InnoDB")).create())
                .create();
    }
}
