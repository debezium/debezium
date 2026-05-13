/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import io.debezium.doc.FixFor;

/**
 * Unit tests for {@link TypeId}.
 *
 * @author Debezium Authors
 */
public class TypeIdTest {

    @Test
    @FixFor("debezium/dbz#1334")
    public void shouldParseSimpleTypeName() {
        final var typeId = TypeId.parse("status");

        assertThat(typeId).isNotNull();
        assertThat(typeId.schema()).isNull();
        assertThat(typeId.typeName()).isEqualTo("status");
        assertThat(typeId.identifier()).isEqualTo("status");
    }

    @Test
    @FixFor("debezium/dbz#1334")
    public void shouldParseQuotedTypeName() {
        final var typeId = TypeId.parse("\"Status\"");

        assertThat(typeId).isNotNull();
        assertThat(typeId.schema()).isNull();
        assertThat(typeId.typeName()).isEqualTo("Status");
        assertThat(typeId.identifier()).isEqualTo("Status");
    }

    @Test
    @FixFor("debezium/dbz#1334")
    public void shouldParseSchemaQualifiedTypeName() {
        final var typeId = TypeId.parse("s1.status");

        assertThat(typeId).isNotNull();
        assertThat(typeId.schema()).isEqualTo("s1");
        assertThat(typeId.typeName()).isEqualTo("status");
        assertThat(typeId.identifier()).isEqualTo("s1.status");
    }

    @Test
    @FixFor("debezium/dbz#1334")
    public void shouldParseSchemaQualifiedTypeNameWithQuotedType() {
        final var typeId = TypeId.parse("s1.\"Status\"");

        assertThat(typeId).isNotNull();
        assertThat(typeId.schema()).isEqualTo("s1");
        assertThat(typeId.typeName()).isEqualTo("Status");
        assertThat(typeId.identifier()).isEqualTo("s1.Status");
    }

    @Test
    @FixFor("debezium/dbz#1334")
    public void shouldParseSchemaQualifiedTypeNameWithQuotedSchema() {
        final var typeId = TypeId.parse("\"s1\".status");

        assertThat(typeId).isNotNull();
        assertThat(typeId.schema()).isEqualTo("s1");
        assertThat(typeId.typeName()).isEqualTo("status");
        assertThat(typeId.identifier()).isEqualTo("s1.status");
    }

    @Test
    @FixFor("debezium/dbz#1334")
    public void shouldParseFullyQuotedSchemaQualifiedTypeName() {
        final var typeId = TypeId.parse("\"s1\".\"Status\"");

        assertThat(typeId).isNotNull();
        assertThat(typeId.schema()).isEqualTo("s1");
        assertThat(typeId.typeName()).isEqualTo("Status");
        assertThat(typeId.identifier()).isEqualTo("s1.Status");
    }

    @Test
    @FixFor("debezium/dbz#1334")
    public void shouldParseTypeNameWithEscapedQuotes() {
        final var typeId = TypeId.parse("\"Status\"\"Type\"");

        assertThat(typeId).isNotNull();
        assertThat(typeId.schema()).isNull();
        assertThat(typeId.typeName()).isEqualTo("Status\"Type");
        assertThat(typeId.identifier()).isEqualTo("Status\"Type");
    }

    @Test
    @FixFor("debezium/dbz#1334")
    public void shouldCompareTypeIdsCorrectly() {
        final var typeId1 = TypeId.parse("s1.Status");
        final var typeId2 = TypeId.parse("s1.\"Status\"");
        final var typeId3 = TypeId.parse("s1.status");

        assertThat(typeId1).isEqualTo(typeId2);
        assertThat(typeId1).isNotEqualTo(typeId3);
    }

    @Test
    @FixFor("debezium/dbz#1334")
    public void shouldGenerateDoubleQuotedString() {
        final var typeId = TypeId.parse("s1.Status");

        assertThat(typeId.toDoubleQuotedString()).isEqualTo("\"s1\".\"Status\"");
    }

    @Test
    @FixFor("debezium/dbz#1334")
    public void shouldHandleNullAndEmptyStrings() {
        assertThat(TypeId.parse(null)).isNull();
        assertThat(TypeId.parse("")).isNull();
    }
}
