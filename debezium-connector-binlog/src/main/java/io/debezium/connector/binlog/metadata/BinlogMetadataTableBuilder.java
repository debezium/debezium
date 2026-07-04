/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog.metadata;

import java.sql.Types;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.TableMapEventMetadata;

import io.debezium.connector.binlog.charset.BinlogCharsetRegistry;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.Table;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;

/**
 * Builds a Debezium {@link Table} from the {@code TABLE_MAP} event metadata that MySQL writes into
 * the binlog when {@code binlog_row_metadata=FULL}. This is the basis for the schema-history-free
 * streaming mode (debezium/dbz#978): the streaming schema is reconstructed from the binlog itself,
 * analogous to how the PostgreSQL connector uses pgoutput relation messages.
 * <p>
 * The mapping mirrors what the DDL-based path ({@code MySqlAntlrDdlParser}) produces so that the
 * downstream value converters behave identically: JDBC types follow the same {@code DataTypeEntry}
 * registrations (e.g. TINYINT/SMALLINT &rarr; {@link Types#SMALLINT}, DATETIME &rarr;
 * {@link Types#TIMESTAMP}, TIMESTAMP &rarr; {@link Types#TIMESTAMP_WITH_TIMEZONE}, ENUM/SET &rarr;
 * {@link Types#CHAR}, JSON/GEOMETRY &rarr; {@link Types#OTHER}).
 * <p>
 * Attributes that the binlog metadata does not carry (column defaults, generated-column flag,
 * comments) are intentionally not populated; those are documented limitations of this mode.
 */
public class BinlogMetadataTableBuilder {

    private final Supplier<BinlogCharsetRegistry> charsetRegistrySupplier;
    private BinlogCharsetRegistry charsetRegistry;
    private boolean charsetRegistryResolved;

    /**
     * Creates a builder without a charset registry; collation ids are resolved from a minimal static
     * catalog only. Intended for tests.
     */
    public BinlogMetadataTableBuilder() {
        this(null);
    }

    /**
     * @param charsetRegistrySupplier lazily supplies the connector's charset registry used to resolve
     *            collation ids to character set names; may be null
     */
    public BinlogMetadataTableBuilder(Supplier<BinlogCharsetRegistry> charsetRegistrySupplier) {
        this.charsetRegistrySupplier = charsetRegistrySupplier;
    }

    // MySQL binlog column type codes (com.github.shyiko...deserialization.ColumnType getCode()).
    private static final int TYPE_DECIMAL = 0;
    private static final int TYPE_TINY = 1;
    private static final int TYPE_SHORT = 2;
    private static final int TYPE_LONG = 3;
    private static final int TYPE_FLOAT = 4;
    private static final int TYPE_DOUBLE = 5;
    private static final int TYPE_TIMESTAMP = 7;
    private static final int TYPE_LONGLONG = 8;
    private static final int TYPE_INT24 = 9;
    private static final int TYPE_DATE = 10;
    private static final int TYPE_TIME = 11;
    private static final int TYPE_DATETIME = 12;
    private static final int TYPE_YEAR = 13;
    private static final int TYPE_NEWDATE = 14;
    private static final int TYPE_VARCHAR = 15;
    private static final int TYPE_BIT = 16;
    private static final int TYPE_TIMESTAMP_V2 = 17;
    private static final int TYPE_DATETIME_V2 = 18;
    private static final int TYPE_TIME_V2 = 19;
    private static final int TYPE_VECTOR = 242;
    private static final int TYPE_JSON = 245;
    private static final int TYPE_NEWDECIMAL = 246;
    private static final int TYPE_ENUM = 247;
    private static final int TYPE_SET = 248;
    private static final int TYPE_TINY_BLOB = 249;
    private static final int TYPE_MEDIUM_BLOB = 250;
    private static final int TYPE_LONG_BLOB = 251;
    private static final int TYPE_BLOB = 252;
    private static final int TYPE_VAR_STRING = 253;
    private static final int TYPE_STRING = 254;
    private static final int TYPE_GEOMETRY = 255;

    private static final int BINARY_COLLATION_ID = 63;

    /**
     * Build a {@link Table} for the given identifier from a {@code TABLE_MAP} event that carries FULL
     * row metadata.
     *
     * @param tableId the fully-qualified table identifier; never null
     * @param data the table-map event data; its {@link TableMapEventData#getEventMetadata()} must be
     *            present (i.e. {@code binlog_row_metadata=FULL})
     * @return the reconstructed table
     * @throws IllegalStateException if the FULL metadata (column names) is not present
     */
    public Table build(TableId tableId, TableMapEventData data) {
        final TableMapEventMetadata meta = data.getEventMetadata();
        if (meta == null || meta.getColumnNames() == null || meta.getColumnNames().isEmpty()) {
            throw new IllegalStateException(
                    "TABLE_MAP for '" + tableId + "' has no column metadata; binlog_row_metadata=FULL is required for the binlog-metadata schema mode");
        }

        final byte[] types = data.getColumnTypes();
        final int[] columnMeta = data.getColumnMetadata();
        final BitSet nullability = data.getColumnNullability();
        final List<String> names = meta.getColumnNames();
        final BitSet signedness = meta.getSignedness();
        final List<String[]> enumValues = meta.getEnumStrValues();
        final List<String[]> setValues = meta.getSetStrValues();
        final List<Integer> geometryTypes = meta.getGeometryTypes();
        final List<Integer> simplePk = meta.getSimplePrimaryKeys();

        final int columnCount = types.length;
        final int[] collationPerColumn = resolveColumnCollations(types, meta);

        final TableEditor table = Table.editor().tableId(tableId);

        int numericIndex = 0;
        int enumIndex = 0;
        int setIndex = 0;
        int geometryIndex = 0;
        for (int i = 0; i < columnCount; i++) {
            final int code = types[i] & 0xFF;
            final ColumnEditor column = Column.editor()
                    .name(names.get(i))
                    .position(i + 1)
                    .optional(nullability != null && nullability.get(i));

            final boolean isNumeric = isNumericType(code);
            final boolean unsigned = isNumeric && signedness != null && signedness.get(numericIndex);
            if (isNumeric) {
                numericIndex++;
            }

            final int collation = collationPerColumn[i];
            final boolean binary = collation == BINARY_COLLATION_ID;

            switch (code) {
                case TYPE_TINY -> intType(column, "TINYINT", Types.SMALLINT, unsigned);
                case TYPE_SHORT -> intType(column, "SMALLINT", Types.SMALLINT, unsigned);
                case TYPE_INT24 -> intType(column, "MEDIUMINT", Types.INTEGER, unsigned);
                case TYPE_LONG -> intType(column, "INT", Types.INTEGER, unsigned);
                case TYPE_LONGLONG -> intType(column, "BIGINT", Types.BIGINT, unsigned);
                case TYPE_FLOAT -> intType(column, "FLOAT", Types.FLOAT, unsigned);
                case TYPE_DOUBLE -> intType(column, "DOUBLE", Types.DOUBLE, unsigned);
                case TYPE_YEAR -> column.type("YEAR").jdbcType(Types.INTEGER);
                case TYPE_DECIMAL, TYPE_NEWDECIMAL -> {
                    column.type(unsigned ? "DECIMAL UNSIGNED" : "DECIMAL").jdbcType(Types.DECIMAL);
                    column.length(columnMeta[i] & 0xFF);
                    column.scale((columnMeta[i] >> 8) & 0xFF);
                }
                // BIT metadata is encoded as (bytes << 8) | remaining_bits, so BIT(8) arrives as 0x0100.
                case TYPE_BIT -> column.type("BIT").jdbcType(Types.BIT).length(((columnMeta[i] >> 8) * 8) + (columnMeta[i] & 0xFF));
                case TYPE_DATE, TYPE_NEWDATE -> column.type("DATE").jdbcType(Types.DATE);
                case TYPE_TIME, TYPE_TIME_V2 -> column.type("TIME").jdbcType(Types.TIME).length(columnMeta[i]);
                case TYPE_DATETIME, TYPE_DATETIME_V2 -> column.type("DATETIME").jdbcType(Types.TIMESTAMP).length(columnMeta[i]);
                case TYPE_TIMESTAMP, TYPE_TIMESTAMP_V2 -> column.type("TIMESTAMP").jdbcType(Types.TIMESTAMP_WITH_TIMEZONE).length(columnMeta[i]);
                case TYPE_JSON -> column.type("JSON").jdbcType(Types.OTHER);
                case TYPE_GEOMETRY -> column.type(geometryTypeName(geometryTypes, geometryIndex++)).jdbcType(Types.OTHER);
                case TYPE_VECTOR -> column.type("VECTOR").jdbcType(Types.OTHER);
                case TYPE_VARCHAR, TYPE_VAR_STRING -> charType(column, binary ? "VARBINARY" : "VARCHAR",
                        binary ? Types.VARBINARY : Types.VARCHAR, byteToCharLength(columnMeta[i], collation), binary ? null : collation);
                case TYPE_TINY_BLOB, TYPE_BLOB, TYPE_MEDIUM_BLOB, TYPE_LONG_BLOB -> blobType(column, code, columnMeta[i], binary, collation);
                case TYPE_STRING -> stringType(column, columnMeta[i], binary, collation,
                        enumValues, setValues, enumIndex, setIndex);
                case TYPE_ENUM -> enumOrSet(column, "ENUM", enumValues, enumIndex, collation);
                case TYPE_SET -> enumOrSet(column, "SET", setValues, setIndex, collation);
                default -> column.type("UNKNOWN").jdbcType(Types.OTHER);
            }

            // STRING code may resolve to ENUM/SET internally; advance those indices there.
            if (code == TYPE_STRING) {
                final int realType = unpackStringRealType(columnMeta[i]);
                if (realType == TYPE_ENUM) {
                    enumIndex++;
                }
                else if (realType == TYPE_SET) {
                    setIndex++;
                }
            }
            else if (code == TYPE_ENUM) {
                enumIndex++;
            }
            else if (code == TYPE_SET) {
                setIndex++;
            }

            table.addColumn(column.create());
        }

        if (simplePk != null && !simplePk.isEmpty()) {
            final List<String> pkNames = new ArrayList<>(simplePk.size());
            for (Integer idx : simplePk) {
                pkNames.add(names.get(idx));
            }
            table.setPrimaryKeyNames(pkNames);
        }

        return table.create();
    }

    private void intType(ColumnEditor column, String baseName, int jdbcType, boolean unsigned) {
        column.type(unsigned ? baseName + " UNSIGNED" : baseName).jdbcType(jdbcType);
    }

    private void charType(ColumnEditor column, String typeName, int jdbcType, int length, Integer collation) {
        column.type(typeName).jdbcType(jdbcType).length(length);
        if (collation != null) {
            column.charsetName(collationName(collation));
        }
    }

    private void blobType(ColumnEditor column, int code, int meta, boolean binary, int collation) {
        // BLOB family: meta = number of length bytes (1=TINY, 2=normal, 3=MEDIUM, 4=LONG).
        final String prefix = switch (code) {
            case TYPE_TINY_BLOB -> "TINY";
            case TYPE_MEDIUM_BLOB -> "MEDIUM";
            case TYPE_LONG_BLOB -> "LONG";
            default -> "";
        };
        if (binary) {
            column.type(prefix + "BLOB").jdbcType(Types.BLOB);
        }
        else {
            column.type(prefix + "TEXT").jdbcType(Types.VARCHAR).charsetName(collationName(collation));
        }
    }

    private void stringType(ColumnEditor column, int meta, boolean binary, int collation,
                            List<String[]> enumValues, List<String[]> setValues, int enumIdx, int setIdx) {
        final int realType = unpackStringRealType(meta);
        if (realType == TYPE_ENUM) {
            enumOrSet(column, "ENUM", enumValues, enumIdx, collation);
            return;
        }
        if (realType == TYPE_SET) {
            enumOrSet(column, "SET", setValues, setIdx, collation);
            return;
        }
        // Plain CHAR / BINARY.
        final int byteLength = unpackStringLength(meta);
        if (binary) {
            column.type("BINARY").jdbcType(Types.BINARY).length(byteLength);
        }
        else {
            column.type("CHAR").jdbcType(Types.CHAR).length(byteToCharLength(byteLength, collation)).charsetName(collationName(collation));
        }
    }

    private void enumOrSet(ColumnEditor column, String typeName, List<String[]> valueLists, int index, int collation) {
        column.jdbcType(Types.CHAR);
        if (valueLists != null && index < valueLists.size()) {
            final String[] values = valueLists.get(index);
            final StringBuilder expr = new StringBuilder(typeName).append('(');
            for (int i = 0; i < values.length; i++) {
                if (i > 0) {
                    expr.append(',');
                }
                expr.append('\'').append(values[i]).append('\'');
            }
            expr.append(')');
            column.type(typeName, expr.toString());
            column.enumValues(List.of(values));
            // Mirror the DDL parser convention (see ColumnDefinitionParserListener): ENUM columns get
            // length 1 and SET columns "number of options + number of commas".
            column.length("ENUM".equals(typeName) ? 1 : Math.max(0, values.length * 2 - 1));
        }
        else {
            column.type(typeName);
        }
        if (collation != BINARY_COLLATION_ID) {
            column.charsetName(collationName(collation));
        }
    }

    /**
     * Resolve the concrete spatial type name from the {@code GEOMETRY_TYPE} metadata field, which carries
     * the real type of every geometry column ({@code Field::geometry_type} codes).
     */
    private static String geometryTypeName(List<Integer> geometryTypes, int geometryIndex) {
        if (geometryTypes == null || geometryIndex >= geometryTypes.size()) {
            return "GEOMETRY";
        }
        return switch (geometryTypes.get(geometryIndex)) {
            case 1 -> "POINT";
            case 2 -> "LINESTRING";
            case 3 -> "POLYGON";
            case 4 -> "MULTIPOINT";
            case 5 -> "MULTILINESTRING";
            case 6 -> "MULTIPOLYGON";
            case 7 -> "GEOMETRYCOLLECTION";
            default -> "GEOMETRY";
        };
    }

    /**
     * Unpack the real type packed into a {@code MYSQL_TYPE_STRING} metadata value (CHAR longer than
     * 255 bytes, ENUM, or SET are all transported as STRING). Canonical MySQL algorithm.
     */
    private static int unpackStringRealType(int meta) {
        if (meta < 256) {
            return TYPE_STRING;
        }
        final int byte0 = meta >> 8;
        if ((byte0 & 0x30) != 0x30) {
            return byte0 | 0x30;
        }
        return byte0;
    }

    private static int unpackStringLength(int meta) {
        if (meta < 256) {
            return meta;
        }
        final int byte0 = meta >> 8;
        final int byte1 = meta & 0xFF;
        if ((byte0 & 0x30) != 0x30) {
            return byte1 | (((byte0 & 0x30) ^ 0x30) << 4);
        }
        return byte1;
    }

    private static boolean isNumericType(int code) {
        return switch (code) {
            case TYPE_TINY, TYPE_SHORT, TYPE_INT24, TYPE_LONG, TYPE_LONGLONG,
                    TYPE_FLOAT, TYPE_DOUBLE, TYPE_YEAR, TYPE_DECIMAL, TYPE_NEWDECIMAL ->
                true;
            default -> false;
        };
    }

    private static boolean isCharacterType(int code) {
        return switch (code) {
            case TYPE_VARCHAR, TYPE_VAR_STRING, TYPE_STRING, TYPE_ENUM, TYPE_SET,
                    TYPE_TINY_BLOB, TYPE_BLOB, TYPE_MEDIUM_BLOB, TYPE_LONG_BLOB ->
                true;
            default -> false;
        };
    }

    /**
     * Resolve the collation id per column. MySQL encodes charset either as an explicit per-character-column
     * list ({@code COLUMN_CHARSET}) or as a default plus sparse overrides ({@code DEFAULT_CHARSET}), both
     * indexed over character-type columns only.
     */
    private int[] resolveColumnCollations(byte[] types, TableMapEventMetadata meta) {
        final int[] result = new int[types.length];
        final List<Integer> explicit = meta.getColumnCharsets();
        final TableMapEventMetadata.DefaultCharset defaultCharset = meta.getDefaultCharset();
        int charIndex = 0;
        for (int i = 0; i < types.length; i++) {
            if (!isCharacterType(types[i] & 0xFF)) {
                result[i] = -1;
                continue;
            }
            if (explicit != null && charIndex < explicit.size()) {
                result[i] = explicit.get(charIndex);
            }
            else if (defaultCharset != null) {
                final Map<Integer, Integer> overrides = defaultCharset.getCharsetCollations();
                result[i] = overrides != null && overrides.containsKey(charIndex)
                        ? overrides.get(charIndex)
                        : defaultCharset.getDefaultCharsetCollation();
            }
            else {
                result[i] = -1;
            }
            charIndex++;
        }
        return result;
    }

    /** Convert a byte length to a character length using the collation's maximum bytes-per-character. */
    private int byteToCharLength(int byteLength, int collation) {
        final int maxBytes = maxBytesPerChar(collation);
        return maxBytes <= 1 ? byteLength : byteLength / maxBytes;
    }

    /**
     * Maximum bytes-per-character of the character set behind the given collation, used to convert the
     * byte length carried by the binlog metadata into the character length of the column.
     */
    private int maxBytesPerChar(int collation) {
        final String charsetName = collationName(collation);
        if (charsetName == null) {
            return 1;
        }
        return switch (charsetName) {
            case "utf8mb4", "utf16", "utf16le", "utf32", "gb18030" -> 4;
            case "utf8", "utf8mb3", "eucjpms", "ujis" -> 3;
            case "ucs2", "gbk", "sjis", "big5", "euckr", "cp932", "gb2312" -> 2;
            default -> 1;
        };
    }

    /**
     * Resolve the character set name for a collation id, preferring the connector's charset registry and
     * falling back to a minimal static catalog when no registry is available (for example in unit tests).
     */
    private String collationName(int collation) {
        final BinlogCharsetRegistry registry = charsetRegistry();
        if (registry != null) {
            final String name = registry.getCharsetNameForCollationIndex(collation);
            if (name != null) {
                return name;
            }
        }
        return switch (collation) {
            case 45, 46, 224, 255 -> "utf8mb4";
            case 33, 83, 192 -> "utf8mb3";
            case 63 -> "binary";
            case 8 -> "latin1";
            default -> null;
        };
    }

    private BinlogCharsetRegistry charsetRegistry() {
        if (!charsetRegistryResolved) {
            charsetRegistryResolved = true;
            charsetRegistry = charsetRegistrySupplier != null ? charsetRegistrySupplier.get() : null;
        }
        return charsetRegistry;
    }
}
