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
 * registrations (e.g. TINYINT &rarr; {@link Types#TINYINT}, SMALLINT &rarr; {@link Types#SMALLINT},
 * DATETIME &rarr; {@link Types#TIMESTAMP}, TIMESTAMP &rarr; {@link Types#TIMESTAMP_WITH_TIMEZONE},
 * ENUM/SET &rarr; {@link Types#CHAR}, JSON/GEOMETRY &rarr; {@link Types#OTHER}).
 * <p>
 * Attributes that the binlog metadata does not carry (column defaults, generated-column flag,
 * comments) are intentionally not populated; those are documented limitations of this mode.
 * <p>
 * The authoritative list of what the server writes is {@code Table_map_log_event::init_metadata_fields()}
 * in MySQL {@code sql/log_event.cc}: {@code MINIMAL} metadata consists of SIGNEDNESS,
 * DEFAULT_CHARSET/COLUMN_CHARSET (string columns) and GEOMETRY_TYPE, and {@code FULL} adds COLUMN_NAME,
 * ENUM_AND_SET_DEFAULT_CHARSET/ENUM_AND_SET_COLUMN_CHARSET, SET_STR_VALUE, ENUM_STR_VALUE, either
 * SIMPLE_PRIMARY_KEY or PRIMARY_KEY_WITH_PREFIX, COLUMN_VISIBILITY and, on MySQL 9, VECTOR_DIMENSIONALITY.
 * No field carries the integer display width, column DEFAULT values, the FLOAT(M,D) precision, or column
 * comments, which is the source-level basis for the documented differences from the schema history mode.
 * COLUMN_VISIBILITY is intentionally not consumed: invisible columns are present in every row event and
 * the DDL-based path keeps them as ordinary columns as well.
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
        final TableMapEventMetadata eventMetadata = data.getEventMetadata();
        if (eventMetadata == null || eventMetadata.getColumnNames() == null || eventMetadata.getColumnNames().isEmpty()) {
            throw new IllegalStateException(
                    "TABLE_MAP for '" + tableId + "' has no column metadata; binlog_row_metadata=FULL is required for the binlog-metadata schema mode");
        }

        final byte[] columnTypes = data.getColumnTypes();
        final int[] columnMetadata = data.getColumnMetadata();
        final BitSet nullability = data.getColumnNullability();
        final List<String> names = eventMetadata.getColumnNames();
        final BitSet signedness = eventMetadata.getSignedness();
        final List<String[]> enumValues = eventMetadata.getEnumStrValues();
        final List<String[]> setValues = eventMetadata.getSetStrValues();
        final List<Integer> geometryTypes = eventMetadata.getGeometryTypes();
        final List<Integer> vectorDimensions = eventMetadata.getVectorDimensionality();
        final List<Integer> simplePk = eventMetadata.getSimplePrimaryKeys();

        final int columnCount = columnTypes.length;
        final int[] collationPerColumn = resolveColumnCollations(columnTypes, columnMetadata, eventMetadata);

        final TableEditor table = Table.editor().tableId(tableId);

        int enumColumnIndex = 0;
        int setColumnIndex = 0;
        int geometryColumnIndex = 0;
        int vectorColumnIndex = 0;
        for (int i = 0; i < columnCount; i++) {
            final int columnTypeCode = columnTypes[i] & 0xFF;
            final ColumnEditor column = Column.editor()
                    .name(names.get(i))
                    .position(i + 1)
                    .optional(nullability != null && nullability.get(i));

            // The deserializer expands the wire's numeric-only signedness bitmap to all column positions.
            final boolean unsigned = isNumericType(columnTypeCode) && signedness != null && signedness.get(i);

            final int collation = collationPerColumn[i];
            final boolean binary = collation == BINARY_COLLATION_ID;

            switch (columnTypeCode) {
                case TYPE_TINY -> numericType(column, "TINYINT", Types.TINYINT, unsigned);
                case TYPE_SHORT -> numericType(column, "SMALLINT", Types.SMALLINT, unsigned);
                case TYPE_INT24 -> numericType(column, "MEDIUMINT", Types.INTEGER, unsigned);
                case TYPE_LONG -> numericType(column, "INT", Types.INTEGER, unsigned);
                case TYPE_LONGLONG -> numericType(column, "BIGINT", Types.BIGINT, unsigned);
                case TYPE_FLOAT -> numericType(column, "FLOAT", Types.FLOAT, unsigned);
                case TYPE_DOUBLE -> numericType(column, "DOUBLE", Types.DOUBLE, unsigned);
                case TYPE_YEAR -> column.type("YEAR").jdbcType(Types.INTEGER);
                // DECIMAL metadata is encoded as (scale << 8) | precision.
                case TYPE_DECIMAL, TYPE_NEWDECIMAL -> {
                    column.type(unsigned ? "DECIMAL UNSIGNED" : "DECIMAL").jdbcType(Types.DECIMAL);
                    column.length(columnMetadata[i] & 0xFF);
                    column.scale((columnMetadata[i] >> 8) & 0xFF);
                }
                // BIT metadata is encoded as (bytes << 8) | remaining_bits, so BIT(8) arrives as 0x0100.
                case TYPE_BIT -> column.type("BIT").jdbcType(Types.BIT).length(((columnMetadata[i] >> 8) * 8) + (columnMetadata[i] & 0xFF));
                case TYPE_DATE, TYPE_NEWDATE -> column.type("DATE").jdbcType(Types.DATE);
                case TYPE_TIME, TYPE_TIME_V2 -> column.type("TIME").jdbcType(Types.TIME).length(columnMetadata[i]);
                case TYPE_DATETIME, TYPE_DATETIME_V2 -> column.type("DATETIME").jdbcType(Types.TIMESTAMP).length(columnMetadata[i]);
                case TYPE_TIMESTAMP, TYPE_TIMESTAMP_V2 -> column.type("TIMESTAMP").jdbcType(Types.TIMESTAMP_WITH_TIMEZONE).length(columnMetadata[i]);
                case TYPE_JSON -> column.type("JSON").jdbcType(Types.OTHER);
                case TYPE_GEOMETRY -> column.type(geometryTypeName(geometryTypes, geometryColumnIndex++)).jdbcType(Types.OTHER);
                case TYPE_VECTOR -> vectorType(column, vectorDimensions, vectorColumnIndex++);
                case TYPE_VARCHAR, TYPE_VAR_STRING -> charType(column, binary ? "VARBINARY" : "VARCHAR",
                        binary ? Types.VARBINARY : Types.VARCHAR, byteToCharLength(columnMetadata[i], collation), binary ? null : collation);
                case TYPE_TINY_BLOB, TYPE_BLOB, TYPE_MEDIUM_BLOB, TYPE_LONG_BLOB -> blobType(column, columnTypeCode, columnMetadata[i], binary, collation);
                case TYPE_STRING -> stringType(column, columnMetadata[i], binary, collation,
                        enumValues, setValues, enumColumnIndex, setColumnIndex);
                case TYPE_ENUM -> enumOrSet(column, "ENUM", enumValues, enumColumnIndex, collation);
                case TYPE_SET -> enumOrSet(column, "SET", setValues, setColumnIndex, collation);
                default -> column.type("UNKNOWN").jdbcType(Types.OTHER);
            }

            // STRING columns may carry ENUM/SET values; advance the corresponding value-list index.
            if (columnTypeCode == TYPE_STRING) {
                final int realType = unpackStringRealType(columnMetadata[i]);
                if (realType == TYPE_ENUM) {
                    enumColumnIndex++;
                }
                else if (realType == TYPE_SET) {
                    setColumnIndex++;
                }
            }
            else if (columnTypeCode == TYPE_ENUM) {
                enumColumnIndex++;
            }
            else if (columnTypeCode == TYPE_SET) {
                setColumnIndex++;
            }

            table.addColumn(column.create());
        }

        if (simplePk != null && !simplePk.isEmpty()) {
            final List<String> pkNames = new ArrayList<>(simplePk.size());
            for (Integer columnIndex : simplePk) {
                pkNames.add(names.get(columnIndex));
            }
            table.setPrimaryKeyNames(pkNames);
        }
        else {
            // When any key part uses a prefix, e.g. PRIMARY KEY (`code`(5)), the server writes
            // PRIMARY_KEY_WITH_PREFIX instead of SIMPLE_PRIMARY_KEY (either/or; see
            // Table_map_log_event::init_primary_key_field() in MySQL sql/log_event.cc). The map keeps
            // the key part order and the prefix length is irrelevant here: the relational model tracks
            // key column names only, exactly like the DDL parser does for a prefixed key.
            final Map<Integer, Integer> prefixPk = eventMetadata.getPrimaryKeysWithPrefix();
            if (prefixPk != null && !prefixPk.isEmpty()) {
                final List<String> pkNames = new ArrayList<>(prefixPk.size());
                for (Integer columnIndex : prefixPk.keySet()) {
                    pkNames.add(names.get(columnIndex));
                }
                table.setPrimaryKeyNames(pkNames);
            }
        }

        return table.create();
    }

    private void numericType(ColumnEditor column, String baseName, int jdbcType, boolean unsigned) {
        column.type(unsigned ? baseName + " UNSIGNED" : baseName).jdbcType(jdbcType);
    }

    private void charType(ColumnEditor column, String typeName, int jdbcType, int length, Integer collation) {
        column.type(typeName).jdbcType(jdbcType).length(length);
        if (collation != null) {
            column.charsetName(collationName(collation));
        }
    }

    private void blobType(ColumnEditor column, int columnTypeCode, int columnMetadata, boolean binary, int collation) {
        // TABLE_MAP uses BLOB for every size, with metadata carrying the number of length bytes.
        final int lengthBytes = switch (columnTypeCode) {
            case TYPE_TINY_BLOB -> 1;
            case TYPE_MEDIUM_BLOB -> 3;
            case TYPE_LONG_BLOB -> 4;
            default -> columnMetadata;
        };
        final String prefix = switch (lengthBytes) {
            case 1 -> "TINY";
            case 3 -> "MEDIUM";
            case 4 -> "LONG";
            default -> "";
        };
        if (binary) {
            column.type(prefix + "BLOB").jdbcType(Types.BLOB);
        }
        else {
            column.type(prefix + "TEXT").jdbcType(Types.VARCHAR).charsetName(collationName(collation));
        }
    }

    private void stringType(ColumnEditor column, int columnMetadata, boolean binary, int collation,
                            List<String[]> enumValues, List<String[]> setValues, int enumColumnIndex, int setColumnIndex) {
        final int realType = unpackStringRealType(columnMetadata);
        if (realType == TYPE_ENUM) {
            enumOrSet(column, "ENUM", enumValues, enumColumnIndex, collation);
            return;
        }
        if (realType == TYPE_SET) {
            enumOrSet(column, "SET", setValues, setColumnIndex, collation);
            return;
        }
        // Plain CHAR / BINARY.
        final int byteLength = unpackStringLength(columnMetadata);
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
     * The {@code VECTOR_DIMENSIONALITY} metadata field (MySQL 9) carries the dimensionality of every
     * vector column, which the DDL parser stores as the column length.
     */
    private static void vectorType(ColumnEditor column, List<Integer> vectorDimensions, int vectorColumnIndex) {
        column.type("VECTOR").jdbcType(Types.OTHER);
        if (vectorDimensions != null && vectorColumnIndex < vectorDimensions.size()) {
            column.length(vectorDimensions.get(vectorColumnIndex));
        }
    }

    /**
     * Resolve the concrete spatial type name from the {@code GEOMETRY_TYPE} metadata field, which carries
     * the real type of every geometry column ({@code Field::geometry_type} codes).
     */
    private static String geometryTypeName(List<Integer> geometryTypes, int geometryColumnIndex) {
        if (geometryTypes == null || geometryColumnIndex >= geometryTypes.size()) {
            return "GEOMETRY";
        }
        return switch (geometryTypes.get(geometryColumnIndex)) {
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
    private static int unpackStringRealType(int columnMetadata) {
        if (columnMetadata < 256) {
            return TYPE_STRING;
        }
        final int byte0 = columnMetadata >> 8;
        if ((byte0 & 0x30) != 0x30) {
            return byte0 | 0x30;
        }
        return byte0;
    }

    private static int unpackStringLength(int columnMetadata) {
        if (columnMetadata < 256) {
            return columnMetadata;
        }
        final int byte0 = columnMetadata >> 8;
        final int byte1 = columnMetadata & 0xFF;
        if ((byte0 & 0x30) != 0x30) {
            return byte1 | (((byte0 & 0x30) ^ 0x30) << 4);
        }
        return byte1;
    }

    private static boolean isNumericType(int columnTypeCode) {
        return switch (columnTypeCode) {
            case TYPE_TINY, TYPE_SHORT, TYPE_INT24, TYPE_LONG, TYPE_LONGLONG,
                    TYPE_FLOAT, TYPE_DOUBLE, TYPE_YEAR, TYPE_DECIMAL, TYPE_NEWDECIMAL ->
                true;
            default -> false;
        };
    }

    /**
     * Whether the column takes part in the DEFAULT_CHARSET/COLUMN_CHARSET numbering. Mirrors
     * {@code is_character_type()} in MySQL {@code sql/log_event.cc}: STRING, VAR_STRING, VARCHAR and the
     * BLOB family only. ENUM and SET columns are excluded even though they are transported as STRING,
     * because the server tests the field's real type; they are numbered separately and take their
     * charset from the ENUM_AND_SET_* fields instead.
     */
    private static boolean isCharacterType(int columnTypeCode) {
        return switch (columnTypeCode) {
            case TYPE_VARCHAR, TYPE_VAR_STRING, TYPE_STRING,
                    TYPE_TINY_BLOB, TYPE_BLOB, TYPE_MEDIUM_BLOB, TYPE_LONG_BLOB ->
                true;
            default -> false;
        };
    }

    private static boolean isEnumOrSetColumn(int columnTypeCode, int columnMetadata) {
        if (columnTypeCode == TYPE_ENUM || columnTypeCode == TYPE_SET) {
            return true;
        }
        if (columnTypeCode == TYPE_STRING) {
            final int realType = unpackStringRealType(columnMetadata);
            return realType == TYPE_ENUM || realType == TYPE_SET;
        }
        return false;
    }

    /**
     * Resolve the collation id per column. MySQL encodes charset either as an explicit per-column list
     * ({@code COLUMN_CHARSET}) or as a default plus sparse overrides ({@code DEFAULT_CHARSET}). Both are
     * indexed over character-type columns only, and ENUM/SET columns have their own pair of fields
     * ({@code ENUM_AND_SET_COLUMN_CHARSET}/{@code ENUM_AND_SET_DEFAULT_CHARSET}) with an independent
     * numbering (see {@code Table_map_log_event::init_charset_field()} in MySQL {@code sql/log_event.cc}).
     */
    private int[] resolveColumnCollations(byte[] columnTypes, int[] columnMetadata, TableMapEventMetadata eventMetadata) {
        final int[] result = new int[columnTypes.length];
        int characterColumnIndex = 0;
        int enumSetColumnIndex = 0;
        for (int i = 0; i < columnTypes.length; i++) {
            final int columnTypeCode = columnTypes[i] & 0xFF;
            if (isEnumOrSetColumn(columnTypeCode, columnMetadata[i])) {
                result[i] = resolveCollation(eventMetadata.getEnumAndSetColumnCharsets(), eventMetadata.getEnumAndSetDefaultCharset(), enumSetColumnIndex++);
            }
            else if (isCharacterType(columnTypeCode)) {
                result[i] = resolveCollation(eventMetadata.getColumnCharsets(), eventMetadata.getDefaultCharset(), characterColumnIndex++);
            }
            else {
                result[i] = -1;
            }
        }
        return result;
    }

    private static int resolveCollation(List<Integer> explicit, TableMapEventMetadata.DefaultCharset defaultCharset, int index) {
        if (explicit != null && index < explicit.size()) {
            return explicit.get(index);
        }
        if (defaultCharset != null) {
            final Map<Integer, Integer> overrides = defaultCharset.getCharsetCollations();
            return overrides != null && overrides.containsKey(index)
                    ? overrides.get(index)
                    : defaultCharset.getDefaultCharsetCollation();
        }
        return -1;
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
