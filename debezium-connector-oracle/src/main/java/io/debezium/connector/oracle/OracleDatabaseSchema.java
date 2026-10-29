/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.connector.oracle.StreamingAdapter.TableNameCaseSensitivity;
import io.debezium.connector.oracle.antlr.OracleDdlParser;
import io.debezium.connector.oracle.relational.OracleColumn;
import io.debezium.relational.Attribute;
import io.debezium.relational.Column;
import io.debezium.relational.CustomConverterRegistry;
import io.debezium.relational.DefaultValueConverter;
import io.debezium.relational.HistorizedRelationalDatabaseSchema;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchemaBuilder;
import io.debezium.relational.Tables;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.spi.topic.TopicNamingStrategy;
import io.debezium.util.LRUCacheMap;

import oracle.jdbc.OracleTypes;
import oracle.sql.CharacterSet;

/**
 * The schema of an Oracle database.
 *
 * @author Gunnar Morling
 */
public class OracleDatabaseSchema extends HistorizedRelationalDatabaseSchema {

    private static final Logger LOGGER = LoggerFactory.getLogger(OracleDatabaseSchema.class);

    public static final String ATTRIBUTE_OBJECT_ID = "OBJECT_ID";
    public static final String ATTRIBUTE_DATA_OBJECT_ID = "DATA_OBJECT_ID";
    private static final TableId NO_SUCH_TABLE = new TableId(null, null, "__NULL");
    private static final int MAX_STANDARD_STRING_BYTES = 4000;
    private static final int MAX_EXTENDED_STRING_BYTES = 32767;

    /**
     * Oracle names multibyte character sets with the number of bits after the region, for example {@code ZHS16GBK}.
     */
    private static final Pattern MULTIBYTE_CHARACTER_SET_NAME = Pattern.compile("^[A-Z]+(16|32)[A-Z].*");

    private final OracleDdlParser ddlParser;
    private final Map<TableId, List<Column>> lobColumnsByTableId = new ConcurrentHashMap<>();
    private final Set<TableId> tablesWarnedForMissingOctetLength = ConcurrentHashMap.newKeySet();
    private final Map<String, TableId> tableIdCache = new ConcurrentHashMap<>();
    private final OracleValueConverters valueConverters;
    private final LRUCacheMap<Long, TableId> objectIdToTableId;
    private final boolean extendedStringsSupported;

    private LastObjectTableIdLookup lastObjectTableIdLookup;

    public OracleDatabaseSchema(OracleConnectorConfig connectorConfig, OracleValueConverters valueConverters,
                                DefaultValueConverter defaultValueConverter, SchemaNameAdjuster schemaNameAdjuster,
                                TopicNamingStrategy<TableId> topicNamingStrategy, TableNameCaseSensitivity tableNameCaseSensitivity,
                                boolean extendedStringsSupported, CustomConverterRegistry customConverterRegistry,
                                CdcSourceTaskContext<? extends CommonConnectorConfig> taskContext) {
        super(connectorConfig, topicNamingStrategy, connectorConfig.getTableFilters().dataCollectionFilter(),
                connectorConfig.getColumnFilter(),
                new TableSchemaBuilder(
                        valueConverters,
                        defaultValueConverter,
                        schemaNameAdjuster,
                        customConverterRegistry,
                        connectorConfig.getSourceInfoStructMaker().schema(),
                        connectorConfig.getFieldNamer(),
                        false,
                        connectorConfig.getEventConvertingFailureHandlingMode()),
                TableNameCaseSensitivity.INSENSITIVE.equals(tableNameCaseSensitivity),
                connectorConfig.getKeyMapper(), taskContext);

        this.valueConverters = valueConverters;
        this.ddlParser = new OracleDdlParser(
                true,
                false,
                connectorConfig.isSchemaCommentsHistoryEnabled(),
                valueConverters,
                connectorConfig.getTableFilters().dataCollectionFilter());

        this.objectIdToTableId = new LRUCacheMap<>(connectorConfig.getObjectIdToTableIdCacheSize());
        this.extendedStringsSupported = extendedStringsSupported;

        if (extendedStringsSupported && connectorConfig.isLobEnabled() && isMultibyteNonUnicode(valueConverters.getDatabaseCharacterSet())) {
            LOGGER.warn("The database character set {} is a multibyte character set that is not Unicode. "
                    + "For VARCHAR2 columns with CHAR length semantics that are created or altered while streaming, "
                    + "the connector estimates {} bytes per character from the character set name to detect extended strings.",
                    valueConverters.getDatabaseCharacterSet(), getMaxBytesPerChar(valueConverters.getDatabaseCharacterSet()));
        }
    }

    public Tables getTables() {
        return tables();
    }

    public OracleValueConverters getValueConverters() {
        return valueConverters;
    }

    @Override
    public OracleDdlParser getDdlParser() {
        return ddlParser;
    }

    @Override
    public void applySchemaChange(SchemaChangeEvent schemaChange) {
        LOGGER.debug("Applying schema change event {}", schemaChange);

        switch (schemaChange.getType()) {
            case CREATE:
            case ALTER:
                schemaChange.getTableChanges().forEach(x -> {
                    buildAndRegisterSchema(x.getTable());
                    tables().overwriteTable(x.getTable());
                });
                break;
            case DROP:
                schemaChange.getTableChanges().forEach(x -> removeSchema(x.getId()));
                break;
            default:
        }

        if (!storeOnlyCapturedTables() ||
                schemaChange.getTables().stream().map(Table::id).anyMatch(getTableFilter()::isIncluded)) {
            LOGGER.debug("Recorded DDL statements for database '{}': {}", schemaChange.getDatabase(), schemaChange.getDdl());
            record(schemaChange, schemaChange.getTableChanges());
        }
    }

    @Override
    protected void removeSchema(TableId id) {
        super.removeSchema(id);
        lobColumnsByTableId.remove(id);
        tableIdCache.remove(id.identifier());
    }

    @Override
    protected void buildAndRegisterSchema(Table table) {
        if (getTableFilter().isIncluded(table.id())) {
            super.buildAndRegisterSchema(table);

            // Cache LOB column mappings for performance
            buildAndRegisterTableLobColumns(table);

            // Cache Object ID to Table ID for performance
            buildAndRegisterTableObjectIdReferences(table);

            tableIdCache.putIfAbsent(table.id().identifier(), table.id());
        }
    }

    public TableId resolveTableId(String catalogName, String schemaName, String tableName) {
        // In practice, the tableIdCache should generally be primed with tables via buildAndRegister
        // or cleaned up by calls to removeSchema. But for performance reasons, we will cache it if
        // there isn't a table record registered, solely for optimal memory footprint in buffers.
        final TableId tableId = new TableId(catalogName, schemaName, tableName);
        return tableIdCache.computeIfAbsent(tableId.identifier(), k -> tableId);
    }

    /**
     * Get the {@link TableId} by {@code objectId}.
     *
     * @param objectId the object id find the table id about, cannot be {@code null}
     * @param dataObjectId the data object id, may be {@code null}
     * @return the table identifier or {@code null} if no entry is cached or in the schema with the object id
     */
    public TableId getTableIdByObjectId(Long objectId, Long dataObjectId) {
        Objects.requireNonNull(objectId, "The database table object id is null and is not allowed");

        // Fast path: check if the last lookup matches
        if (lastObjectTableIdLookup != null &&
                objectId.equals(lastObjectTableIdLookup.objectId()) &&
                Objects.equals(lastObjectTableIdLookup.dataObjectId(), dataObjectId)) {
            return (lastObjectTableIdLookup.tableId() == NO_SUCH_TABLE) ? null : lastObjectTableIdLookup.tableId();
        }

        // Internally we cache this using a bounded cache for performance reasons, particularly when a
        // transaction may refer to the same table for consecutive DML events. This avoids the need to
        // iterate the list of tables on each DML event observed.
        TableId cachedTableId = objectIdToTableId.get(objectId);
        if (cachedTableId == null) {
            cachedTableId = tableObjectIdToTableId(objectId, dataObjectId);
            objectIdToTableId.put(objectId, cachedTableId);
        }

        // Cache the lookup to avoid a map hit should the next inquiry be for the same objectId/dataObjectId.
        lastObjectTableIdLookup = new LastObjectTableIdLookup(objectId, dataObjectId, cachedTableId);

        // There is not any table for this object ID, so we have to convert back the placeholder
        // and return null.
        return (cachedTableId == NO_SUCH_TABLE) ? null : cachedTableId;
    }

    /**
     * Get a list of large object (LOB) columns for the specified relational table identifier.
     *
     * @param id the relational table identifier
     * @return a list of LOB columns, may be empty if the table has no LOB columns
     */
    public List<Column> getLobColumnsForTable(TableId id) {
        return lobColumnsByTableId.getOrDefault(id, Collections.emptyList());
    }

    /**
     * Returns whether the specified value is the unavailable value placeholder for an LOB column.
     */
    public boolean isColumnUnavailableValuePlaceholder(Column column, Object value) {
        if (isClobColumn(column) || isXmlColumn(column) || isExtendedStringColumn(column) || isJsonColumn(column)) {
            return valueConverters.getUnavailableValuePlaceholderString().equals(value);
        }
        else if (isBlobColumn(column)) {
            return ByteBuffer.wrap(valueConverters.getUnavailableValuePlaceholderBinary()).equals(value);
        }
        return false;
    }

    /**
     * Return whether the column is replaced by the {@code unavailable.value.placeholder}.
     */
    public static boolean isNullReplacedByUnavailableValue(Column column) {
        return isLobColumn(column) || isXmlColumn(column) || isExtendedStringColumn(column) || isJsonColumn(column);
    }

    /**
     * Get the maximum length in bytes of a {@code VARCHAR2} or {@code NVARCHAR2} column, which depends on the
     * database's {@code MAX_STRING_SIZE} setting.
     *
     * @param extendedStringsSupported whether {@code MAX_STRING_SIZE} is {@code EXTENDED}
     * @return the maximum length in bytes
     */
    public static int getMaxStringBytes(boolean extendedStringsSupported) {
        return extendedStringsSupported ? MAX_EXTENDED_STRING_BYTES : MAX_STANDARD_STRING_BYTES;
    }

    /**
     * Get the maximum number of bytes a single character uses in the given character set.
     * <p>
     * For multibyte character sets that are not Unicode, the width is derived from the number of bits in the
     * Oracle character set name, for example 2 for {@code ZHS16GBK} and 4 for {@code ZHS32GB18030}. The
     * {@code JA16EUC} sets use up to 3 bytes. Over-counting only treats a column as an extended string
     * column, while under-counting would lose its changes.
     *
     * @param characterSet the character set, may be {@code null}
     * @return the maximum bytes per character, at least 1
     */
    public static int getMaxBytesPerChar(CharacterSet characterSet) {
        if (characterSet == null) {
            return 1;
        }
        return switch (characterSet.getOracleId()) {
            case CharacterSet.AL32UTF8_CHARSET -> 4;
            case CharacterSet.UTF8_CHARSET -> 3;
            case CharacterSet.AL16UTF16_CHARSET, CharacterSet.AL16UTF16LE_CHARSET -> 2;
            default -> characterSet.isUnicode() ? 4 : getMaxBytesPerCharFromName(characterSet.toString());
        };
    }

    /**
     * Return whether the provided relational column model is a LOB data type.
     */
    private static boolean isLobColumn(Column column) {
        return isClobColumn(column) || isBlobColumn(column) || isJsonColumn(column);
    }

    /**
     * Return whether the provided relational column model is a XML data type.
     */
    private static boolean isXmlColumn(Column column) {
        return column.jdbcType() == OracleTypes.SQLXML;
    }

    /**
     * Returns whether the provided relational column model is a CLOB or NCLOB data type.
     */
    private static boolean isClobColumn(Column column) {
        return column.jdbcType() == OracleTypes.CLOB || column.jdbcType() == OracleTypes.NCLOB;
    }

    /**
     * Returns whether the provided relational column model is a CLOB data type.
     */
    private static boolean isBlobColumn(Column column) {
        return column.jdbcType() == OracleTypes.BLOB;
    }

    /**
     * Returns whether the provided relational column model is an extended string data type, a
     * {@code VARCHAR2} or {@code NVARCHAR2} column whose maximum length is more than 4000 bytes.
     */
    private static boolean isExtendedStringColumn(Column column) {
        if (!isStringColumn(column)) {
            return false;
        }
        final int octetLength = getOctetLength(column);
        if (octetLength != Column.UNSET_INT_VALUE) {
            return octetLength > MAX_STANDARD_STRING_BYTES;
        }
        // Without a byte length only the declared length is known, which is in characters for
        // CHAR semantics, so this misses some extended string columns.
        if (column.jdbcType() == OracleTypes.VARCHAR) {
            return column.length() > MAX_STANDARD_STRING_BYTES;
        }
        return column.length() > MAX_STANDARD_STRING_BYTES / 2;
    }

    private static boolean isStringColumn(Column column) {
        return column.jdbcType() == OracleTypes.VARCHAR || column.jdbcType() == OracleTypes.NVARCHAR;
    }

    private static int getOctetLength(Column column) {
        return column instanceof OracleColumn oracleColumn ? oracleColumn.octetLength() : Column.UNSET_INT_VALUE;
    }

    private static int getMaxBytesPerCharFromName(String characterSetName) {
        if (characterSetName == null) {
            return 1;
        }
        final Matcher matcher = MULTIBYTE_CHARACTER_SET_NAME.matcher(characterSetName);
        if (!matcher.matches()) {
            return 1;
        }
        if (characterSetName.startsWith("JA16EUC")) {
            return 3;
        }
        return "16".equals(matcher.group(1)) ? 2 : 4;
    }

    private static boolean isMultibyteNonUnicode(CharacterSet characterSet) {
        return characterSet != null && !characterSet.isUnicode() && getMaxBytesPerChar(characterSet) > 1;
    }

    private static boolean isJsonColumn(Column column) {
        return "JSON".equals(column.typeName());
    }

    private void buildAndRegisterTableObjectIdReferences(Table table) {
        final Attribute attribute = table.attributeWithName(ATTRIBUTE_OBJECT_ID);
        if (attribute != null) {
            objectIdToTableId.put(attribute.asLong(), table.id());
            // The last lookup may have recorded this object id as unknown before the table was registered,
            // so it must not keep answering for it.
            lastObjectTableIdLookup = null;
        }
    }

    private void buildAndRegisterTableLobColumns(Table table) {
        final List<Column> lobColumns = new ArrayList<>();
        for (Column column : table.columns()) {
            switch (column.jdbcType()) {
                case OracleTypes.CLOB:
                case OracleTypes.NCLOB:
                case OracleTypes.BLOB:
                case OracleTypes.SQLXML:
                    lobColumns.add(column);
                    break;
                case OracleTypes.VARCHAR:
                case OracleTypes.NVARCHAR:
                    if (extendedStringsSupported) {
                        warnIfOctetLengthIsMissing(table, column);
                        if (isExtendedStringColumn(column)) {
                            lobColumns.add(column);
                        }
                    }
                    break;
                default: {
                    if (isJsonColumn(column)) {
                        lobColumns.add(column);
                    }
                }
            }
        }
        if (!lobColumns.isEmpty()) {
            lobColumnsByTableId.put(table.id(), lobColumns);
        }
        else {
            lobColumnsByTableId.remove(table.id());
        }
    }

    private void warnIfOctetLengthIsMissing(Table table, Column column) {
        // Other types reported as VARCHAR, such as ROWID, never have a byte length
        final boolean hasOctetLength = "VARCHAR2".equals(column.typeName()) || "NVARCHAR2".equals(column.typeName());
        if (hasOctetLength && column.length() != Column.UNSET_INT_VALUE && getOctetLength(column) == Column.UNSET_INT_VALUE
                && tablesWarnedForMissingOctetLength.add(table.id())) {
            LOGGER.warn("Table '{}' has no byte length for column '{}', so its declared length is used instead "
                    + "and it may not be detected as an extended string column.",
                    table.id(), column.name());
        }
    }

    private TableId tableObjectIdToTableId(Long tableObjectId, Long dataObjectId) {
        for (TableId tableId : tableIds()) {
            final Table table = tableFor(tableId);
            final Attribute attribute = table.attributeWithName(ATTRIBUTE_OBJECT_ID);
            if (attribute != null && attribute.asLong().equals(tableObjectId)) {
                if (dataObjectId != null) {
                    final Attribute dataAttribute = table.attributeWithName(ATTRIBUTE_DATA_OBJECT_ID);
                    if (dataAttribute == null || !dataAttribute.asLong().equals(dataObjectId)) {
                        // Did not match, continue
                        continue;
                    }
                }
                LOGGER.debug("Table lookup for object {} resolved to '{}'", tableObjectId, table.id());
                return table.id();
            }
        }
        // A non-null placeholder must be inserted for non-existing value to avoid the expensive
        // look-up across the table schemas in future calls, so inserting explicitly non-existing
        // placeholder here.
        LOGGER.debug("Table lookup for object id {} did not find a match.", tableObjectId);
        return NO_SUCH_TABLE;
    }

    /**
     * A simple record that represents the last lookup in the {@code objectIdToTableId} map.
     * This is to provide a more efficient way to resolve the identifier lookup when there
     * are repeated operations for the same table in the transaction logs.
     *
     * @param objectId object's identifier, should not be {@code null}
     * @param dataObjectId object's data identifier, should not be {@code null}
     * @param tableId the lookup table identifier
     */
    record LastObjectTableIdLookup(Long objectId, Long dataObjectId, TableId tableId) {
    }
}
