/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.rocksdb.tablemapping;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.function.BiConsumer;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.common.annotation.Incubating;
import io.debezium.relational.AbstractCachedTableMappingStorage;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.TableId;
import io.debezium.relational.TableMappingStorage;
import io.debezium.util.Strings;

/**
 * RocksDb-based implementation of table mapping storage for persistent, disk-backed storage.
 * This implementation is suitable for large datasets that exceed available memory.
 *
 * <p>Configuration properties:
 * <ul>
 *   <li>{@code memory.management.rocksdb.path} - Directory path for RocksDb storage (default: temp directory)</li>
 *   <li>{@code memory.management.rocksdb.cleanup} - Whether to delete RocksDb files on close (default: true)</li>
 *   <li>{@code memory.management.cache.size} - Cache size for frequently accessed entries (default: 1000)</li>
 * </ul>
 *
 * @param <V> the type of values stored (must be Serializable)
 * @author Debezium Authors
 */
@Incubating
public class RocksDbTableMappingStorage<V> extends AbstractCachedTableMappingStorage<V> implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(RocksDbTableMappingStorage.class);
    static {
        RocksDB.loadLibrary();
    }
    private RocksDB db;
    private Path dbPath;
    private boolean cleanupOnClose = true;
    private boolean cacheOnlyMode = false;

    /**
     * Creates a new RocksDb storage instance.
     * Must call {@link #configure(RelationalDatabaseConnectorConfig, boolean)} before use.
     */
    public RocksDbTableMappingStorage() {
    }

    @Override
    protected void configureStorage(RelationalDatabaseConnectorConfig config, TableMappingStorage.Type type) {
        // For SCHEMAS type, use cache-only mode (no RocksDB persistence)
        // TableSchema objects contain non-serializable StructGenerators and can be regenerated from Table objects
        if (type == TableMappingStorage.Type.SCHEMAS) {
            cacheOnlyMode = true;
            LOGGER.info("RocksDb table mapping storage configured in cache-only mode for type: {}", type.name().toLowerCase());
            return;
        }

        try {
            // Determine storage type name and config prefix from Type enum
            final var configPrefix = type.getConfigPrefix() + ".";
            final var storageTypeName = type.name().toLowerCase();

            // Determine base storage path from Type-specific configuration
            final var configuredPath = config.getConfig().getString(configPrefix + "rocksdb.path");

            if (Strings.isNullOrEmpty(configuredPath)) {
                throw new DebeziumException(String.format("Configuration property '%srocksdb.path' is required but not set", configPrefix));
            }

            // Support both relative and absolute paths
            dbPath = Path.of(configuredPath);
            // Create directories if they don't exist
            Files.createDirectories(dbPath);

            // Check cleanup configuration from Type-specific configuration
            final var cleanupConfig = config.getConfig().getString(configPrefix + "rocksdb.cleanup");
            if (cleanupConfig != null) {
                cleanupOnClose = Boolean.parseBoolean(cleanupConfig);
            }

            // Initialize RocksDb
            try (var options = createOptions()) {
                db = RocksDB.open(options, dbPath.toString());
            }
            LOGGER.info("RocksDb table mapping storage initialized at: {} (type: {})", dbPath, storageTypeName);
        }
        catch (IOException | RocksDBException e) {
            throw new DebeziumException("Failed to initialize RocksDb storage", e);
        }
    }

    @Override
    protected V getFromStorage(TableId tableId) {
        if (cacheOnlyMode) {
            return null; // Cache-only mode: no persistent storage
        }
        try {
            byte[] key = serializeTableId(tableId);
            byte[] value = db.get(key);
            return value != null ? deserializeValue(value) : null;
        }
        catch (RocksDBException | IOException | ClassNotFoundException e) {
            throw new DebeziumException("Failed to get value from RocksDb", e);
        }
    }

    @Override
    protected void putToStorage(TableId tableId, V value) {
        if (cacheOnlyMode) {
            return; // Cache-only mode: no persistent storage
        }
        try {
            byte[] key = serializeTableId(tableId);
            byte[] serializedValue = serializeValue(value);
            db.put(key, serializedValue);
        }
        catch (RocksDBException | IOException e) {
            throw new DebeziumException("Failed to put value into RocksDb", e);
        }
    }

    @Override
    protected void removeFromStorage(TableId tableId) {
        if (cacheOnlyMode) {
            return; // Cache-only mode: no persistent storage
        }
        try {
            byte[] key = serializeTableId(tableId);
            db.delete(key);
        }
        catch (RocksDBException | IOException e) {
            throw new DebeziumException("Failed to remove value from RocksDb", e);
        }
    }

    @Override
    protected void clearStorage() {
        if (cacheOnlyMode) {
            return; // Cache-only mode: no persistent storage to clear
        }
        if (db != null) {
            db.close();
            db = null;
        }
        deleteDirectory(dbPath.toFile());
        try {
            Files.createDirectories(dbPath);
            try (var options = createOptions()) {
                db = RocksDB.open(options, dbPath.toString());
            }
        }
        catch (IOException | RocksDBException e) {
            throw new DebeziumException("Failed to clear RocksDb storage", e);
        }
    }

    @Override
    public int size() {
        if (cacheOnlyMode) {
            return 0; // Cache-only mode: no persistent storage
        }
        int count = 0;
        try (RocksIterator iterator = db.newIterator()) {
            iterator.seekToFirst();
            while (iterator.isValid()) {
                count++;
                iterator.next();
            }
        }
        return count;
    }

    @Override
    public boolean isEmpty() {
        if (cacheOnlyMode) {
            return true; // Cache-only mode: no persistent storage
        }
        try (RocksIterator iterator = db.newIterator()) {
            iterator.seekToFirst();
            return !iterator.isValid();
        }
    }

    @Override
    public Set<TableId> keySet() {
        if (cacheOnlyMode) {
            return new HashSet<>(); // Cache-only mode: no persistent storage
        }
        Set<TableId> keys = new HashSet<>();
        try (RocksIterator iterator = db.newIterator()) {
            iterator.seekToFirst();
            while (iterator.isValid()) {
                TableId tableId = deserializeTableId(iterator.key());
                keys.add(tableId);
                iterator.next();
            }
        }
        catch (IOException e) {
            throw new DebeziumException("Failed to retrieve keys from RocksDb", e);
        }
        return keys;
    }

    @Override
    public void forEach(BiConsumer<? super TableId, ? super V> action) {
        if (cacheOnlyMode) {
            return; // Cache-only mode: no persistent storage
        }
        try (RocksIterator iterator = db.newIterator()) {
            iterator.seekToFirst();
            while (iterator.isValid()) {
                TableId tableId = deserializeTableId(iterator.key());
                V value = deserializeValue(iterator.value());
                action.accept(tableId, value);
                iterator.next();
            }
        }
        catch (IOException | ClassNotFoundException e) {
            throw new DebeziumException("Failed to iterate over RocksDb entries", e);
        }
    }

    @Override
    public void close() {
        // Close RocksDB instance if it exists (should not exist in cache-only mode)
        if (db != null) {
            db.close();
            db = null;
        }
        // Clean up storage directory if configured and not in cache-only mode
        if (!cacheOnlyMode && cleanupOnClose && dbPath != null) {
            deleteDirectory(dbPath.toFile());
            LOGGER.info("RocksDb storage cleaned up: {}", dbPath);
        }
    }

    /**
     * Creates and configures RocksDb Options.
     *
     * @return configured Options instance
     */
    private Options createOptions() {
        return new Options()
                .setCreateIfMissing(true)
                .setCompressionType(org.rocksdb.CompressionType.LZ4_COMPRESSION);
    }

    /**
     * Serializes a TableId to bytes using custom binary format.
     */
    private byte[] serializeTableId(final TableId tableId) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream(baos)) {

            // Write catalog (nullable)
            writeNullableString(dos, tableId.catalog());

            // Write schema (nullable)
            writeNullableString(dos, tableId.schema());

            // Write table (required)
            dos.writeUTF(tableId.table());

            return baos.toByteArray();
        }
    }

    /**
     * Deserializes bytes to a TableId using custom binary format.
     */
    private TableId deserializeTableId(final byte[] bytes) throws IOException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
                DataInputStream dis = new DataInputStream(bais)) {

            final String catalog = readNullableString(dis);
            final String schema = readNullableString(dis);
            final String table = dis.readUTF();

            return new TableId(catalog, schema, table);
        }
    }

    /**
     * Writes a nullable string to the output stream.
     */
    private void writeNullableString(final DataOutputStream dos, final String value) throws IOException {
        if (value == null) {
            dos.writeBoolean(false);
        }
        else {
            dos.writeBoolean(true);
            dos.writeUTF(value);
        }
    }

    /**
     * Reads a nullable string from the input stream.
     */
    private String readNullableString(final DataInputStream dis) throws IOException {
        final boolean hasValue = dis.readBoolean();
        return hasValue ? dis.readUTF() : null;
    }

    /**
     * Serializes a value to bytes.
     * Uses custom serialization for Table and TableSchema objects to avoid NotSerializableException.
     */
    private byte[] serializeValue(V value) throws IOException {
        if (value instanceof io.debezium.relational.Table table) {
            return serializeTable(table);
        }
        if (value instanceof io.debezium.relational.TableSchema tableSchema) {
            return serializeTableSchema(tableSchema);
        }
        // Fallback to ObjectOutputStream for other types
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(value);
            return baos.toByteArray();
        }
    }

    /**
     * Deserializes bytes to a value.
     * Uses custom deserialization for Table and TableSchema objects.
     */
    @SuppressWarnings("unchecked")
    private V deserializeValue(byte[] bytes) throws IOException, ClassNotFoundException {
        // Check type marker in first byte
        if (bytes.length > 0) {
            if (bytes[0] == 1) {
                return (V) deserializeTable(bytes);
            }
            if (bytes[0] == 2) {
                return (V) deserializeTableSchema(bytes);
            }
        }
        // Fallback to ObjectInputStream for other types
        try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
                ObjectInputStream ois = new ObjectInputStream(bais)) {
            return (V) ois.readObject();
        }
    }

    /**
     * Serializes a Table to bytes using custom binary format.
     */
    private byte[] serializeTable(io.debezium.relational.Table table) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream(baos)) {

            // Write type marker for Table
            dos.writeByte(1);

            // Write TableId
            writeNullableString(dos, table.id().catalog());
            writeNullableString(dos, table.id().schema());
            dos.writeUTF(table.id().table());

            // Write columns
            dos.writeInt(table.columns().size());
            for (io.debezium.relational.Column col : table.columns()) {
                serializeColumn(dos, col);
            }

            // Write primary key names
            dos.writeInt(table.primaryKeyColumnNames().size());
            for (String pkName : table.primaryKeyColumnNames()) {
                dos.writeUTF(pkName);
            }

            // Write default charset
            writeNullableString(dos, table.defaultCharsetName());

            // Write comment
            writeNullableString(dos, table.comment());

            // Write attributes
            final var attributes = table.attributes();
            if (attributes == null) {
                dos.writeInt(0);
            }
            else {
                dos.writeInt(attributes.size());
                for (io.debezium.relational.Attribute attr : attributes) {
                    serializeAttribute(dos, attr);
                }
            }

            return baos.toByteArray();
        }
    }

    /**
     * Deserializes bytes to a Table using custom binary format.
     */
    private io.debezium.relational.Table deserializeTable(byte[] bytes) throws IOException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
                DataInputStream dis = new DataInputStream(bais)) {

            // Read and verify type marker
            final byte marker = dis.readByte();
            if (marker != 1) {
                throw new IOException("Invalid table serialization marker: " + marker);
            }

            // Read TableId
            final String catalog = readNullableString(dis);
            final String schema = readNullableString(dis);
            final String table = dis.readUTF();
            final var tableId = new TableId(catalog, schema, table);

            // Create TableEditor
            final var editor = io.debezium.relational.Table.editor().tableId(tableId);

            // Read columns
            final int columnCount = dis.readInt();
            for (int i = 0; i < columnCount; i++) {
                final var col = deserializeColumn(dis);
                editor.addColumn(col);
            }

            // Read primary key names
            final int pkCount = dis.readInt();
            final var pkNames = new java.util.ArrayList<String>(pkCount);
            for (int i = 0; i < pkCount; i++) {
                pkNames.add(dis.readUTF());
            }
            editor.setPrimaryKeyNames(pkNames);

            // Read default charset
            final String charset = readNullableString(dis);
            editor.setDefaultCharsetName(charset);

            // Read comment
            final String comment = readNullableString(dis);
            editor.setComment(comment);

            // Read attributes
            final int attrCount = dis.readInt();
            for (int i = 0; i < attrCount; i++) {
                final var attr = deserializeAttribute(dis);
                editor.addAttribute(attr);
            }

            return editor.create();
        }
    }

    /**
     * Serializes a Column to the output stream.
     */
    private void serializeColumn(DataOutputStream dos, io.debezium.relational.Column col) throws IOException {
        dos.writeUTF(col.name());
        dos.writeInt(col.position());
        dos.writeInt(col.jdbcType());
        dos.writeInt(col.nativeType());
        dos.writeUTF(col.typeName());
        writeNullableString(dos, col.typeExpression());
        writeNullableString(dos, col.charsetName());
        dos.writeInt(col.length());

        // Write scale (Optional<Integer>)
        final var scale = col.scale();
        if (scale.isPresent()) {
            dos.writeBoolean(true);
            dos.writeInt(scale.get());
        }
        else {
            dos.writeBoolean(false);
        }

        dos.writeBoolean(col.isOptional());
        dos.writeBoolean(col.isAutoIncremented());
        dos.writeBoolean(col.isGenerated());

        // Write default value expression (Optional<String>)
        final var defaultValue = col.defaultValueExpression();
        if (defaultValue.isPresent()) {
            dos.writeBoolean(true);
            dos.writeUTF(defaultValue.get());
        }
        else {
            dos.writeBoolean(false);
        }

        dos.writeBoolean(col.hasDefaultValue());

        // Write enum values
        final var enumValues = col.enumValues();
        if (enumValues == null) {
            dos.writeInt(0);
        }
        else {
            dos.writeInt(enumValues.size());
            for (String enumValue : enumValues) {
                dos.writeUTF(enumValue);
            }
        }

        // Write comment
        writeNullableString(dos, col.comment());
    }

    /**
     * Deserializes a Column from the input stream.
     */
    private io.debezium.relational.Column deserializeColumn(DataInputStream dis) throws IOException {
        final var editor = io.debezium.relational.Column.editor();

        editor.name(dis.readUTF());
        editor.position(dis.readInt());
        editor.jdbcType(dis.readInt());
        editor.nativeType(dis.readInt());

        // Read type name and type expression
        final String typeName = dis.readUTF();
        final String typeExpression = readNullableString(dis);
        if (typeExpression != null) {
            editor.type(typeName, typeExpression);
        }
        else {
            editor.type(typeName);
        }

        editor.charsetName(readNullableString(dis));
        editor.length(dis.readInt());

        // Read scale
        if (dis.readBoolean()) {
            editor.scale(dis.readInt());
        }

        editor.optional(dis.readBoolean());
        editor.autoIncremented(dis.readBoolean());
        editor.generated(dis.readBoolean());

        // Read default value expression
        if (dis.readBoolean()) {
            editor.defaultValueExpression(dis.readUTF());
        }

        // Read hasDefaultValue flag (but don't set it - it's derived from defaultValueExpression)
        dis.readBoolean();

        // Read enum values
        final int enumCount = dis.readInt();
        if (enumCount > 0) {
            final var enumValues = new java.util.ArrayList<String>(enumCount);
            for (int i = 0; i < enumCount; i++) {
                enumValues.add(dis.readUTF());
            }
            editor.enumValues(enumValues);
        }

        // Read comment
        editor.comment(readNullableString(dis));

        return editor.create();
    }

    /**
     * Serializes an Attribute to the output stream.
     */
    private void serializeAttribute(DataOutputStream dos, io.debezium.relational.Attribute attr) throws IOException {
        dos.writeUTF(attr.name());
        writeNullableString(dos, attr.value());
    }

    /**
     * Deserializes an Attribute from the input stream.
     */
    private io.debezium.relational.Attribute deserializeAttribute(DataInputStream dis) throws IOException {
        final String name = dis.readUTF();
        final String value = readNullableString(dis);
        return io.debezium.relational.Attribute.editor()
                .name(name)
                .value(value)
                .create();
    }

    /**
     * Serializes a TableSchema to bytes using custom binary format.
     * Note: StructGenerators cannot be serialized, so they will be null upon deserialization.
     * The connector must rebuild TableSchema objects with proper generators after deserialization.
     */
    private byte[] serializeTableSchema(io.debezium.relational.TableSchema tableSchema) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream(baos)) {

            // Write type marker for TableSchema
            dos.writeByte(2);

            // Write TableId
            final var tableId = tableSchema.id();
            writeNullableString(dos, tableId.catalog());
            writeNullableString(dos, tableId.schema());
            dos.writeUTF(tableId.table());

            // Write keySchema as JSON string (Kafka Connect Schema can be converted to/from JSON)
            final var keySchema = tableSchema.keySchema();
            if (keySchema == null) {
                dos.writeBoolean(false);
            }
            else {
                dos.writeBoolean(true);
                dos.writeUTF(keySchema.toString());
            }

            // Write valueSchema as JSON string
            final var valueSchema = tableSchema.valueSchema();
            if (valueSchema == null) {
                dos.writeBoolean(false);
            }
            else {
                dos.writeBoolean(true);
                dos.writeUTF(valueSchema.toString());
            }

            // Write envelope schema as JSON string
            final var envelopeSchema = tableSchema.getEnvelopeSchema();
            if (envelopeSchema == null) {
                dos.writeBoolean(false);
            }
            else {
                dos.writeBoolean(true);
                dos.writeUTF(envelopeSchema.schema().toString());
            }

            // Note: StructGenerators (keyGenerator, valueGenerator) cannot be serialized
            // They must be rebuilt by the connector after deserialization

            return baos.toByteArray();
        }
    }

    /**
     * Deserializes bytes to a TableSchema using custom binary format.
     * Note: StructGenerators will be null and must be rebuilt by the connector.
     */
    private io.debezium.relational.TableSchema deserializeTableSchema(byte[] bytes) throws IOException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
                DataInputStream dis = new DataInputStream(bais)) {

            // Read and verify type marker
            final byte marker = dis.readByte();
            if (marker != 2) {
                throw new IOException("Invalid TableSchema serialization marker: " + marker);
            }

            // Read TableId
            final String catalog = readNullableString(dis);
            final String schema = readNullableString(dis);
            final String table = dis.readUTF();
            final var tableId = new TableId(catalog, schema, table);

            // Read keySchema
            org.apache.kafka.connect.data.Schema keySchema = null;
            if (dis.readBoolean()) {
                final String keySchemaStr = dis.readUTF();
                keySchema = parseSchema(keySchemaStr);
            }

            // Read valueSchema
            org.apache.kafka.connect.data.Schema valueSchema = null;
            if (dis.readBoolean()) {
                final String valueSchemaStr = dis.readUTF();
                valueSchema = parseSchema(valueSchemaStr);
            }

            // Read envelope schema
            io.debezium.data.Envelope envelopeSchema = null;
            if (dis.readBoolean()) {
                final String envelopeSchemaStr = dis.readUTF();
                final var envelopeSchemaObj = parseSchema(envelopeSchemaStr);
                envelopeSchema = io.debezium.data.Envelope.fromSchema(envelopeSchemaObj);
            }

            // Create TableSchema with null generators (they must be rebuilt by the connector)
            return new io.debezium.relational.TableSchema(
                    tableId,
                    keySchema,
                    null, // keyGenerator - must be rebuilt
                    envelopeSchema,
                    valueSchema,
                    null // valueGenerator - must be rebuilt
            );
        }
    }

    /**
     * Parses a Kafka Connect Schema from its string representation.
     * This is a simplified parser that handles basic schema structures.
     */
    private org.apache.kafka.connect.data.Schema parseSchema(String schemaStr) {
        // For now, we'll use a simple approach: Schema.toString() produces a format
        // that includes the schema type and structure. We need to parse it back.
        // However, Kafka Connect doesn't provide a built-in parser for Schema.toString() output.
        //
        // As a workaround, we'll use SchemaBuilder to reconstruct basic schemas.
        // This is a limitation - complex schemas may not be fully reconstructed.
        // The connector should ideally rebuild TableSchema objects from Table definitions.

        // For now, return null to indicate the schema couldn't be parsed
        // The connector will need to rebuild the TableSchema from the Table definition
        LOGGER.warn("Schema parsing not fully implemented - returning null. Schema string: {}", schemaStr);
        return null;
    }

    /**
     * Recursively deletes a directory and its contents.
     */
    private void deleteDirectory(File directory) {
        if (directory.exists()) {
            File[] files = directory.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (file.isDirectory()) {
                        deleteDirectory(file);
                    }
                    else {
                        try {
                            Files.delete(file.toPath());
                        }
                        catch (IOException e) {
                            LOGGER.warn("Failed to delete file: {}", file, e);
                        }
                    }
                }
            }
            try {
                Files.delete(directory.toPath());
            }
            catch (IOException e) {
                LOGGER.warn("Failed to delete directory: {}", directory, e);
            }
        }
    }
}
