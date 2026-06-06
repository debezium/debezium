/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.mysql.antlr.listener;

import java.sql.Types;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.antlr.v4.runtime.tree.ParseTreeListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.antlr.AntlrDdlParser;
import io.debezium.antlr.DataTypeResolver;
import io.debezium.connector.mysql.antlr.MySqlAntlrDdlParser;
import io.debezium.ddl.parser.mysql.generated.MySqlParser;
import io.debezium.ddl.parser.mysql.generated.MySqlParserBaseListener;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.TableEditor;
import io.debezium.relational.ddl.DataType;

/**
 * Parser listener that is parsing column definition part of MySQL statements.
 *
 * @author Roman Kuchár <kucharrom@gmail.com>.
 */
public class ColumnDefinitionParserListener extends MySqlParserBaseListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(ColumnDefinitionParserListener.class);

    private static final Pattern DOT = Pattern.compile("\\.");
    private static final Pattern SIGNED_PATTERN = Pattern.compile("SIGNED", Pattern.CASE_INSENSITIVE);
    private static final Pattern UNSIGNED_PATTERN = Pattern.compile("UNSIGNED", Pattern.CASE_INSENSITIVE);
    private static final Pattern ZEROFILL_PATTERN = Pattern.compile("ZEROFILL", Pattern.CASE_INSENSITIVE);
    private static final Pattern BINARY_PATTERN = Pattern.compile("BINARY", Pattern.CASE_INSENSITIVE);

    private final MySqlAntlrDdlParser parser;
    private final DataTypeResolver dataTypeResolver;
    private final TableEditor tableEditor;
    private ColumnEditor columnEditor;
    private boolean uniqueColumn;
    private AtomicReference<Boolean> optionalColumn = new AtomicReference<>();
    private DefaultValueParserListener defaultValueListener;
    private boolean skipColumnAddition = false; // Set to true in multi-column ALTER TABLE scenarios

    private final List<ParseTreeListener> listeners;

    public ColumnDefinitionParserListener(TableEditor tableEditor, ColumnEditor columnEditor, MySqlAntlrDdlParser parser,
                                          List<ParseTreeListener> listeners) {
        this.tableEditor = tableEditor;
        this.columnEditor = columnEditor;
        this.parser = parser;
        this.dataTypeResolver = parser.dataTypeResolver();
        this.listeners = listeners;
    }

    public void setColumnEditor(ColumnEditor columnEditor) {
        this.columnEditor = columnEditor;
    }

    public ColumnEditor getColumnEditor() {
        return columnEditor;
    }

    /**
     * Set to true when this listener is being used for multi-column ALTER TABLE scenarios
     * where the AlterTableParserListener will handle column addition.
     */
    public void setSkipColumnAddition(boolean skipColumnAddition) {
        this.skipColumnAddition = skipColumnAddition;
    }

    public Column getColumn() {
        return columnEditor.create();
    }

    @Override
    public void enterColumnDefinition(MySqlParser.ColumnDefinitionContext ctx) {
        uniqueColumn = false;
        optionalColumn = new AtomicReference<>();

        resolveColumnDataType(ctx.fieldDefinition().dataType());

        parser.runIfNotNull(() -> {
            defaultValueListener = new DefaultValueParserListener(this, optionalColumn);
            listeners.add(defaultValueListener);
        }, tableEditor);
        super.enterColumnDefinition(ctx);
    }

    /**
     * Process a fieldDefinition directly (used by ALTER TABLE where there's no columnDefinition wrapper).
     * This initializes the column's data type and sets up listeners for default values.
     */
    public void processFieldDefinition(MySqlParser.FieldDefinitionContext ctx) {
        uniqueColumn = false;
        optionalColumn = new AtomicReference<>();

        resolveColumnDataType(ctx.dataType());

        parser.runIfNotNull(() -> {
            defaultValueListener = new DefaultValueParserListener(this, optionalColumn);
            listeners.add(defaultValueListener);
        }, tableEditor);
    }

    /**
     * Process a columnDefinition manually, including all column attributes.
     * Used in multi-column ALTER TABLE scenarios where we need to manually walk the parse tree.
     */
    public void processColumnDefinition(MySqlParser.ColumnDefinitionContext ctx) {
        uniqueColumn = false;
        optionalColumn = new AtomicReference<>();

        resolveColumnDataType(ctx.fieldDefinition().dataType());

        parser.runIfNotNull(() -> {
            defaultValueListener = new DefaultValueParserListener(this, optionalColumn);
            // Manually process all column attributes
            if (ctx.fieldDefinition().columnAttribute() != null) {
                for (MySqlParser.ColumnAttributeContext attrCtx : ctx.fieldDefinition().columnAttribute()) {
                    // Process specific attributes without triggering listener lifecycle
                    processColumnAttributeDirect(attrCtx);
                    // Also process DEFAULT values through the default value listener
                    defaultValueListener.enterColumnAttribute(attrCtx);
                }
            }
            defaultValueListener.exitDefaultValue(true);
        }, tableEditor);
    }

    /**
     * Process column attributes directly without triggering the listener lifecycle.
     * Used in manual processing mode (multi-column ALTER TABLE).
     */
    private void processColumnAttributeDirect(MySqlParser.ColumnAttributeContext ctx) {
        // Handle NULL/NOT NULL (no value label, check for nullLiteral)
        if (ctx.nullLiteral() != null) {
            optionalColumn.set(Boolean.valueOf(ctx.NOT_SYMBOL() == null));
        }
    }

    /**
     * Finalize column processing (used by ALTER TABLE where there's no columnDefinition wrapper).
     * Sets the optional property and creates the final column.
     * Should be called after all columnAttribute events have been processed.
     */
    public Column finalizeColumn() {
        if (optionalColumn.get() != null) {
            columnEditor.optional(optionalColumn.get().booleanValue());
        }
        else {
            // MySQL default behavior: columns are nullable unless explicitly marked NOT NULL
            columnEditor.optional(true);
        }

        parser.runIfNotNull(() -> {
            if (defaultValueListener != null) {
                defaultValueListener.exitDefaultValue(false);
                listeners.remove(defaultValueListener);
            }
        }, tableEditor);

        return columnEditor.create();
    }

    @Override
    public void exitColumnDefinition(MySqlParser.ColumnDefinitionContext ctx) {
        if (optionalColumn.get() != null) {
            columnEditor.optional(optionalColumn.get().booleanValue());
        }
        else {
            // MySQL default behavior: columns are nullable unless explicitly marked NOT NULL
            // This handles implicit nullability in DDL like: ALTER TABLE ... CHANGE col col DATE DEFAULT NULL
            columnEditor.optional(true);
        }

        // In multi-column ALTER TABLE scenarios, the AlterTableParserListener handles column addition
        // Skip adding the column here to avoid adding wrong/incomplete column editors
        if (!skipColumnAddition) {
            if (uniqueColumn && !tableEditor.hasPrimaryKey()) {
                // take the first unique constrain if no primary key is set
                tableEditor.addColumn(columnEditor.create());
                tableEditor.setPrimaryKeyNames(columnEditor.name());
            }
            else {
                // Add the column to the table after all properties have been set
                tableEditor.addColumn(columnEditor.create());
            }
        }

        parser.runIfNotNull(() -> {
            if (defaultValueListener != null) {
                defaultValueListener.exitDefaultValue(false);
                listeners.remove(defaultValueListener);
            }
        }, tableEditor);
        super.exitColumnDefinition(ctx);
    }

    @Override
    public void enterColumnAttribute(MySqlParser.ColumnAttributeContext ctx) {
        // Handle COLLATE
        if (ctx.collate() != null && ctx.collate().collationName() != null) {
            String collationName = ctx.collate().collationName().getText();
            // Infer charset from collation name (e.g., "utf8mb4_unicode_ci" -> "utf8mb4")
            if (collationName != null && !collationName.isEmpty()) {
                int underscorePos = collationName.indexOf('_');
                if (underscorePos > 0) {
                    String inferredCharset = collationName.substring(0, underscorePos);
                    // Only set if charset wasn't already explicitly specified
                    if (columnEditor.charsetName() == null) {
                        columnEditor.charsetName(inferredCharset);
                    }
                }
            }
        }
        // Handle NULL/NOT NULL (no value label, check for nullLiteral)
        else if (ctx.nullLiteral() != null) {
            optionalColumn.set(Boolean.valueOf(ctx.NOT_SYMBOL() == null));
        }
        // Handle attributes with value labels
        else if (ctx.value != null) {
            int valueType = ctx.value.getType();

            // UNIQUE KEY
            if (valueType == MySqlParser.UNIQUE_SYMBOL) {
                uniqueColumn = true;
            }
            // PRIMARY KEY
            else if (valueType == MySqlParser.KEY_SYMBOL) {
                // this rule will be parsed only if no primary key is set in a table
                // otherwise the statement can't be executed due to multiple primary key error
                optionalColumn.set(Boolean.FALSE);
                tableEditor.addColumn(columnEditor.create());
                tableEditor.setPrimaryKeyNames(columnEditor.name());
            }
            // COMMENT
            else if (valueType == MySqlParser.COMMENT_SYMBOL) {
                if (!parser.skipComments()) {
                    if (ctx.textLiteral() != null) {
                        String commentText = ctx.textLiteral().getText();
                        columnEditor.comment(parser.withoutQuotes(commentText));
                    }
                }
            }
            // AUTO_INCREMENT
            else if (valueType == MySqlParser.AUTO_INCREMENT_SYMBOL) {
                columnEditor.autoIncremented(true);
                columnEditor.generated(true);
            }
            // SERIAL DEFAULT VALUE
            else if (valueType == MySqlParser.SERIAL_SYMBOL) {
                serialColumn();
            }
            // ON UPDATE NOW() - marks column as auto-updated/generated
            else if (valueType == MySqlParser.ON_SYMBOL) {
                columnEditor.autoIncremented(true);
                columnEditor.generated(true);
            }
        }
        super.enterColumnAttribute(ctx);
    }

    private void resolveColumnDataType(MySqlParser.DataTypeContext dataTypeContext) {
        LOGGER.debug("Resolving dataType: {}", dataTypeContext.getText());

        String charsetName = null;
        DataType dataType = null;
        Integer fallbackJdbcType = null;
        try {
            dataType = dataTypeResolver.resolveDataType(dataTypeContext);
        }
        catch (Exception e) {
            // DataTypeResolver couldn't resolve the type (e.g., NATIONAL CHAR)
            String contextText = dataTypeContext.getText().toUpperCase();
            if (contextText.contains("NATIONAL") || contextText.startsWith("NCHAR")) {
                fallbackJdbcType = contextText.contains("VARCHAR") || contextText.contains("VARYING")
                        ? Types.NVARCHAR
                        : Types.NCHAR;
                dataType = io.debezium.relational.ddl.DataType.userDefinedType(contextText);
                LOGGER.debug("Created fallback DataType for NATIONAL type: {}, jdbcType={}", contextText, fallbackJdbcType);
            }
            else {
                throw e;
            }
        }

        LOGGER.debug("Resolved dataType: name={}, jdbcType={}", dataType.name(), dataType.jdbcType());

        // Track if BINARY modifier is present (from charsetWithOptBinary context or dataType context)
        boolean hasBinaryModifier = false;

        // Check for BINARY modifier in dataType context (for NCHAR/NVARCHAR)
        if (dataTypeContext.BINARY_SYMBOL() != null) {
            hasBinaryModifier = true;
        }

        // Extract charset from charsetWithOptBinary if present
        if (dataTypeContext.charsetWithOptBinary() != null) {
            MySqlParser.CharsetWithOptBinaryContext charsetCtx = dataTypeContext.charsetWithOptBinary();
            if (charsetCtx.charsetName() != null) {
                charsetName = parser.extractCharset(charsetCtx.charsetName(), null);
            }
            if (charsetCtx.BINARY_SYMBOL() != null) {
                hasBinaryModifier = true;
            }
        }

        // Handle fieldLength for types that support it
        if (dataTypeContext.fieldLength() != null) {
            String lengthText = extractFieldLength(dataTypeContext.fieldLength());
            if (lengthText != null) {
                Integer length = parseLength(lengthText);
                columnEditor.length(length);
            }
        }

        // Handle typeDatetimePrecision for TIMESTAMP/DATETIME/TIME types
        if (dataTypeContext.typeDatetimePrecision() != null) {
            String precisionText = dataTypeContext.typeDatetimePrecision().INT_NUMBER().getText();
            if (precisionText != null) {
                Integer precision = Integer.valueOf(precisionText);
                columnEditor.length(precision);
            }
        }

        // Handle precision (two dimensions like DECIMAL(10,2))
        if (dataTypeContext.precision() != null) {
            // precision: OPEN_PAR_SYMBOL INT_NUMBER COMMA_SYMBOL INT_NUMBER CLOSE_PAR_SYMBOL
            String precisionText = dataTypeContext.precision().getText();
            // Remove parentheses and split by comma
            precisionText = precisionText.substring(1, precisionText.length() - 1);
            String[] parts = precisionText.split(",");
            if (parts.length == 2) {
                Integer length = parseLength(parts[0].trim());
                Integer scale = Integer.valueOf(parts[1].trim());
                columnEditor.length(length);
                columnEditor.scale(scale);
            }
        }

        // Handle floatOptions (can be either fieldLength or precision)
        if (dataTypeContext.floatOptions() != null) {
            MySqlParser.FloatOptionsContext floatOpts = dataTypeContext.floatOptions();
            if (floatOpts.fieldLength() != null) {
                String lengthText = extractFieldLength(floatOpts.fieldLength());
                if (lengthText != null) {
                    Integer length = parseLength(lengthText);
                    columnEditor.length(length);
                }
            }
            else if (floatOpts.precision() != null) {
                String precisionText = floatOpts.precision().getText();
                precisionText = precisionText.substring(1, precisionText.length() - 1);
                String[] parts = precisionText.split(",");
                if (parts.length == 2) {
                    Integer length = parseLength(parts[0].trim());
                    Integer scale = Integer.valueOf(parts[1].trim());
                    columnEditor.length(length);
                    columnEditor.scale(scale);
                }
            }
        }

        // Handle ENUM and SET types
        if (dataTypeContext.stringList() != null) {
            MySqlParser.StringListContext stringListCtx = dataTypeContext.stringList();

            // Extract charset if specified
            if (dataTypeContext.charsetWithOptBinary() != null &&
                    dataTypeContext.charsetWithOptBinary().charsetName() != null) {
                charsetName = dataTypeContext.charsetWithOptBinary().charsetName().getText();
            }

            String dataTypeName = dataType.name().toUpperCase();
            if (dataTypeName.equals("SET")) {
                // After DBZ-132, it will always be comma separated
                int optionsSize = stringListCtx.textString().size();
                columnEditor.length(Math.max(0, optionsSize * 2 - 1)); // number of options + number of commas
            }
            else {
                columnEditor.length(1);
            }
        }

        String dataTypeName = dataType.name() != null ? dataType.name().toUpperCase() : null;

        // Fix DataTypeResolver misidentification: "NCHAR BINARY" resolves to "BINARY" instead of "NCHAR"
        // Also use fallback JDBC type if DataTypeResolver failed
        Integer overrideJdbcType = fallbackJdbcType;
        String contextTextUpper = dataTypeContext.getText().toUpperCase();
        if ("BINARY".equals(dataTypeName) && (contextTextUpper.startsWith("NCHAR") || contextTextUpper.startsWith("NVARCHAR"))) {
            if (contextTextUpper.startsWith("NVARCHAR")) {
                dataTypeName = "NVARCHAR";
                overrideJdbcType = Types.NVARCHAR;
            }
            else if (contextTextUpper.startsWith("NCHAR")) {
                dataTypeName = "NCHAR";
                overrideJdbcType = Types.NCHAR;
            }
        }

        // Handle NATIONAL CHAR variants - preserve exact DDL syntax with proper spacing
        int effectiveJdbcType = (overrideJdbcType != null) ? overrideJdbcType : dataType.jdbcType();
        if (effectiveJdbcType == Types.NCHAR || effectiveJdbcType == Types.NVARCHAR) {
            String contextText = dataTypeContext.getText().toUpperCase();

            // Remove field length/precision to get just the type name
            String typeOnlyText = contextText.replaceAll("\\([^)]*\\)", "");

            // Match common NATIONAL/NCHAR type patterns
            if (typeOnlyText.equals("NATIONALCHAR")) {
                dataTypeName = "NATIONAL CHAR";
            }
            else if (typeOnlyText.equals("NATIONALVARCHAR")) {
                dataTypeName = "NATIONAL VARCHAR";
            }
            else if (typeOnlyText.equals("NATIONALCHARACTER")) {
                dataTypeName = "NATIONAL CHARACTER";
            }
            else if (typeOnlyText.contains("NATIONALCHARACTER") && typeOnlyText.contains("VARYING")) {
                dataTypeName = "NATIONAL CHARACTER VARYING";
            }
            else if (typeOnlyText.contains("NATIONALCHAR") && typeOnlyText.contains("VARYING")) {
                dataTypeName = "NATIONAL CHAR VARYING";
            }
            else if (typeOnlyText.contains("NCHAR") && typeOnlyText.contains("VARCHAR")) {
                dataTypeName = "NCHAR VARCHAR";
            }
            else if (typeOnlyText.contains("NCHAR") && typeOnlyText.contains("VARYING")) {
                dataTypeName = "NCHAR VARYING";
            }
            else if (typeOnlyText.equals("NCHAR")) {
                dataTypeName = "NCHAR";
            }
            else if (typeOnlyText.equals("NVARCHAR")) {
                dataTypeName = "NVARCHAR";
            }
            else if (dataTypeName == null) {
                dataTypeName = (effectiveJdbcType == Types.NCHAR) ? "NCHAR" : "NVARCHAR";
            }
        }

        // Handle fieldOptions (SIGNED, UNSIGNED, ZEROFILL, BINARY)
        if (dataTypeName != null && dataTypeContext.fieldOptions() != null) {
            MySqlParser.FieldOptionsContext fieldOpts = dataTypeContext.fieldOptions();

            // Track LAST occurrence of each modifier type (MySQL behavior)
            String signedness = null;
            boolean zerofill = false;
            boolean binary = false;
            for (int i = 0; i < fieldOpts.getChildCount(); i++) {
                String childText = fieldOpts.getChild(i).getText();
                if (SIGNED_PATTERN.matcher(childText).matches()) {
                    signedness = "SIGNED";
                }
                else if (UNSIGNED_PATTERN.matcher(childText).matches()) {
                    signedness = "UNSIGNED";
                }
                else if (ZEROFILL_PATTERN.matcher(childText).matches()) {
                    zerofill = true;
                }
                else if (BINARY_PATTERN.matcher(childText).matches()) {
                    binary = true;
                }
            }

            // Build final modifier string - MySQL rule: ZEROFILL implies UNSIGNED
            StringBuilder modifiers = new StringBuilder();
            if (zerofill) {
                modifiers.append(" UNSIGNED ZEROFILL");
            }
            else if (signedness != null) {
                modifiers.append(" ").append(signedness);
            }
            if (binary) {
                modifiers.append(" BINARY");
            }

            if (modifiers.length() > 0) {
                dataTypeName = dataTypeName + modifiers.toString();
            }
        }

        // Apply BINARY modifier from charsetWithOptBinary if present
        // This comes from CHAR/VARCHAR with BINARY keyword (e.g., CHAR(60) BINARY)
        if (hasBinaryModifier && dataTypeName != null && !dataTypeName.endsWith(" BINARY")) {
            dataTypeName = dataTypeName + " BINARY";
            LOGGER.debug("Added BINARY modifier from charsetWithOptBinary: {}", dataTypeName);
        }

        // If we still don't have a type name, something went wrong
        if (dataTypeName == null) {
            LOGGER.warn("DataType name is null for context: {}. JDBC type: {}",
                    dataTypeContext.getText(), dataType.jdbcType());
        }

        if (dataTypeName != null && (dataTypeName.equals("ENUM") || dataTypeName.equals("SET"))) {
            MySqlParser.StringListContext stringListCtx = dataTypeContext.stringList();
            List<String> collectionOptions = stringListCtx.textString().stream()
                    .map(AntlrDdlParser::getText)
                    .collect(Collectors.toList());
            columnEditor.type(dataTypeName);
            columnEditor.enumValues(collectionOptions);
        }
        else if (dataTypeName != null && dataTypeName.equals("SERIAL")) {
            // SERIAL is an alias for BIGINT UNSIGNED NOT NULL AUTO_INCREMENT UNIQUE
            columnEditor.type("BIGINT UNSIGNED");
            serialColumn();
        }
        else {
            columnEditor.type(dataTypeName);
        }

        // Use override JDBC type if we corrected a misidentification, otherwise use dataType's JDBC type
        int jdbcDataType = (overrideJdbcType != null) ? overrideJdbcType : dataType.jdbcType();
        columnEditor.jdbcType(jdbcDataType);

        if (columnEditor.length() == -1) {
            columnEditor.length((int) dataType.length());
        }
        if (!columnEditor.scale().isPresent() && dataType.scale() != Column.UNSET_INT_VALUE) {
            columnEditor.scale(dataType.scale());
        }
        if (Types.NCHAR == jdbcDataType || Types.NVARCHAR == jdbcDataType) {
            // NCHAR and NVARCHAR columns always uses utf8 as charset
            columnEditor.charsetName("utf8");

            if (Types.NCHAR == jdbcDataType && columnEditor.length() == -1) {
                // Explicitly set NCHAR column size as 1 when no length specified
                columnEditor.length(1);
            }
        }
        else {
            columnEditor.charsetName(charsetName);
        }
    }

    private String extractFieldLength(MySqlParser.FieldLengthContext fieldLengthCtx) {
        // fieldLength: OPEN_PAR_SYMBOL (real_ulonglong_number | DECIMAL_NUMBER) CLOSE_PAR_SYMBOL
        if (fieldLengthCtx.real_ulonglong_number() != null) {
            return fieldLengthCtx.real_ulonglong_number().getText();
        }
        else if (fieldLengthCtx.DECIMAL_NUMBER() != null) {
            return fieldLengthCtx.DECIMAL_NUMBER().getText();
        }
        return null;
    }

    private Integer parseLength(String lengthStr) {
        // Handle decimal values (invalid MySQL syntax but sometimes present in DDL)
        // e.g., DECIMAL(19.5) should be treated as DECIMAL(19)
        Long length;
        try {
            length = Long.parseLong(lengthStr);
        }
        catch (NumberFormatException e) {
            // Try parsing as double and truncate to integer
            try {
                double doubleValue = Double.parseDouble(lengthStr);
                length = (long) doubleValue;
                LOGGER.warn("Column `{}`.`{}` has decimal length '{}', truncating to '{}'",
                        tableEditor.tableId(), columnEditor.name(), lengthStr, length);
            }
            catch (NumberFormatException e2) {
                // If still can't parse, return default
                LOGGER.warn("Cannot parse length '{}' for column `{}`.`{}`, using default 10",
                        lengthStr, tableEditor.tableId(), columnEditor.name());
                return 10; // MySQL default for DECIMAL
            }
        }

        // If length is 0 or negative (from truncation like 0.0 → 0), use MySQL default
        if (length <= 0) {
            LOGGER.warn("Column `{}`.`{}` has invalid length '{}', using default 10",
                    tableEditor.tableId(), columnEditor.name(), length);
            return 10; // MySQL default for DECIMAL
        }

        if (length > Integer.MAX_VALUE) {
            LOGGER.warn("The length '{}' of the column `{}`.`{}` is too large to be supported, truncating it to '{}'",
                    length, tableEditor.tableId(), columnEditor.name(), Integer.MAX_VALUE);
            length = (long) Integer.MAX_VALUE;
        }
        return length.intValue();
    }

    private void serialColumn() {
        if (optionalColumn.get() == null) {
            optionalColumn.set(Boolean.FALSE);
        }
        uniqueColumn = true;
        columnEditor.autoIncremented(true);
        columnEditor.generated(true);
    }
}
