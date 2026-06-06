/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.mysql.antlr.listener;

import static io.debezium.antlr.AntlrDdlParser.getText;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.antlr.v4.runtime.tree.ParseTreeListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.mysql.antlr.MySqlAntlrDdlParser;
import io.debezium.ddl.parser.mysql.generated.MySqlParser;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.TableId;
import io.debezium.text.ParsingException;

/**
 * Parser listener that is parsing MySQL ALTER TABLE statements.
 *
 * @author Roman Kuchár <kucharrom@gmail.com>.
 */
public class AlterTableParserListener extends TableCommonParserListener {

    private static final int STARTING_INDEX = 1;

    private final static Logger LOG = LoggerFactory.getLogger(AlterTableParserListener.class);

    private ColumnEditor defaultValueColumnEditor;
    private DefaultValueParserListener defaultValueListener;

    private List<ColumnEditor> columnEditors;
    private int parsingColumnIndex = STARTING_INDEX;

    private AlterType currentAlterType = AlterType.OTHER;
    private MySqlParser.AlterListItemContext currentAlterContext = null;

    public AlterTableParserListener(MySqlAntlrDdlParser parser, List<ParseTreeListener> listeners) {
        super(parser, listeners);
    }

    @Override
    public void enterAlterTable(MySqlParser.AlterTableContext ctx) {
        final TableId tableId = parser.parseQualifiedTableId(ctx.tableRef());
        if (parser.databaseTables().forTable(tableId) == null) {
            LOG.debug("Ignoring ALTER TABLE statement for non-captured table {}", tableId);
            return;
        }
        tableEditor = parser.databaseTables().editTable(tableId);
        if (tableEditor == null) {
            throw new ParsingException(null, "Trying to alter table " + tableId.toString()
                    + ", which does not exist. Query: " + getText(ctx));
        }
        super.enterAlterTable(ctx);
    }

    @Override
    public void exitAlterTable(MySqlParser.AlterTableContext ctx) {
        parser.runIfNotNull(() -> {
            if (columnDefinitionListener != null) {
                listeners.remove(columnDefinitionListener);
            }
            parser.databaseTables().overwriteTable(tableEditor.create());
            parser.signalAlterTable(tableEditor.tableId(), null, ctx.getParent());
        }, tableEditor);
        super.exitAlterTable(ctx);
        tableEditor = null;
    }

    @Override
    public void enterAlterListItem(MySqlParser.AlterListItemContext ctx) {
        parser.runIfNotNull(() -> {
            currentAlterContext = ctx;
            currentAlterType = detectAlterType(ctx);

            switch (currentAlterType) {
                case ADD_COLUMN -> handleEnterAddColumn(ctx);
                case ADD_COLUMNS -> handleEnterAddColumns(ctx);
                case CHANGE_COLUMN -> handleEnterChangeColumn(ctx);
                case MODIFY_COLUMN -> handleEnterModifyColumn(ctx);
                case DROP_COLUMN -> handleEnterDropColumn(ctx);
                case DROP_PRIMARY_KEY -> handleEnterDropPrimaryKey(ctx);
                case RENAME_TABLE -> handleEnterRenameTable(ctx);
                case ALTER_DEFAULT -> handleEnterAlterDefault(ctx);
                case ADD_CONSTRAINT -> {
                } // Handled by TableCommonParserListener.enterTableConstraintDef
                case RENAME_COLUMN -> handleEnterRenameColumn(ctx);
                case CONVERT_CHARSET -> handleEnterConvertCharset(ctx);
                default -> {
                }
            }
        }, tableEditor);
        super.enterAlterListItem(ctx);
    }

    @Override
    public void exitAlterListItem(MySqlParser.AlterListItemContext ctx) {
        parser.runIfNotNull(() -> {
            if (ctx != currentAlterContext) {
                return; // wrong context - do nothing
            }

            switch (currentAlterType) {
                case ADD_COLUMN -> handleExitAddColumn(ctx);
                case ADD_COLUMNS -> handleExitAddColumns(ctx);
                case CHANGE_COLUMN -> handleExitChangeColumn(ctx);
                case MODIFY_COLUMN -> handleExitModifyColumn(ctx);
                case ALTER_DEFAULT -> handleExitAlterDefault(ctx);
                case RENAME_COLUMN -> handleExitRenameColumn(ctx);
                default -> {
                }
            }

            currentAlterType = AlterType.OTHER;
            currentAlterContext = null;
        }, tableEditor);
        super.exitAlterListItem(ctx);
    }

    @Override
    public void enterColumnDefinition(MySqlParser.ColumnDefinitionContext ctx) {
        // In multi-column mode, manually process column definition (not in listeners list)
        if (columnEditors != null) {
            columnDefinitionListener.processColumnDefinition(ctx);
            return;
        }
        super.enterColumnDefinition(ctx);
    }

    @Override
    public void exitColumnDefinition(MySqlParser.ColumnDefinitionContext ctx) {
        // In multi-column mode, manually call listener and switch editors
        if (columnEditors != null) {
            // Manually call exit on current editor BEFORE switching
            columnDefinitionListener.exitColumnDefinition(ctx);

            // Then switch to next editor for the next column
            parser.runIfNotNull(() -> {
                if (columnEditors.size() > parsingColumnIndex) {
                    columnDefinitionListener.setColumnEditor(columnEditors.get(parsingColumnIndex++));
                }
            }, tableEditor);
            return;
        }
        super.exitColumnDefinition(ctx);
    }

    private AlterType detectAlterType(MySqlParser.AlterListItemContext ctx) {
        // ADD COLUMN (with or without COLUMN keyword)
        if (ctx.ADD_SYMBOL() != null) {
            if (ctx.tableConstraintDef() != null) {
                return AlterType.ADD_CONSTRAINT;
            }
            else if (ctx.tableElementList() != null) {
                return AlterType.ADD_COLUMNS;
            }
            else if (ctx.identifier() != null && ctx.fieldDefinition() != null) {
                return AlterType.ADD_COLUMN;
            }
        }
        // CHANGE COLUMN
        else if (ctx.CHANGE_SYMBOL() != null && ctx.columnInternalRef() != null) {
            return AlterType.CHANGE_COLUMN;
        }
        // MODIFY COLUMN
        else if (ctx.MODIFY_SYMBOL() != null && ctx.columnInternalRef() != null) {
            return AlterType.MODIFY_COLUMN;
        }
        // ALTER COLUMN (SET/DROP DEFAULT) - Check BEFORE DROP COLUMN
        // because "ALTER COLUMN x DROP DEFAULT" has both ALTER_SYMBOL and DROP_SYMBOL
        else if (ctx.ALTER_SYMBOL() != null && ctx.columnInternalRef() != null) {
            return AlterType.ALTER_DEFAULT;
        }
        // DROP PRIMARY KEY
        else if (ctx.DROP_SYMBOL() != null && ctx.PRIMARY_SYMBOL() != null && ctx.KEY_SYMBOL() != null) {
            return AlterType.DROP_PRIMARY_KEY;
        }
        // DROP COLUMN
        else if (ctx.DROP_SYMBOL() != null && ctx.columnInternalRef() != null) {
            // Check if it's DROP COLUMN (not DROP FOREIGN KEY, etc.)
            if (ctx.PRIMARY_SYMBOL() == null && ctx.FOREIGN_SYMBOL() == null &&
                    ctx.keyOrIndex() == null && ctx.CHECK_SYMBOL() == null && ctx.CONSTRAINT_SYMBOL() == null) {
                return AlterType.DROP_COLUMN;
            }
        }
        // RENAME TABLE
        else if (ctx.RENAME_SYMBOL() != null && ctx.tableName() != null) {
            return AlterType.RENAME_TABLE;
        }
        // RENAME COLUMN
        else if (ctx.RENAME_SYMBOL() != null && ctx.COLUMN_SYMBOL() != null && ctx.columnInternalRef() != null) {
            return AlterType.RENAME_COLUMN;
        }
        // CONVERT TO CHARSET
        else if (ctx.CONVERT_SYMBOL() != null && ctx.charset() != null) {
            return AlterType.CONVERT_CHARSET;
        }

        return AlterType.OTHER;
    }

    private void handleEnterAddColumn(MySqlParser.AlterListItemContext ctx) {
        String columnName = parser.parseName(ctx.identifier());
        ColumnEditor columnEditor = Column.editor().name(columnName);
        columnDefinitionListener = new ColumnDefinitionParserListener(tableEditor, columnEditor, parser, listeners);
        listeners.add(columnDefinitionListener);

        // Process fieldDefinition directly since there's no columnDefinition wrapper in ALTER TABLE
        if (ctx.fieldDefinition() != null) {
            columnDefinitionListener.processFieldDefinition(ctx.fieldDefinition());
        }
    }

    private void handleExitAddColumn(MySqlParser.AlterListItemContext ctx) {
        if (columnDefinitionListener == null) {
            return;
        }

        // Finalize column processing and get the column with all properties set
        Column column = columnDefinitionListener.finalizeColumn();
        tableEditor.addColumn(column);

        String columnName = column.name();
        // Check for FIRST or AFTER
        if (ctx.place() != null) {
            if (ctx.place().FIRST_SYMBOL() != null) {
                tableEditor.reorderColumn(columnName, null);
            }
            else if (ctx.place().AFTER_SYMBOL() != null) {
                String afterColumn = parser.parseName(ctx.place().identifier());
                tableEditor.reorderColumn(columnName, afterColumn);
            }
        }
        listeners.remove(columnDefinitionListener);
        columnDefinitionListener = null;
    }

    private void handleEnterAddColumns(MySqlParser.AlterListItemContext ctx) {
        // ADD (tableElementList) - multiple columns
        if (ctx.tableElementList() == null || ctx.tableElementList().tableElement() == null) {
            return;
        }

        List<MySqlParser.TableElementContext> elements = ctx.tableElementList().tableElement();
        List<MySqlParser.ColumnDefinitionContext> columnDefs = new ArrayList<>();

        // Extract column definitions from table elements
        for (MySqlParser.TableElementContext element : elements) {
            if (element.columnDefinition() != null) {
                columnDefs.add(element.columnDefinition());
            }
        }

        if (columnDefs.isEmpty()) {
            return;
        }

        // Initialize column editors for all columns
        columnEditors = new ArrayList<>(columnDefs.size());
        for (MySqlParser.ColumnDefinitionContext colDef : columnDefs) {
            String columnName = parser.parseName(colDef.columnName().identifier());
            columnEditors.add(Column.editor().name(columnName));
        }

        columnDefinitionListener = new ColumnDefinitionParserListener(tableEditor, columnEditors.get(0), parser, listeners);
        columnDefinitionListener.setSkipColumnAddition(true); // Multi-column scenario: columns added in handleExitAddColumns
    }

    private void handleExitAddColumns(MySqlParser.AlterListItemContext ctx) {
        if (columnEditors != null) {
            columnEditors.forEach(columnEditor -> tableEditor.addColumn(columnEditor.create()));
            if (columnDefinitionListener != null) {
                listeners.remove(columnDefinitionListener);
            }
            columnEditors = null;
            parsingColumnIndex = STARTING_INDEX;
        }
    }

    private void handleEnterChangeColumn(MySqlParser.AlterListItemContext ctx) {
        // CHANGE COLUMN columnInternalRef identifier fieldDefinition
        String oldColumnName = parser.parseName(ctx.columnInternalRef().identifier());
        Column existingColumn = tableEditor.columnWithName(oldColumnName);
        if (existingColumn != null) {
            ColumnEditor columnEditor = existingColumn.edit();
            columnEditor.unsetDefaultValueExpression();
            columnEditor.unsetLength();
            if (columnEditor.scale().isPresent()) {
                columnEditor.unsetScale();
            }

            columnDefinitionListener = new ColumnDefinitionParserListener(tableEditor, columnEditor, parser, listeners);
            listeners.add(columnDefinitionListener);

            // Process fieldDefinition directly since there's no columnDefinition wrapper in ALTER TABLE
            if (ctx.fieldDefinition() != null) {
                columnDefinitionListener.processFieldDefinition(ctx.fieldDefinition());
            }
        }
        else {
            throw new ParsingException(null, "Trying to change column " + oldColumnName + " in "
                    + tableEditor.tableId().toString() + " table, which does not exist. Query: " + getText(ctx));
        }
    }

    private void handleExitChangeColumn(MySqlParser.AlterListItemContext ctx) {
        if (columnDefinitionListener == null) {
            return;
        }

        // Finalize column processing and get the column with all properties set
        Column column = columnDefinitionListener.finalizeColumn();
        tableEditor.addColumn(column);

        // The new column name is ctx.identifier() in CHANGE COLUMN oldName newName
        String newColumnName = parser.parseName(ctx.identifier());
        if (newColumnName != null && !column.name().equals(newColumnName)) {
            tableEditor.renameColumn(column.name(), newColumnName);
        }

        // Check for FIRST or AFTER
        if (ctx.place() != null) {
            if (ctx.place().FIRST_SYMBOL() != null) {
                tableEditor.reorderColumn(newColumnName, null);
            }
            else if (ctx.place().AFTER_SYMBOL() != null) {
                String afterColumn = parser.parseName(ctx.place().identifier());
                tableEditor.reorderColumn(newColumnName, afterColumn);
            }
        }

        listeners.remove(columnDefinitionListener);
        columnDefinitionListener = null;
    }

    private void handleEnterModifyColumn(MySqlParser.AlterListItemContext ctx) {
        // MODIFY COLUMN columnInternalRef fieldDefinition
        String columnName = parser.parseName(ctx.columnInternalRef().identifier());
        Column existingColumn = tableEditor.columnWithName(columnName);
        if (existingColumn != null) {
            ColumnEditor columnEditor = Column.editor().name(columnName);

            columnDefinitionListener = new ColumnDefinitionParserListener(tableEditor, columnEditor, parser, listeners);
            listeners.add(columnDefinitionListener);

            // Process fieldDefinition directly since there's no columnDefinition wrapper in ALTER TABLE
            if (ctx.fieldDefinition() != null) {
                columnDefinitionListener.processFieldDefinition(ctx.fieldDefinition());
            }
        }
        else {
            throw new ParsingException(null, "Trying to change column " + columnName + " in "
                    + tableEditor.tableId().toString() + " table, which does not exist. Query: " + getText(ctx));
        }
    }

    private void handleExitModifyColumn(MySqlParser.AlterListItemContext ctx) {
        if (columnDefinitionListener == null) {
            return;
        }

        // Finalize column processing and get the column with all properties set
        Column column = columnDefinitionListener.finalizeColumn();
        tableEditor.addColumn(column);

        // Check for FIRST or AFTER
        if (ctx.place() != null) {
            if (ctx.place().FIRST_SYMBOL() != null) {
                tableEditor.reorderColumn(column.name(), null);
            }
            else if (ctx.place().AFTER_SYMBOL() != null) {
                String afterColumn = parser.parseName(ctx.place().identifier());
                tableEditor.reorderColumn(column.name(), afterColumn);
            }
        }

        listeners.remove(columnDefinitionListener);
        columnDefinitionListener = null;
    }

    private void handleEnterDropColumn(MySqlParser.AlterListItemContext ctx) {
        // DROP COLUMN columnInternalRef
        String columnName = parser.parseName(ctx.columnInternalRef().identifier());
        tableEditor.removeColumn(columnName);
    }

    private void handleEnterDropPrimaryKey(MySqlParser.AlterListItemContext ctx) {
        // DROP PRIMARY KEY - clear the primary key names
        tableEditor.setPrimaryKeyNames();
    }

    private void handleEnterRenameTable(MySqlParser.AlterListItemContext ctx) {
        // RENAME (TO|AS)? tableName
        final TableId newTableId = parser.parseQualifiedTableId(ctx.tableName());
        parser.databaseTables().overwriteTable(tableEditor.create());
        parser.databaseTables().renameTable(tableEditor.tableId(), newTableId);
        tableEditor = parser.databaseTables().editTable(newTableId);
    }

    private void handleEnterAlterDefault(MySqlParser.AlterListItemContext ctx) {
        // ALTER COLUMN columnInternalRef (SET DEFAULT ... | DROP DEFAULT)
        String columnName = parser.parseName(ctx.columnInternalRef().identifier());
        Column column = tableEditor.columnWithName(columnName);
        if (column != null) {
            defaultValueColumnEditor = column.edit();
            if (ctx.SET_SYMBOL() != null && ctx.DEFAULT_SYMBOL() != null) {
                // Extract the default value directly from the context
                // Grammar: SET DEFAULT (exprWithParentheses | signedLiteralOrNull)
                if (ctx.signedLiteralOrNull() != null) {
                    String defaultValue = extractDefaultValue(ctx.signedLiteralOrNull());
                    if (defaultValue != null) {
                        defaultValueColumnEditor.defaultValueExpression(defaultValue);
                    }
                }
                else if (ctx.exprWithParentheses() != null) {
                    // Expression-based default - set to null
                    defaultValueColumnEditor.defaultValueExpression(null);
                }
            }
            else if (ctx.DROP_SYMBOL() != null && ctx.DEFAULT_SYMBOL() != null) {
                defaultValueColumnEditor.unsetDefaultValueExpression();
            }
        }
    }

    private String extractDefaultValue(MySqlParser.SignedLiteralOrNullContext ctx) {
        // Handle NULL
        if (ctx.nullAsLiteral() != null) {
            return null;
        }

        // Handle signedLiteral
        final var signed = ctx.signedLiteral();
        if (signed == null) {
            return null;
        }

        final var sign = extractSign(signed);

        // Handle signed number
        if (signed.ulong_number() != null) {
            return sign + signed.ulong_number().getText();
        }

        // Handle literal (text, numeric, temporal, etc.)
        if (signed.literal() != null) {
            return sign + extractLiteralValue(signed.literal());
        }

        return null;
    }

    private String extractSign(MySqlParser.SignedLiteralContext signed) {
        if (signed.PLUS_OPERATOR() != null) {
            return "+";
        }
        return signed.MINUS_OPERATOR() != null ? "-" : "";
    }

    private String extractLiteralValue(MySqlParser.LiteralContext literalCtx) {
        // Text literal
        if (literalCtx.textLiteral() != null) {
            String text = literalCtx.textLiteral().getText();
            return unquote(text);
        }

        // Numeric literal
        if (literalCtx.numLiteral() != null) {
            return literalCtx.numLiteral().getText();
        }

        // Boolean literal
        if (literalCtx.boolLiteral() != null) {
            return literalCtx.boolLiteral().getText();
        }

        // Temporal literal
        if (literalCtx.temporalLiteral() != null) {
            return literalCtx.temporalLiteral().getText();
        }

        return null;
    }

    private String unquote(String stringLiteral) {
        if (stringLiteral != null && ((stringLiteral.startsWith("'") && stringLiteral.endsWith("'"))
                || (stringLiteral.startsWith("\"") && stringLiteral.endsWith("\"")))) {
            return stringLiteral.substring(1, stringLiteral.length() - 1);
        }
        return stringLiteral;
    }

    private void handleExitAlterDefault(MySqlParser.AlterListItemContext ctx) {
        if (defaultValueColumnEditor == null) {
            return;
        }

        tableEditor.updateColumn(defaultValueColumnEditor.create());
        defaultValueColumnEditor = null;
    }

    private void handleEnterRenameColumn(MySqlParser.AlterListItemContext ctx) {
        // RENAME COLUMN columnInternalRef TO identifier
        String oldColumnName = parser.parseName(ctx.columnInternalRef().identifier());
        Column existingColumn = tableEditor.columnWithName(oldColumnName);
        if (existingColumn != null) {
            ColumnEditor columnEditor = existingColumn.edit();

            columnDefinitionListener = new ColumnDefinitionParserListener(tableEditor, columnEditor, parser, listeners);
            listeners.add(columnDefinitionListener);
        }
        else {
            throw new ParsingException(null, "Trying to change column " + oldColumnName + " in "
                    + tableEditor.tableId().toString() + " table, which does not exist. Query: " + getText(ctx));
        }
    }

    private void handleExitRenameColumn(MySqlParser.AlterListItemContext ctx) {
        if (columnDefinitionListener == null) {
            return;
        }

        Column column = columnDefinitionListener.getColumn();
        tableEditor.addColumn(column);

        // Find the new column name - it's ctx.identifier() in RENAME COLUMN old TO new
        if (ctx.identifier() != null) {
            String newColumnName = parser.parseName(ctx.identifier());
            if (newColumnName != null && !column.name().equals(newColumnName)) {
                tableEditor.renameColumn(column.name(), newColumnName);
            }
        }

        listeners.remove(columnDefinitionListener);
        columnDefinitionListener = null;
    }

    private void handleEnterConvertCharset(MySqlParser.AlterListItemContext ctx) {
        // CONVERT TO CHARSET charsetName
        String charsetName = null;
        if (ctx.charsetName() != null) {
            charsetName = parser.extractCharset(ctx.charsetName(), ctx.collate() != null ? ctx.collate().collationName() : null);
        }

        if (charsetName != null) {
            final String finalCharsetName = charsetName;
            tableEditor.setDefaultCharsetName(finalCharsetName);
            tableEditor.setColumns(tableEditor.columns().stream()
                    .map(column -> {
                        final ColumnEditor columnEditor = column.edit();
                        if (columnEditor.charsetName() != null) {
                            columnEditor.charsetNameOfTable(finalCharsetName);
                            columnEditor.charsetName(finalCharsetName);
                        }
                        return columnEditor;
                    })
                    .map(ColumnEditor::create)
                    .collect(Collectors.toList()));
        }
    }

    @Override
    public void enterCreateTableOption(MySqlParser.CreateTableOptionContext ctx) {
        // Handle table options like COMMENT
        if (!parser.skipComments() && ctx.option != null && ctx.option.getType() == MySqlParser.COMMENT_SYMBOL) {
            parser.runIfNotNull(() -> {
                if (ctx.textStringLiteral() != null) {
                    tableEditor.setComment(parser.withoutQuotes(ctx.textStringLiteral().getText()));
                }
            }, tableEditor);
        }
        super.enterCreateTableOption(ctx);
    }
}
