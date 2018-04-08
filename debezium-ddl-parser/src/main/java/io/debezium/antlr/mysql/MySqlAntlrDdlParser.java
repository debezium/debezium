/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.antlr.mysql;

import io.debezium.antlr.AntlrDdlParser;
import io.debezium.antlr.DataTypeResolver;
import io.debezium.antlr.DataTypeResolver.DataTypeEntry;
import io.debezium.antlr.ProxyParseTreeListener;
import io.debezium.ddl.parser.mysql.generated.MySqlLexer;
import io.debezium.ddl.parser.mysql.generated.MySqlParser;
import io.debezium.ddl.parser.mysql.generated.MySqlParserBaseListener;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.Table;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;
import io.debezium.text.ParsingException;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.misc.Interval;
import org.antlr.v4.runtime.tree.ParseTree;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Roman Kuchár <kucharrom@gmail.com>.
 */
public class MySqlAntlrDdlParser extends AntlrDdlParser<MySqlLexer, MySqlParser> {

    private TableEditor tableEditor;
    private ColumnEditor columnEditor;

    public MySqlAntlrDdlParser() {
        super();
        systemVariables = new MySqlSystemVariables();
    }

    @Override
    protected ParseTree parseTree(MySqlParser parser) {
        return parser.root();
    }

    @Override
    protected void assignParserListeners(ProxyParseTreeListener proxyParseTreeListener) {
        proxyParseTreeListener.add(new CreateTableParserListener());
        proxyParseTreeListener.add(new DropTableParserListener());
        proxyParseTreeListener.add(new AlterTableParserListener());
        proxyParseTreeListener.add(new ColumnDefinitionParserListener());
        proxyParseTreeListener.add(new RenameTableParserListener());
//        proxyParseTreeListener.add(new TruncateTableParserListener());
        proxyParseTreeListener.add(new CreateUniqueIndexParserListener());
        proxyParseTreeListener.add(new FinishSqlStatementParserListener());
    }

    @Override
    protected MySqlLexer createNewLexerInstance(CharStream charStreams) {
        return new MySqlLexer(charStreams);
    }

    @Override
    protected MySqlParser createNewParserInstance(CommonTokenStream commonTokenStream) {
        return new MySqlParser(commonTokenStream);
    }

    @Override
    protected boolean isGrammarInUpperCase() {
        return true;
    }

    @Override
    protected void initDataTypes(DataTypeResolver dataTypeResolver) {
        // TODO: solve data types that are not in provided lexer
//        dataTypes.register(Types.DOUBLE, "DOUBLE PRECISION[(M[,D])] [UNSIGNED|SIGNED] [ZEROFILL]");
//        dataTypes.register(Types.NVARCHAR, "NVARCHAR(L)");
//        dataTypes.register(Types.NVARCHAR, "NATIONAL VARCHAR(L)");
//        dataTypes.register(Types.NVARCHAR, "NCHAR VARCHAR(L)");
//        dataTypes.register(Types.NVARCHAR, "NATIONAL CHARACTER VARYING(L)");
//        dataTypes.register(Types.NVARCHAR, "NATIONAL CHAR VARYING(L)");
//        dataTypes.register(Types.NCHAR, "NCHAR[(L)]");
//        dataTypes.register(Types.NCHAR, "NATIONAL CHARACTER(L)");
//        dataTypes.register(Types.BLOB, "TINYTEXT BINARY");
//        dataTypes.register(Types.BLOB, "TEXT BINARY");
//        dataTypes.register(Types.BLOB, "MEDIUMTEXT BINARY");
//        dataTypes.register(Types.BLOB, "LONGTEXT BINARY");
//        dataTypes.register(Types.OTHER, "JSON");
//        dataTypes.register(Types.OTHER, "GEOMETRY");
        dataTypeResolver.registerDataTypes(MySqlParser.StringDataTypeContext.class.getCanonicalName(), Arrays.asList(
                new DataTypeEntry(MySqlParser.CHAR, Types.BINARY),
                new DataTypeEntry(MySqlParser.VARCHAR, Types.VARCHAR),
                new DataTypeEntry(MySqlParser.TINYTEXT, Types.BLOB),
                new DataTypeEntry(MySqlParser.TEXT, Types.BLOB),
                new DataTypeEntry(MySqlParser.MEDIUMTEXT, Types.BLOB),
                new DataTypeEntry(MySqlParser.LONGTEXT, Types.BLOB)
        ));
        dataTypeResolver.registerDataTypes(MySqlParser.DimensionDataTypeContext.class.getCanonicalName(), Arrays.asList(
                new DataTypeEntry(MySqlParser.TINYINT, Types.SMALLINT),
                new DataTypeEntry(MySqlParser.SMALLINT, Types.SMALLINT),
                new DataTypeEntry(MySqlParser.MEDIUMINT, Types.INTEGER),
                new DataTypeEntry(MySqlParser.INT, Types.INTEGER),
                new DataTypeEntry(MySqlParser.INTEGER, Types.INTEGER),
                new DataTypeEntry(MySqlParser.BIGINT, Types.BIGINT),
                new DataTypeEntry(MySqlParser.REAL, Types.REAL),
                new DataTypeEntry(MySqlParser.DOUBLE, Types.DOUBLE),
                new DataTypeEntry(MySqlParser.FLOAT, Types.FLOAT),
                new DataTypeEntry(MySqlParser.DECIMAL, Types.DECIMAL),
                new DataTypeEntry(MySqlParser.DEC, Types.DECIMAL),
                new DataTypeEntry(MySqlParser.FIXED, Types.DECIMAL),
                new DataTypeEntry(MySqlParser.NUMERIC, Types.NUMERIC),
                new DataTypeEntry(MySqlParser.BIT, Types.BIT),
                new DataTypeEntry(MySqlParser.TIME, Types.TIME),
                new DataTypeEntry(MySqlParser.TIMESTAMP, Types.TIME_WITH_TIMEZONE),
                new DataTypeEntry(MySqlParser.DATETIME, Types.TIMESTAMP),
                new DataTypeEntry(MySqlParser.BINARY, Types.BINARY),
                new DataTypeEntry(MySqlParser.VARBINARY, Types.VARBINARY),
                new DataTypeEntry(MySqlParser.YEAR, Types.INTEGER)
        ));
        dataTypeResolver.registerDataTypes(MySqlParser.SimpleDataTypeContext.class.getCanonicalName(), Arrays.asList(
                new DataTypeEntry(MySqlParser.DATE, Types.DATE),
                new DataTypeEntry(MySqlParser.TINYBLOB, Types.BLOB),
                new DataTypeEntry(MySqlParser.BLOB, Types.BLOB),
                new DataTypeEntry(MySqlParser.MEDIUMBLOB, Types.BLOB),
                new DataTypeEntry(MySqlParser.LONGBLOB, Types.BLOB),
                new DataTypeEntry(MySqlParser.BOOL, Types.BOOLEAN),
                new DataTypeEntry(MySqlParser.BOOLEAN, Types.BOOLEAN)
        ));
        dataTypeResolver.registerDataTypes(MySqlParser.CollectionDataTypeContext.class.getCanonicalName(), Arrays.asList(
                new DataTypeEntry(MySqlParser.ENUM, Types.CHAR),
                new DataTypeEntry(MySqlParser.SET, Types.CHAR)
        ));
        dataTypeResolver.registerDataTypes(MySqlParser.SpatialDataTypeContext.class.getCanonicalName(), Arrays.asList(
                new DataTypeEntry(MySqlParser.GEOMETRYCOLLECTION, Types.OTHER),
                new DataTypeEntry(MySqlParser.LINESTRING, Types.OTHER),
                new DataTypeEntry(MySqlParser.MULTILINESTRING, Types.OTHER),
                new DataTypeEntry(MySqlParser.MULTIPOINT, Types.OTHER),
                new DataTypeEntry(MySqlParser.MULTIPOLYGON, Types.OTHER),
                new DataTypeEntry(MySqlParser.POINT, Types.OTHER),
                new DataTypeEntry(MySqlParser.POLYGON, Types.OTHER)
        ));
    }

    private TableId parseQualifiedTableId(MySqlParser.TableNameContext tableNameContext) {
        String fullTableName = tableNameContext.fullId().getText();
        int dotIndex;
        if ((dotIndex = fullTableName.indexOf(".")) > 0) {
            return resolveTableId(fullTableName.substring(0, dotIndex),
                    fullTableName.substring(dotIndex + 1, fullTableName.length()));
        }
        else {
            return resolveTableId(currentSchema(), fullTableName);
        }
    }

    private String parseColumnName(MySqlParser.UidContext uidContext) {
        return uidContext.getText();
    }

    private String getFullTableName(TableId tableId) {
        if (tableId.catalog() != null) {
            return tableId.catalog() + "." + tableId.table();
        }
        else {
            return tableId.table();
        }
    }

    private void resolveColumnDataType(MySqlParser.DataTypeContext dataTypeContext) {
        String dataTypeName;
        if (dataTypeContext instanceof MySqlParser.StringDataTypeContext) {
            // CHAR | VARCHAR | TINYTEXT | TEXT | MEDIUMTEXT | LONGTEXT
            MySqlParser.StringDataTypeContext stringDataTypeContext = (MySqlParser.StringDataTypeContext) dataTypeContext;
            dataTypeName = stringDataTypeContext.typeName.getText();

            if (stringDataTypeContext.lengthOneDimension() != null) {
                Integer length = Integer.valueOf(stringDataTypeContext.lengthOneDimension().decimalLiteral().getText());
                columnEditor.length(length);
            }
        }
        else if (dataTypeContext instanceof MySqlParser.DimensionDataTypeContext) {
            // TINYINT | SMALLINT | MEDIUMINT | INT | INTEGER | BIGINT
            // REAL | DOUBLE | FLOAT
            // DECIMAL | NUMERIC | DEC | FIXED
            // BIT | TIME | TIMESTAMP | DATETIME | BINARY | VARBINARY | YEAR
            MySqlParser.DimensionDataTypeContext dimensionDataTypeContext = (MySqlParser.DimensionDataTypeContext) dataTypeContext;
            dataTypeName = dimensionDataTypeContext.typeName.getText();

            Integer length = null;
            Integer scale = null;
            if (dimensionDataTypeContext.lengthOneDimension() != null) {
                length = Integer.valueOf(dimensionDataTypeContext.lengthOneDimension().decimalLiteral().getText());
            }

            if (dimensionDataTypeContext.lengthTwoDimension() != null) {
                List<MySqlParser.DecimalLiteralContext> decimalLiterals = dimensionDataTypeContext.lengthTwoDimension().decimalLiteral();
                length = Integer.valueOf(decimalLiterals.get(0).getText());
                scale = Integer.valueOf(decimalLiterals.get(1).getText());
            }

            if (dimensionDataTypeContext.lengthTwoOptionalDimension() != null) {
                List<MySqlParser.DecimalLiteralContext> decimalLiterals = dimensionDataTypeContext.lengthTwoOptionalDimension().decimalLiteral();
                length = Integer.valueOf(decimalLiterals.get(0).getText());

                if (decimalLiterals.size() > 1) {
                    scale = Integer.valueOf(decimalLiterals.get(1).getText());
                }
            }
            if (length != null) {
                columnEditor.length(length);
            }
            if (scale != null) {
                columnEditor.scale(scale);
            }
        }
        else if (dataTypeContext instanceof MySqlParser.SimpleDataTypeContext) {
            // DATE | TINYBLOB | BLOB | MEDIUMBLOB | LONGBLOB | BOOL | BOOLEAN
            dataTypeName = ((MySqlParser.SimpleDataTypeContext) dataTypeContext).typeName.getText();
        }
        else if (dataTypeContext instanceof MySqlParser.CollectionDataTypeContext) {
            // ENUM | SET
            // do not care about charsetName or collationName
            dataTypeName = ((MySqlParser.CollectionDataTypeContext) dataTypeContext).typeName.getText();
        }
        else if (dataTypeContext instanceof MySqlParser.SpatialDataTypeContext) {
            // GEOMETRYCOLLECTION | LINESTRING | MULTILINESTRING | MULTIPOINT | MULTIPOLYGON | POINT | POLYGON
            dataTypeName = ((MySqlParser.SpatialDataTypeContext) dataTypeContext).typeName.getText();
        }
        else {
            throw new IllegalStateException("Not recognized instance of data type context for " + dataTypeContext.getText());
        }

        columnEditor.type(dataTypeName);
        columnEditor.jdbcType(dataTypeResolver.resolveDataType(dataTypeContext));
    }

    private void parsePrimaryIndexColumnNames(MySqlParser.IndexColumnNamesContext indexColumnNamesContext) {
        List<String> pkColumnNames = indexColumnNamesContext.indexColumnName().stream()
                .map(indexColumnNameContext -> {
                    // MySQL does not allow a primary key to have nullable columns, so let's make sure we model that correctly ...
                    String columnName = parseColumnName(indexColumnNameContext.uid());
                    Column column = tableEditor.columnWithName(columnName);
                    if (column != null && column.isOptional()) {
                        tableEditor.addColumn(column.edit().optional(false).create());
                    }
                    return columnName;
                })
                .collect(Collectors.toList());

        tableEditor.setPrimaryKeyNames(pkColumnNames);
    }

    /**
     * Parser listener for MySQL create table queries.
     */
    private class CreateTableParserListener extends MySqlParserBaseListener {

        @Override
        public void enterColumnCreateTable(MySqlParser.ColumnCreateTableContext ctx) {
            TableId tableId = parseQualifiedTableId(ctx.tableName());
            tableEditor = databaseTables.editOrCreateTable(tableId);
            super.enterColumnCreateTable(ctx);
        }

        @Override
        public void exitColumnCreateTable(MySqlParser.ColumnCreateTableContext ctx) {
            databaseTables.overwriteTable(tableEditor.create());
            signalCreateTable(tableEditor.tableId(), ctx);
            super.exitColumnCreateTable(ctx);
        }

        @Override
        public void exitCopyCreateTable(MySqlParser.CopyCreateTableContext ctx) {
            TableId tableId = parseQualifiedTableId(ctx.tableName(0));
            TableId originalTableId = parseQualifiedTableId(ctx.tableName(1));
            Table original = databaseTables.forTable(originalTableId);
            if (original != null) {
                databaseTables.overwriteTable(tableId, original.columns(), original.primaryKeyColumnNames(), original.defaultCharsetName());
                signalCreateTable(tableId, ctx);
            }
            super.exitCopyCreateTable(ctx);
        }

        @Override
        public void enterColumnDeclaration(MySqlParser.ColumnDeclarationContext ctx) {
            String columnName = parseColumnName(ctx.uid());
            columnEditor = Column.editor().name(columnName);
            super.enterColumnDeclaration(ctx);
        }

        @Override
        public void exitColumnDeclaration(MySqlParser.ColumnDeclarationContext ctx) {
            tableEditor.addColumn(columnEditor.create());
            columnEditor = null;
            super.exitColumnDeclaration(ctx);
        }

        @Override
        public void enterPrimaryKeyTableConstraint(MySqlParser.PrimaryKeyTableConstraintContext ctx) {
            parsePrimaryIndexColumnNames(ctx.indexColumnNames());
            super.enterPrimaryKeyTableConstraint(ctx);
        }

        @Override
        public void enterUniqueKeyTableConstraint(MySqlParser.UniqueKeyTableConstraintContext ctx) {
            if (!tableEditor.hasPrimaryKey()) {
                parsePrimaryIndexColumnNames(ctx.indexColumnNames());
            }
            super.enterUniqueKeyTableConstraint(ctx);
        }
    }

    /**
     * Parser listener for MySQL drop table queries.
     */
    private class DropTableParserListener extends MySqlParserBaseListener {

        @Override
        public void enterDropTable(MySqlParser.DropTableContext ctx) {
            Interval interval = new Interval(ctx.start.getStartIndex(), ctx.tables().start.getStartIndex() - 1);
            String prefix = ctx.start.getInputStream().getText(interval);
            ctx.tables().tableName().forEach(tableNameContext -> {
                TableId tableId = parseQualifiedTableId(tableNameContext);
                databaseTables.removeTable(tableId);
                signalDropTable(tableId, prefix + tableId.table()
                        + (ctx.dropType != null ? " " + ctx.dropType.getText() : ""));
            });
            super.enterDropTable(ctx);
        }
    }

    /**
     * Parser listener for MySQL alter table queries.
     */
    private class AlterTableParserListener extends MySqlParserBaseListener {

        private static final int STARTING_INDEX = 1;

        private int parsingColumnIndex = STARTING_INDEX;
        private List<ColumnEditor> columnEditors;

        @Override
        public void enterAlterTable(MySqlParser.AlterTableContext ctx) {
            TableId tableId = parseQualifiedTableId(ctx.tableName());
            tableEditor = databaseTables.editTable(tableId);
            if (tableEditor == null) {
                throw new ParsingException(null, "Trying to alter table " + getFullTableName(tableId)
                        + ", which does not exists.");
            }
            super.enterAlterTable(ctx);
        }

        @Override
        public void exitAlterTable(MySqlParser.AlterTableContext ctx) {
            runIfTableEditorNotNull(() -> {
                databaseTables.overwriteTable(tableEditor.create());
                signalAlterTable(tableEditor.tableId(), null, ctx.getParent());
            });
            super.exitAlterTable(ctx);
        }

        @Override
        public void enterAlterByAddColumn(MySqlParser.AlterByAddColumnContext ctx) {
            runIfTableEditorNotNull(() -> {
                String columnName = parseColumnName(ctx.uid(0));
                columnEditor = Column.editor().name(columnName);
            });
            super.exitAlterByAddColumn(ctx);
        }

        @Override
        public void exitAlterByAddColumn(MySqlParser.AlterByAddColumnContext ctx) {
            runIfAllEditorsNotNull(() -> {
                String columnName = columnEditor.name();
                tableEditor.addColumn(columnEditor.create());

                if (ctx.FIRST() != null) {
                    tableEditor.reorderColumn(columnName, null);
                }
                else if (ctx.AFTER() != null) {
                    String afterColumn = parseColumnName(ctx.uid(1));
                    tableEditor.reorderColumn(columnName, afterColumn);
                }
            });
            super.exitAlterByAddColumn(ctx);
        }

        @Override
        public void enterAlterByAddColumns(MySqlParser.AlterByAddColumnsContext ctx) {
            // multiple columns are added. Initialize a list of column editors for them
            runIfTableEditorNotNull(() -> {
                columnEditors = new ArrayList<>(ctx.uid().size());
                for (MySqlParser.UidContext uidContext : ctx.uid()) {
                    String columnName = parseColumnName(uidContext);
                    columnEditors.add(Column.editor().name(columnName));
                }
                columnEditor = columnEditors.get(0);
            });
            super.enterAlterByAddColumns(ctx);
        }

        @Override
        public void exitColumnDefinition(MySqlParser.ColumnDefinitionContext ctx) {
            runIfAllEditorsNotNull(() -> {
                if (columnEditors != null) {
                    // column editor list is not null when a multiple columns are parsed in one statement
                    if (columnEditors.size() > parsingColumnIndex) {
                        // assign next column editor to parse another column definition
                        columnEditor = columnEditors.get(parsingColumnIndex++);
                    }
                    else {
                        // all columns parsed
                        // reset global variables for next parsed statement
                        columnEditors = null;
                        parsingColumnIndex = STARTING_INDEX;
                    }
                }
            });
            super.exitColumnDefinition(ctx);
        }

        @Override
        public void exitAlterByAddColumns(MySqlParser.AlterByAddColumnsContext ctx) {
            runIfAllEditorsNotNull(() -> columnEditors.forEach(columnEditor -> tableEditor.addColumn(columnEditor.create())));
            super.exitAlterByAddColumns(ctx);
        }

        @Override
        public void enterAlterByChangeColumn(MySqlParser.AlterByChangeColumnContext ctx) {
            runIfTableEditorNotNull(() -> {
                String oldColumnName = parseColumnName(ctx.oldColumn);
                Column existingColumn = tableEditor.columnWithName(oldColumnName);
                if (existingColumn != null) {
                    columnEditor = existingColumn.edit();
                }
                else {
                    throw new ParsingException(null, "Trying to change column " + oldColumnName + " in "
                            + getFullTableName(tableEditor.tableId()) + " table, which does not exists.");
                }
            });
            super.enterAlterByChangeColumn(ctx);
        }

        @Override
        public void exitAlterByChangeColumn(MySqlParser.AlterByChangeColumnContext ctx) {
            runIfAllEditorsNotNull(() -> {
                tableEditor.addColumn(columnEditor.create());
                String newColumnName = parseColumnName(ctx.newColumn);
                tableEditor.renameColumn(columnEditor.name(), newColumnName);

                if (ctx.FIRST() != null) {
                    tableEditor.reorderColumn(newColumnName, null);
                }
                else if (ctx.afterColumn != null) {
                    tableEditor.reorderColumn(newColumnName, parseColumnName(ctx.afterColumn));
                }
            });
            super.exitAlterByChangeColumn(ctx);
        }

        @Override
        public void enterAlterByModifyColumn(MySqlParser.AlterByModifyColumnContext ctx) {
            runIfTableEditorNotNull(() -> {
                String columnName = parseColumnName(ctx.uid(0));
                Column column = tableEditor.columnWithName(columnName);
                if (column != null) {
                    columnEditor = column.edit();
                }
                else {
                    throw new ParsingException(null, "Trying to change column " + columnName + " in "
                            + getFullTableName(tableEditor.tableId()) + " table, which does not exists.");
                }
            });
            super.enterAlterByModifyColumn(ctx);
        }

        @Override
        public void exitAlterByModifyColumn(MySqlParser.AlterByModifyColumnContext ctx) {
            runIfAllEditorsNotNull(() -> {
                tableEditor.addColumn(columnEditor.create());

                if (ctx.FIRST() != null) {
                    tableEditor.reorderColumn(columnEditor.name(), null);
                }
                else if (ctx.AFTER() != null) {
                    String afterColumn = parseColumnName(ctx.uid(1));
                    tableEditor.reorderColumn(columnEditor.name(), afterColumn);
                }
            });
            super.exitAlterByModifyColumn(ctx);
        }

        @Override
        public void enterAlterByDropColumn(MySqlParser.AlterByDropColumnContext ctx) {
            runIfTableEditorNotNull(() -> tableEditor.removeColumn(parseColumnName(ctx.uid())));
            super.enterAlterByDropColumn(ctx);
        }

        @Override
        public void enterAlterByRename(MySqlParser.AlterByRenameContext ctx) {
            runIfTableEditorNotNull(() -> {
                TableId newTableId = resolveTableId(currentSchema(), ctx.uid().getText());
                databaseTables.renameTable(tableEditor.tableId(), newTableId);
            });
            super.enterAlterByRename(ctx);
        }

        @Override
        public void enterAlterByChangeDefault(MySqlParser.AlterByChangeDefaultContext ctx) {
            runIfTableEditorNotNull(() -> {
                String columnName = parseColumnName(ctx.uid());
                Column column = tableEditor.columnWithName(columnName);
                if (column != null) {
                    ColumnEditor columnEditor = column.edit();
                    columnEditor.generated(ctx.DROP() != null);
                }
            });
            super.enterAlterByChangeDefault(ctx);
        }

        @Override
        public void enterAlterByAddPrimaryKey(MySqlParser.AlterByAddPrimaryKeyContext ctx) {
            runIfTableEditorNotNull(() -> parsePrimaryIndexColumnNames(ctx.indexColumnNames()));
            super.enterAlterByAddPrimaryKey(ctx);
        }

        @Override
        public void enterAlterByDropPrimaryKey(MySqlParser.AlterByDropPrimaryKeyContext ctx) {
            runIfTableEditorNotNull(() -> tableEditor.setPrimaryKeyNames(new ArrayList<>()));
            super.enterAlterByDropPrimaryKey(ctx);
        }

        @Override
        public void enterAlterByAddUniqueKey(MySqlParser.AlterByAddUniqueKeyContext ctx) {
            runIfTableEditorNotNull(() -> {
                if (!tableEditor.hasPrimaryKey()) {
                    // this may eventually get overwritten by a real PK
                    parsePrimaryIndexColumnNames(ctx.indexColumnNames());
                }
            });
            super.enterAlterByAddUniqueKey(ctx);
        }
    }

    /**
     * Parser listener for MySQL column definition queries.
     */
    private class ColumnDefinitionParserListener extends MySqlParserBaseListener {

        @Override
        public void enterColumnDefinition(MySqlParser.ColumnDefinitionContext ctx) {
            runIfAllEditorsNotNull(() -> {
                resolveColumnDataType(ctx.dataType());
            });
            super.enterColumnDefinition(ctx);
        }

        @Override
        public void enterUniqueKeyColumnConstraint(MySqlParser.UniqueKeyColumnConstraintContext ctx) {
            runIfAllEditorsNotNull(() -> {
                if (!tableEditor.hasPrimaryKey()) {
                    // take the first unique constrain if no primary key is set
                    tableEditor.setPrimaryKeyNames(columnEditor.name());
                }
            });
            super.enterUniqueKeyColumnConstraint(ctx);
        }

        @Override
        public void enterPrimaryKeyColumnConstraint(MySqlParser.PrimaryKeyColumnConstraintContext ctx) {
            runIfAllEditorsNotNull(() -> {
                // this rule will be parsed only if no primary key is set in a table
                // otherwise the statement can't be executed due to multiple primary key error
                columnEditor.optional(false);
                tableEditor.setPrimaryKeyNames(columnEditor.name());
            });
            super.enterPrimaryKeyColumnConstraint(ctx);
        }

        @Override
        public void enterNullNotnull(MySqlParser.NullNotnullContext ctx) {
            runIfAllEditorsNotNull(() -> columnEditor.optional(ctx.NOT() == null));

            super.enterNullNotnull(ctx);
        }

        @Override
        public void enterDefaultColumnConstraint(MySqlParser.DefaultColumnConstraintContext ctx) {
            runIfAllEditorsNotNull(() -> columnEditor.generated(true));
            super.enterDefaultColumnConstraint(ctx);
        }

        @Override
        public void enterAutoIncrementColumnConstraint(MySqlParser.AutoIncrementColumnConstraintContext ctx) {
            runIfAllEditorsNotNull(() -> {
                columnEditor.autoIncremented(true);
                columnEditor.generated(true);
            });
            super.enterAutoIncrementColumnConstraint(ctx);
        }
    }

    /**
     * Parser listener for MySQL rename table queries.
     */
    private class RenameTableParserListener extends MySqlParserBaseListener {

        @Override
        public void enterRenameTableClause(MySqlParser.RenameTableClauseContext ctx) {
            TableId oldTable = parseQualifiedTableId(ctx.tableName(0));
            TableId newTable = parseQualifiedTableId(ctx.tableName(1));
            databaseTables.renameTable(oldTable, newTable);
            signalAlterTable(newTable, oldTable, ctx);
            super.enterRenameTableClause(ctx);
        }
    }

    /**
     * Parser listener for MySQL truncate table queries.
     */
    private class TruncateTableParserListener extends MySqlParserBaseListener {

        @Override
        public void enterTruncateTable(MySqlParser.TruncateTableContext ctx) {
            //TODO how to react for this type of query?
            TableId tableId = parseQualifiedTableId(ctx.tableName());
            super.enterTruncateTable(ctx);
        }
    }

    /**
     * Parser listener for MySQL create unique index sql statement.
     */
    private class CreateUniqueIndexParserListener extends MySqlParserBaseListener {

        @Override
        public void enterCreateIndex(MySqlParser.CreateIndexContext ctx) {
            if (ctx.UNIQUE() != null) {
                TableId tableId = parseQualifiedTableId(ctx.tableName());
                tableEditor = databaseTables.editTable(tableId);
                if (tableEditor != null) {
                    if (!tableEditor.hasPrimaryKey()) {
                        parsePrimaryIndexColumnNames(ctx.indexColumnNames());
                        signalAlterTable(tableId, null, ctx);
                    }
                }
                else {
                    throw new ParsingException(null, "Trying to create index on non existing table " + getFullTableName(tableId));
                }
            }
            super.enterCreateIndex(ctx);
        }
    }

    /**
     * Parser listener for MySQL finishing parsing of one sql statement.
     */
    private class FinishSqlStatementParserListener extends MySqlParserBaseListener {

        @Override
        public void exitSqlStatement(MySqlParser.SqlStatementContext ctx) {
            if (tableEditor != null) {
                // reset global values for next statement that could be parsed with this instance
                tableEditor = null;
                columnEditor = null;
                debugParsed(ctx);
            }
            else {
                // if table editor was not set, then nothing was parsed
                debugSkipped(ctx);
            }
            super.exitSqlStatement(ctx);
        }
    }

    /**
     * Runs the function if {@link MySqlAntlrDdlParser#tableEditor} is not null.
     *
     * @param function function to run.
     */
    private void runIfTableEditorNotNull(Runnable function) {
        if (tableEditor != null) {
            function.run();
        }
    }

    /**
     * Runs the function if {@link MySqlAntlrDdlParser#tableEditor} and {@link MySqlAntlrDdlParser#columnEditor} is not null.
     *
     * @param function function to run.
     */
    private void runIfAllEditorsNotNull(Runnable function) {
        if (tableEditor != null && columnEditor != null) {
            function.run();
        }
    }

}
