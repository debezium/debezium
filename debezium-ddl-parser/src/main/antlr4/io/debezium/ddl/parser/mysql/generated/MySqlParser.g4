parser grammar MySqlParser;

/*
 * Copyright © 2025, Oracle and/or its affiliates
 */

/*
 * Merged in all changes up to mysql-trunk git revision [d2c9971] (24. January 2024).
 *
 * MySQL grammar for ANTLR 4.5+ with language features from MySQL 8.0 and up.
 * The server version in the generated parser can be switched at runtime, making it so possible
 * to switch the supported feature set dynamically.
 *
 * The coverage of the MySQL language should be 100%, but there might still be bugs or omissions.
 *
 * To use this grammar you will need a few support classes (which should be close to where you found this grammar).
 * These classes implement the target specific action code, so we don't clutter the grammar with that
 * and make it simpler to adjust it for other targets. See the demo/test project for further details.
 *
 * Written by Mike Lischke. Direct all bug reports, omissions etc. to mike.lischke@oracle.com.
 */

//----------------------------------------------------------------------------------------------------------------------

// $antlr-format alignTrailingComments on, columnLimit 130, minEmptyLines 1, maxEmptyLinesToKeep 1, reflowComments off
// $antlr-format useTab off, allowShortRulesOnASingleLine off, allowShortBlocksOnASingleLine on, alignSemicolons hanging
// $antlr-format alignColons hanging

options {
    superClass = MySqlParserBase;
    tokenVocab = MySqlLexer;
}

@header {
    import io.debezium.antlr.mysql.MySqlParserBase;
}

//----------------------------------------------------------------------------------------------------------------------

queries
    : query* EOF
    ;

query
    : (simpleStatement | beginWork) SEMICOLON_SYMBOL?
    ;

simpleStatement
    :
    // DDL
    alterStatement
    | createStatement
    | dropStatement
    | renameTableStatement
    | truncateTableStatement
    | importStatement

    // DML
    | callStatement
    | deleteStatement
    | doStatement
    | handlerStatement
    | insertStatement
    | loadStatement
    | replaceStatement
    | selectStatement
    | updateStatement
    | transactionOrLockingStatement
    | replicationStatement
    | preparedStatement

    // Data Directory
    | cloneStatement

    // Database administration
    | accountManagementStatement
    | tableAdministrationStatement
    | uninstallStatement
    | installStatement
    | setStatement // SET PASSWORD is handled in accountManagementStatement.
    | showDatabasesStatement
    | showTablesStatement
    | showTriggersStatement
    | showEventsStatement
    | showTableStatusStatement
    | showOpenTablesStatement
    | showParseTreeStatement
    | showPluginsStatement
    | showEngineLogsStatement
    | showEngineMutexStatement
    | showEngineStatusStatement
    | showColumnsStatement
    | showBinaryLogsStatement
    | showBinaryLogStatusStatement
    | showReplicasStatement
    | showBinlogEventsStatement
    | showRelaylogEventsStatement
    | showKeysStatement
    | showEnginesStatement
    | showCountWarningsStatement
    | showCountErrorsStatement
    | showWarningsStatement
    | showErrorsStatement
    | showProfilesStatement
    | showProfileStatement
    | showStatusStatement
    | showProcessListStatement
    | showVariablesStatement
    | showCharacterSetStatement
    | showCollationStatement
    | showPrivilegesStatement
    | showGrantsStatement
    | showCreateDatabaseStatement
    | showCreateTableStatement
    | showCreateViewStatement
    | showMasterStatusStatement
    | showReplicaStatusStatement
    | showCreateProcedureStatement
    | showCreateFunctionStatement
    | showCreateTriggerStatement
    | showCreateProcedureStatusStatement
    | showCreateFunctionStatusStatement
    | showCreateProcedureCodeStatement
    | showCreateFunctionCodeStatement
    | showCreateEventStatement
    | showCreateUserStatement
    | resourceGroupManagement
    | otherAdministrativeStatement

    // MySQL utility statements
    | utilityStatement
    | getDiagnosticsStatement
    | signalStatement
    | resignalStatement
    ;

//----------------- DDL statements -------------------------------------------------------------------------------------

alterStatement
    : ALTER_SYMBOL (
        alterTable
        | alterDatabase
        | PROCEDURE_SYMBOL procedureRef routineAlterOptions?
        | FUNCTION_SYMBOL functionRef routineAlterOptions?
        | alterView
        | alterEvent
        | alterTablespace
        | {this.isServerVersionGe80014()}? alterUndoTablespace
        | alterLogfileGroup
        | alterServer
        // ALTER USER is part of the user management rule.
        | alterInstanceStatement
    )
    ;

alterDatabase
    : DATABASE_SYMBOL schemaRef alterDatabaseOption+
    ;

alterDatabaseOption
    : createDatabaseOption
    | READ_SYMBOL ONLY_SYMBOL EQUAL_OPERATOR? ternaryOption
    ;

alterEvent
    : definerClause? EVENT_SYMBOL eventRef (ON_SYMBOL SCHEDULE_SYMBOL schedule)? (
        ON_SYMBOL COMPLETION_SYMBOL NOT_SYMBOL? PRESERVE_SYMBOL
    )? (RENAME_SYMBOL TO_SYMBOL qualifiedIdentifier)? (
        ENABLE_SYMBOL
        | DISABLE_SYMBOL (ON_SYMBOL replica)?
    )? (COMMENT_SYMBOL textLiteral)? (DO_SYMBOL compoundStatement)?
    ;

alterLogfileGroup
    : LOGFILE_SYMBOL GROUP_SYMBOL logfileGroupRef ADD_SYMBOL UNDOFILE_SYMBOL textLiteral alterLogfileGroupOptions?
    ;

alterLogfileGroupOptions
    : alterLogfileGroupOption (COMMA_SYMBOL? alterLogfileGroupOption)*
    ;

alterLogfileGroupOption
    : tsOptionInitialSize
    | tsOptionEngine
    | tsOptionWait
    ;

alterServer
    : SERVER_SYMBOL serverRef serverOptions
    ;

alterTable
    : onlineOption? TABLE_SYMBOL tableRef alterTableActions?
    ;

alterTableActions
    : alterCommandList (partitionClause | removePartitioning)?
    | partitionClause
    | removePartitioning
    | (alterCommandsModifierList COMMA_SYMBOL)? standaloneAlterCommands
    ;

alterCommandList
    : alterCommandsModifierList
    | (alterCommandsModifierList COMMA_SYMBOL)? alterList
    ;

alterCommandsModifierList
    : alterCommandsModifier (COMMA_SYMBOL alterCommandsModifier)*
    ;

standaloneAlterCommands
    : DISCARD_SYMBOL TABLESPACE_SYMBOL
    | IMPORT_SYMBOL TABLESPACE_SYMBOL
    | alterPartition
    | {this.isServerVersionGe80014()}? (
        SECONDARY_LOAD_SYMBOL
        | SECONDARY_UNLOAD_SYMBOL
    )
    ;

alterPartition
    : ADD_SYMBOL PARTITION_SYMBOL noWriteToBinLog? (
        partitionDefinitions
        | PARTITIONS_SYMBOL real_ulong_number
    )
    | DROP_SYMBOL PARTITION_SYMBOL identifierList
    | REBUILD_SYMBOL PARTITION_SYMBOL noWriteToBinLog? allOrPartitionNameList

    // yes, twice "no write to bin log".
    | OPTIMIZE_SYMBOL PARTITION_SYMBOL noWriteToBinLog? allOrPartitionNameList noWriteToBinLog?
    | ANALYZE_SYMBOL PARTITION_SYMBOL noWriteToBinLog? allOrPartitionNameList
    | CHECK_SYMBOL PARTITION_SYMBOL allOrPartitionNameList checkOption*
    | REPAIR_SYMBOL PARTITION_SYMBOL noWriteToBinLog? allOrPartitionNameList repairType*
    | COALESCE_SYMBOL PARTITION_SYMBOL noWriteToBinLog? real_ulong_number
    | TRUNCATE_SYMBOL PARTITION_SYMBOL allOrPartitionNameList
    | REORGANIZE_SYMBOL PARTITION_SYMBOL noWriteToBinLog? (
        identifierList INTO_SYMBOL partitionDefinitions
    )?
    | EXCHANGE_SYMBOL PARTITION_SYMBOL identifier WITH_SYMBOL TABLE_SYMBOL tableRef withValidation?
    | DISCARD_SYMBOL PARTITION_SYMBOL allOrPartitionNameList TABLESPACE_SYMBOL
    | IMPORT_SYMBOL PARTITION_SYMBOL allOrPartitionNameList TABLESPACE_SYMBOL
    ;

alterList
    : (alterListItem | createTableOptionsSpaceSeparated) (
        COMMA_SYMBOL (
            alterListItem
            | alterCommandsModifier
            | createTableOptionsSpaceSeparated
        )
    )*
    ;

alterCommandsModifier
    : alterAlgorithmOption
    | alterLockOption
    | withValidation
    ;

alterListItem
    : ADD_SYMBOL COLUMN_SYMBOL? (
        identifier fieldDefinition checkOrReferences? place?
        | OPEN_PAR_SYMBOL tableElementList CLOSE_PAR_SYMBOL
    )
    | ADD_SYMBOL tableConstraintDef
    | CHANGE_SYMBOL COLUMN_SYMBOL? columnInternalRef identifier fieldDefinition place?
    | MODIFY_SYMBOL COLUMN_SYMBOL? columnInternalRef fieldDefinition place?
    | DROP_SYMBOL (
        COLUMN_SYMBOL? columnInternalRef restrict?
        | FOREIGN_SYMBOL KEY_SYMBOL columnInternalRef
        | PRIMARY_SYMBOL KEY_SYMBOL
        | keyOrIndex indexRef
        | {this.isServerVersionGe80017()}? CHECK_SYMBOL identifier
        | {this.isServerVersionGe80019()}? CONSTRAINT_SYMBOL identifier
    )
    | DISABLE_SYMBOL KEYS_SYMBOL
    | ENABLE_SYMBOL KEYS_SYMBOL
    | ALTER_SYMBOL COLUMN_SYMBOL? columnInternalRef (
        SET_SYMBOL DEFAULT_SYMBOL (
            {this.isServerVersionGe80014()}? exprWithParentheses
            | signedLiteralOrNull
        )
        | DROP_SYMBOL DEFAULT_SYMBOL
        | {this.isServerVersionGe80024()}? SET_SYMBOL visibility
    )
    | ALTER_SYMBOL INDEX_SYMBOL indexRef visibility
    | {this.isServerVersionGe80017()}? ALTER_SYMBOL CHECK_SYMBOL identifier constraintEnforcement
    | {this.isServerVersionGe80019()}? ALTER_SYMBOL CONSTRAINT_SYMBOL identifier constraintEnforcement
    | RENAME_SYMBOL COLUMN_SYMBOL columnInternalRef TO_SYMBOL identifier
    | RENAME_SYMBOL (TO_SYMBOL | AS_SYMBOL)? tableName
    | RENAME_SYMBOL keyOrIndex indexRef TO_SYMBOL indexName
    | CONVERT_SYMBOL TO_SYMBOL charset (
        {this.isServerVersionGe80014()}? DEFAULT_SYMBOL
        | charsetName
    ) collate?
    | FORCE_SYMBOL
    | ORDER_SYMBOL BY_SYMBOL alterOrderList
    ;

place
    : AFTER_SYMBOL identifier
    | FIRST_SYMBOL
    ;

restrict
    : RESTRICT_SYMBOL
    | CASCADE_SYMBOL
    ;

alterOrderList
    : identifier direction? (COMMA_SYMBOL identifier direction?)*
    ;

alterAlgorithmOption
    : ALGORITHM_SYMBOL EQUAL_OPERATOR? (DEFAULT_SYMBOL | identifier)
    ;

alterLockOption
    : LOCK_SYMBOL EQUAL_OPERATOR? (DEFAULT_SYMBOL | identifier)
    ;

indexLockAndAlgorithm
    : alterAlgorithmOption alterLockOption?
    | alterLockOption alterAlgorithmOption?
    ;

withValidation
    : (WITH_SYMBOL | WITHOUT_SYMBOL) VALIDATION_SYMBOL
    ;

removePartitioning
    : REMOVE_SYMBOL PARTITIONING_SYMBOL
    ;

allOrPartitionNameList
    : ALL_SYMBOL
    | identifierList
    ;

alterTablespace
    : TABLESPACE_SYMBOL tablespaceRef (
        (ADD_SYMBOL | DROP_SYMBOL) DATAFILE_SYMBOL textLiteral alterTablespaceOptions?
        | RENAME_SYMBOL TO_SYMBOL identifier
        | {this.isServerVersionGe80014()}? alterTablespaceOptions
    )
    ;

alterUndoTablespace
    : UNDO_SYMBOL TABLESPACE_SYMBOL tablespaceRef SET_SYMBOL (
        ACTIVE_SYMBOL
        | INACTIVE_SYMBOL
    ) undoTableSpaceOptions?
    ;

undoTableSpaceOptions
    : undoTableSpaceOption (COMMA_SYMBOL? undoTableSpaceOption)*
    ;

undoTableSpaceOption
    : tsOptionEngine
    ;

alterTablespaceOptions
    : alterTablespaceOption (COMMA_SYMBOL? alterTablespaceOption)*
    ;

alterTablespaceOption
    : INITIAL_SIZE_SYMBOL EQUAL_OPERATOR? sizeNumber
    | tsOptionAutoextendSize
    | tsOptionMaxSize
    | tsOptionEngine
    | tsOptionWait
    | tsOptionEncryption
    | {this.isServerVersionGe80024()}? tsOptionEngineAttribute
    ;

changeTablespaceOption
    : INITIAL_SIZE_SYMBOL EQUAL_OPERATOR? sizeNumber
    | tsOptionAutoextendSize
    | tsOptionMaxSize
    ;

alterView
    : viewAlgorithm? definerClause? viewSuid? VIEW_SYMBOL viewRef viewTail
    ;

// This is not the full view_tail from sql_yacc.yy as we have either a view name or a view reference,
// depending on whether we come from createView or alterView. Everything until this difference is duplicated in those rules.
viewTail
    : columnInternalRefList? AS_SYMBOL viewQueryBlock
    ;

viewQueryBlock
    : queryExpressionWithOptLockingClauses viewCheckOption?
    ;

viewCheckOption
    : WITH_SYMBOL (CASCADED_SYMBOL | LOCAL_SYMBOL)? CHECK_SYMBOL OPTION_SYMBOL
    ;

alterInstanceStatement
    : INSTANCE_SYMBOL ROTATE_SYMBOL textOrIdentifier MASTER_SYMBOL KEY_SYMBOL
    | {this.isServerVersionGe80024()}? (
        RELOAD_SYMBOL TLS_SYMBOL (
            NO_SYMBOL ROLLBACK_SYMBOL ON_SYMBOL ERROR_SYMBOL
            | FOR_SYMBOL CHANNEL_SYMBOL identifier (
                NO_SYMBOL ROLLBACK_SYMBOL ON_SYMBOL ERROR_SYMBOL
            )?
        )
        | (ENABLE_SYMBOL | DISABLE_SYMBOL) identifier identifier
        | RELOAD_SYMBOL KEYRING_SYMBOL
    )
    ;

//----------------------------------------------------------------------------------------------------------------------

createStatement
    : CREATE_SYMBOL (
        createDatabase
        | createTable
        | createFunction
        | createProcedure
        | createUdf
        | createLogfileGroup
        | createView
        | createTrigger
        | createIndex
        | createServer
        | createTablespace
        | createEvent
        | createRole
        | {this.isServerVersionGe80011()}? createSpatialReference
        | {this.isServerVersionGe80014()}? createUndoTablespace
    )
    ;

createDatabase
    : DATABASE_SYMBOL ifNotExists? schemaName createDatabaseOption*
    ;

createDatabaseOption
    : defaultCharset
    | defaultCollation
    | {this.isServerVersionGe80016()}? defaultEncryption
    ;

createTable
    : TEMPORARY_SYMBOL? TABLE_SYMBOL ifNotExists? tableName (
        (OPEN_PAR_SYMBOL tableElementList CLOSE_PAR_SYMBOL)? createTableOptionsEtc?
        | LIKE_SYMBOL tableRef
        | OPEN_PAR_SYMBOL LIKE_SYMBOL tableRef CLOSE_PAR_SYMBOL
    )
    ;

tableElementList
    : tableElement (COMMA_SYMBOL tableElement)*
    ;

tableElement
    : columnDefinition
    | tableConstraintDef
    ;

duplicateAsQe
    : (REPLACE_SYMBOL | IGNORE_SYMBOL)? asCreateQueryExpression
    ;

asCreateQueryExpression
    : AS_SYMBOL? queryExpressionWithOptLockingClauses
    ;

queryExpressionOrParens
    : queryExpression lockingClauseList?
    | queryExpressionParens
    ;

queryExpressionWithOptLockingClauses
    : queryExpression lockingClauseList?
    ;

createRoutine
    : // Rule for external use only.
    CREATE_SYMBOL (createProcedure | createFunction | createUdf) SEMICOLON_SYMBOL? EOF
    ;

createProcedure
    : definerClause? PROCEDURE_SYMBOL ifNotExists? procedureName OPEN_PAR_SYMBOL (
        procedureParameter (COMMA_SYMBOL procedureParameter)*
    )? CLOSE_PAR_SYMBOL routineCreateOption* storedRoutineBody
    ;

routineString
    : textStringLiteral
    | DOLLAR_QUOTED_STRING_TEXT
    ;

storedRoutineBody
    : compoundStatement
    | {this.isStoredRoutineBody()}? AS_SYMBOL routineString
    ;

createFunction
    : definerClause? FUNCTION_SYMBOL ifNotExists? functionName OPEN_PAR_SYMBOL (
        functionParameter (COMMA_SYMBOL functionParameter)*
    )? CLOSE_PAR_SYMBOL RETURNS_SYMBOL typeWithOptCollate routineCreateOption* storedRoutineBody
    ;

createUdf
    : AGGREGATE_SYMBOL? FUNCTION_SYMBOL ifNotExists? udfName RETURNS_SYMBOL type = (
        STRING_SYMBOL
        | INT_SYMBOL
        | REAL_SYMBOL
        | DECIMAL_SYMBOL
    ) SONAME_SYMBOL textLiteral
    ;

// sp_c_chistic in the server grammar.
routineCreateOption
    : routineOption
    | NOT_SYMBOL? DETERMINISTIC_SYMBOL
    ;

// sp_a_chistics in the server grammar.
routineAlterOptions
    : routineCreateOption+
    ;

// sp_chistic in the server grammar.
routineOption
    : option = COMMENT_SYMBOL textLiteral
    | option = LANGUAGE_SYMBOL (
        SQL_SYMBOL
        | {this.isServerVersionGe80032()}? identifier
    )
    | option = NO_SYMBOL SQL_SYMBOL
    | option = CONTAINS_SYMBOL SQL_SYMBOL
    | option = READS_SYMBOL SQL_SYMBOL DATA_SYMBOL
    | option = MODIFIES_SYMBOL SQL_SYMBOL DATA_SYMBOL
    | option = SQL_SYMBOL SECURITY_SYMBOL security = (
        DEFINER_SYMBOL
        | INVOKER_SYMBOL
    )
    ;

createIndex
    : onlineOption? (
        UNIQUE_SYMBOL? type = INDEX_SYMBOL indexName indexTypeClause? createIndexTarget indexOption*
        | type = FULLTEXT_SYMBOL INDEX_SYMBOL indexName createIndexTarget fulltextIndexOption*
        | type = SPATIAL_SYMBOL INDEX_SYMBOL indexName createIndexTarget spatialIndexOption*
    ) indexLockAndAlgorithm?
    ;

indexNameAndType
    : indexName
    | indexName? USING_SYMBOL indexType
    | indexName TYPE_SYMBOL indexType
    ;

createIndexTarget
    : ON_SYMBOL tableRef keyListWithExpression
    ;

createLogfileGroup
    : LOGFILE_SYMBOL GROUP_SYMBOL logfileGroupName ADD_SYMBOL UNDOFILE_SYMBOL textLiteral logfileGroupOptions?
    ;

logfileGroupOptions
    : logfileGroupOption (COMMA_SYMBOL? logfileGroupOption)*
    ;

logfileGroupOption
    : tsOptionInitialSize
    | tsOptionUndoRedoBufferSize
    | tsOptionNodegroup
    | tsOptionEngine
    | tsOptionWait
    | tsOptionComment
    ;

createServer
    : SERVER_SYMBOL serverName FOREIGN_SYMBOL DATA_SYMBOL WRAPPER_SYMBOL textOrIdentifier serverOptions
    ;

serverOptions
    : OPTIONS_SYMBOL OPEN_PAR_SYMBOL serverOption (COMMA_SYMBOL serverOption)* CLOSE_PAR_SYMBOL
    ;

// Options for CREATE/ALTER SERVER, used for the federated storage engine.
serverOption
    : option = HOST_SYMBOL textLiteral
    | option = DATABASE_SYMBOL textLiteral
    | option = USER_SYMBOL textLiteral
    | option = PASSWORD_SYMBOL textLiteral
    | option = SOCKET_SYMBOL textLiteral
    | option = OWNER_SYMBOL textLiteral
    | option = PORT_SYMBOL ulong_number
    ;

createTablespace
    : TABLESPACE_SYMBOL tablespaceName tsDataFileName (
        USE_SYMBOL LOGFILE_SYMBOL GROUP_SYMBOL logfileGroupRef
    )? tablespaceOptions?
    ;

createUndoTablespace
    : UNDO_SYMBOL TABLESPACE_SYMBOL tablespaceName ADD_SYMBOL tsDataFile undoTableSpaceOptions?
    ;

tsDataFileName
    : ADD_SYMBOL tsDataFile
    | {this.isServerVersionGe80014()}? (ADD_SYMBOL tsDataFile)? // now optional
    ;

tsDataFile
    : DATAFILE_SYMBOL textLiteral
    ;

tablespaceOptions
    : tablespaceOption (COMMA_SYMBOL? tablespaceOption)*
    ;

tablespaceOption
    : tsOptionInitialSize
    | tsOptionAutoextendSize
    | tsOptionMaxSize
    | tsOptionExtentSize
    | tsOptionNodegroup
    | tsOptionEngine
    | tsOptionWait
    | tsOptionComment
    | tsOptionFileblockSize
    | {this.isServerVersionGe80014()}? tsOptionEncryption
    ;

tsOptionInitialSize
    : INITIAL_SIZE_SYMBOL EQUAL_OPERATOR? sizeNumber
    ;

tsOptionUndoRedoBufferSize
    : (UNDO_BUFFER_SIZE_SYMBOL | REDO_BUFFER_SIZE_SYMBOL) EQUAL_OPERATOR? sizeNumber
    ;

tsOptionAutoextendSize
    : AUTOEXTEND_SIZE_SYMBOL EQUAL_OPERATOR? sizeNumber
    ;

tsOptionMaxSize
    : MAX_SIZE_SYMBOL EQUAL_OPERATOR? sizeNumber
    ;

tsOptionExtentSize
    : EXTENT_SIZE_SYMBOL EQUAL_OPERATOR? sizeNumber
    ;

tsOptionNodegroup
    : NODEGROUP_SYMBOL EQUAL_OPERATOR? real_ulong_number
    ;

tsOptionEngine
    : STORAGE_SYMBOL? ENGINE_SYMBOL EQUAL_OPERATOR? engineRef
    ;

tsOptionWait
    : (WAIT_SYMBOL | NO_WAIT_SYMBOL)
    ;

tsOptionComment
    : COMMENT_SYMBOL EQUAL_OPERATOR? textLiteral
    ;

tsOptionFileblockSize
    : FILE_BLOCK_SIZE_SYMBOL EQUAL_OPERATOR? sizeNumber
    ;

tsOptionEncryption
    : ENCRYPTION_SYMBOL EQUAL_OPERATOR? textStringLiteral
    ;

tsOptionEngineAttribute
    : ENGINE_SYMBOL EQUAL_OPERATOR? jsonAttribute
    ;

createView
    : viewReplaceOrAlgorithm? definerClause? viewSuid? VIEW_SYMBOL viewName viewTail
    ;

viewReplaceOrAlgorithm
    : OR_SYMBOL REPLACE_SYMBOL viewAlgorithm?
    | viewAlgorithm
    ;

viewAlgorithm
    : ALGORITHM_SYMBOL EQUAL_OPERATOR algorithm = (
        UNDEFINED_SYMBOL
        | MERGE_SYMBOL
        | TEMPTABLE_SYMBOL
    )
    ;

viewSuid
    : SQL_SYMBOL SECURITY_SYMBOL (DEFINER_SYMBOL | INVOKER_SYMBOL)
    ;

createTrigger
    : definerClause? TRIGGER_SYMBOL ifNotExists? triggerName timing = (
        BEFORE_SYMBOL
        | AFTER_SYMBOL
    ) event = (INSERT_SYMBOL | UPDATE_SYMBOL | DELETE_SYMBOL) ON_SYMBOL tableRef FOR_SYMBOL EACH_SYMBOL ROW_SYMBOL
        triggerFollowsPrecedesClause? compoundStatement
    ;

triggerFollowsPrecedesClause
    : ordering = (FOLLOWS_SYMBOL | PRECEDES_SYMBOL) textOrIdentifier // not a trigger reference!
    ;

createEvent
    : definerClause? EVENT_SYMBOL ifNotExists? eventName ON_SYMBOL SCHEDULE_SYMBOL schedule (
        ON_SYMBOL COMPLETION_SYMBOL NOT_SYMBOL? PRESERVE_SYMBOL
    )? (ENABLE_SYMBOL | DISABLE_SYMBOL (ON_SYMBOL replica)?)? (
        COMMENT_SYMBOL textLiteral
    )? DO_SYMBOL compoundStatement
    ;

createRole
    :
    // The server grammar has a clear_privileges rule here, which is only used to clear internal state.
    ROLE_SYMBOL ifNotExists? roleList
    ;

createSpatialReference
    : OR_SYMBOL REPLACE_SYMBOL SPATIAL_SYMBOL REFERENCE_SYMBOL SYSTEM_SYMBOL real_ulonglong_number srsAttribute*
    | SPATIAL_SYMBOL REFERENCE_SYMBOL SYSTEM_SYMBOL ifNotExists? real_ulonglong_number srsAttribute*
    ;

srsAttribute
    : NAME_SYMBOL TEXT_SYMBOL textStringNoLinebreak
    | DEFINITION_SYMBOL TEXT_SYMBOL textStringNoLinebreak
    | ORGANIZATION_SYMBOL textStringNoLinebreak IDENTIFIED_SYMBOL BY_SYMBOL real_ulonglong_number
    | DESCRIPTION_SYMBOL TEXT_SYMBOL textStringNoLinebreak
    ;

//----------------------------------------------------------------------------------------------------------------------

dropStatement
    : DROP_SYMBOL (
        dropDatabase
        | dropEvent
        | dropFunction
        | dropProcedure
        | dropIndex
        | dropLogfileGroup
        | dropServer
        | dropTable
        | dropTableSpace
        | dropTrigger
        | dropView
        | dropRole
        | {this.isServerVersionGe80011()}? dropSpatialReference
        | {this.isServerVersionGe80014()}? dropUndoTablespace
    )
    ;

dropDatabase
    : DATABASE_SYMBOL ifExists? schemaRef
    ;

dropEvent
    : EVENT_SYMBOL ifExists? eventRef
    ;

dropFunction
    : FUNCTION_SYMBOL ifExists? functionRef // Including UDFs.
    ;

dropProcedure
    : PROCEDURE_SYMBOL ifExists? procedureRef
    ;

dropIndex
    : onlineOption? type = INDEX_SYMBOL indexRef ON_SYMBOL tableRef indexLockAndAlgorithm?
    ;

dropLogfileGroup
    : LOGFILE_SYMBOL GROUP_SYMBOL logfileGroupRef (
        dropLogfileGroupOption (COMMA_SYMBOL? dropLogfileGroupOption)*
    )?
    ;

dropLogfileGroupOption
    : tsOptionWait
    | tsOptionEngine
    ;

dropServer
    : SERVER_SYMBOL ifExists? serverRef
    ;

dropTable
    : TEMPORARY_SYMBOL? type = (TABLE_SYMBOL | TABLES_SYMBOL) ifExists? tableRefList (
        RESTRICT_SYMBOL
        | CASCADE_SYMBOL
    )?
    ;

dropTableSpace
    : TABLESPACE_SYMBOL tablespaceRef (
        dropLogfileGroupOption (COMMA_SYMBOL? dropLogfileGroupOption)*
    )?
    ;

dropTrigger
    : TRIGGER_SYMBOL ifExists? triggerRef
    ;

dropView
    : VIEW_SYMBOL ifExists? viewRefList (RESTRICT_SYMBOL | CASCADE_SYMBOL)?
    ;

dropRole
    : ROLE_SYMBOL ifExists? roleList
    ;

dropSpatialReference
    : SPATIAL_SYMBOL REFERENCE_SYMBOL SYSTEM_SYMBOL ifExists? real_ulonglong_number
    ;

dropUndoTablespace
    : UNDO_SYMBOL TABLESPACE_SYMBOL tablespaceRef undoTableSpaceOptions?
    ;

//----------------------------------------------------------------------------------------------------------------------

renameTableStatement
    : RENAME_SYMBOL (TABLE_SYMBOL | TABLES_SYMBOL) renamePair (
        COMMA_SYMBOL renamePair
    )*
    ;

renamePair
    : tableRef TO_SYMBOL tableName
    ;

//----------------------------------------------------------------------------------------------------------------------

truncateTableStatement
    : TRUNCATE_SYMBOL TABLE_SYMBOL? tableRef
    ;

//----------------------------------------------------------------------------------------------------------------------

importStatement
    : IMPORT_SYMBOL TABLE_SYMBOL FROM_SYMBOL textStringLiteralList
    ;

//--------------- DML statements ---------------------------------------------------------------------------------------

callStatement
    : CALL_SYMBOL procedureRef (OPEN_PAR_SYMBOL exprList? CLOSE_PAR_SYMBOL)?
    ;

deleteStatement
    : withClause? DELETE_SYMBOL deleteStatementOption* (
        FROM_SYMBOL (
            tableAliasRefList USING_SYMBOL tableReferenceList whereClause? // Multi table variant 1.
            | tableRef ({this.isServerVersionGe80017()}? tableAlias)? partitionDelete? whereClause? orderClause? simpleLimitClause?
            // Single table delete.
        )
        | tableAliasRefList FROM_SYMBOL tableReferenceList whereClause? // Multi table variant 2.
    )
    ;

partitionDelete
    : PARTITION_SYMBOL OPEN_PAR_SYMBOL identifierList CLOSE_PAR_SYMBOL
    ;

deleteStatementOption
    : // opt_delete_option in sql_yacc.yy, but the name collides with another rule (delete_options).
    QUICK_SYMBOL
    | LOW_PRIORITY_SYMBOL
    | QUICK_SYMBOL
    | IGNORE_SYMBOL
    ;

doStatement
    : DO_SYMBOL selectItemList
    ;

handlerStatement
    : HANDLER_SYMBOL (
        tableRef OPEN_SYMBOL tableAlias?
        | identifier (
            CLOSE_SYMBOL
            | READ_SYMBOL handlerReadOrScan whereClause? limitClause?
        )
    )
    ;

handlerReadOrScan
    : (FIRST_SYMBOL | NEXT_SYMBOL) // Scan function.
    | identifier (
        // The rkey part.
        (FIRST_SYMBOL | NEXT_SYMBOL | PREV_SYMBOL | LAST_SYMBOL)
        | (
            EQUAL_OPERATOR
            | LESS_THAN_OPERATOR
            | GREATER_THAN_OPERATOR
            | LESS_OR_EQUAL_OPERATOR
            | GREATER_OR_EQUAL_OPERATOR
        ) OPEN_PAR_SYMBOL values CLOSE_PAR_SYMBOL
    )
    ;

//----------------------------------------------------------------------------------------------------------------------

insertStatement
    : INSERT_SYMBOL insertLockOption? IGNORE_SYMBOL? INTO_SYMBOL? tableRef usePartition? (
        insertFromConstructor valuesReference?
        | SET_SYMBOL updateList valuesReference?
        | insertQueryExpression
    ) insertUpdateList?
    ;

insertLockOption
    : LOW_PRIORITY_SYMBOL
    | DELAYED_SYMBOL // Only allowed if no select is used. Check in the semantic phase.
    | HIGH_PRIORITY_SYMBOL
    ;

insertFromConstructor
    : (OPEN_PAR_SYMBOL fields? CLOSE_PAR_SYMBOL)? insertValues
    ;

fields
    : insertIdentifier (COMMA_SYMBOL insertIdentifier)*
    ;

insertValues
    : (VALUES_SYMBOL | VALUE_SYMBOL) valueList
    ;

insertQueryExpression
    : queryExpression
    | queryExpressionParens
    | (OPEN_PAR_SYMBOL fields? CLOSE_PAR_SYMBOL)? queryExpressionWithOptLockingClauses
    ;

valueList
    : OPEN_PAR_SYMBOL values? CLOSE_PAR_SYMBOL (
        COMMA_SYMBOL OPEN_PAR_SYMBOL values? CLOSE_PAR_SYMBOL
    )*
    ;

values
    : (expr | DEFAULT_SYMBOL) (COMMA_SYMBOL (expr | DEFAULT_SYMBOL))*
    ;

valuesReference
    : {this.isServerVersionGe80018()}? AS_SYMBOL identifier columnInternalRefList?
    ;

insertUpdateList
    : ON_SYMBOL DUPLICATE_SYMBOL KEY_SYMBOL UPDATE_SYMBOL updateList
    ;

//----------------------------------------------------------------------------------------------------------------------

loadStatement
    : LOAD_SYMBOL dataOrXml loadDataLock? loadFrom? LOCAL_SYMBOL? loadSourceType? textStringLiteral sourceCount? sourceOrder? (
        REPLACE_SYMBOL
        | IGNORE_SYMBOL
    )? INTO_SYMBOL TABLE_SYMBOL tableRef usePartition? charsetClause? xmlRowsIdentifiedBy? fieldsClause? linesClause?
        loadDataFileTail loadParallel? loadMemory? loadAlgorithm?
    ;

dataOrXml
    : DATA_SYMBOL
    | XML_SYMBOL
    ;

loadDataLock
    : LOW_PRIORITY_SYMBOL
    | CONCURRENT_SYMBOL
    ;

loadFrom
    : {this.isServerVersionGe80200()}? FROM_SYMBOL
    ;

loadSourceType
    : INFILE_SYMBOL
    | {this.isServerVersionGe80200()}? (URL_SYMBOL | S3_SYMBOL)
    ;

sourceCount
    : {this.isServerVersionGe80200()}? (
        COUNT_SYMBOL INT_NUMBER
        | pureIdentifier INT_NUMBER
    )
    ;

sourceOrder
    : {this.isServerVersionGe80200()}? IN_SYMBOL PRIMARY_SYMBOL KEY_SYMBOL ORDER_SYMBOL
    ;

xmlRowsIdentifiedBy
    : ROWS_SYMBOL IDENTIFIED_SYMBOL BY_SYMBOL textString
    ;

loadDataFileTail
    : (IGNORE_SYMBOL INT_NUMBER (LINES_SYMBOL | ROWS_SYMBOL))? loadDataFileTargetList? (
        SET_SYMBOL updateList
    )?
    ;

loadDataFileTargetList
    : OPEN_PAR_SYMBOL fieldOrVariableList? CLOSE_PAR_SYMBOL
    ;

fieldOrVariableList
    : (columnRef | AT_SIGN_SYMBOL textOrIdentifier | AT_AT_SIGN_SYMBOL) (
        COMMA_SYMBOL (
            columnRef
            | AT_SIGN_SYMBOL textOrIdentifier
            | AT_TEXT_SUFFIX
            | AT_AT_SIGN_SYMBOL
        )
    )*
    ;

loadAlgorithm
    : {this.isServerVersionGe80200()}? ALGORITHM_SYMBOL EQUAL_OPERATOR BULK_SYMBOL
    ;

loadParallel
    : {this.isServerVersionGe80200()}? PARALLEL_SYMBOL EQUAL_OPERATOR INT_NUMBER
    ;

loadMemory
    : {this.isServerVersionGe80200()}? MEMORY_SYMBOL EQUAL_OPERATOR sizeNumber
    ;

//----------------------------------------------------------------------------------------------------------------------

replaceStatement
    : REPLACE_SYMBOL (LOW_PRIORITY_SYMBOL | DELAYED_SYMBOL)? INTO_SYMBOL? tableRef usePartition? (
        insertFromConstructor
        | SET_SYMBOL updateList
        | insertQueryExpression
    )
    ;

//----------------------------------------------------------------------------------------------------------------------

selectStatement
    : queryExpression lockingClauseList?
    | selectStatementWithInto
    ;

selectStatementWithInto
    : OPEN_PAR_SYMBOL selectStatementWithInto CLOSE_PAR_SYMBOL
    | queryExpression intoClause lockingClauseList?
    | queryExpression lockingClauseList intoClause
    | {this.isSelectStatementWithInto()}? queryExpressionParens intoClause
    ;

queryExpression
    : withClause? queryExpressionBody orderClause? limitClause?
    ;

queryExpressionBody
    : (queryPrimary | queryExpressionParens)

    // Unlimited UNIONS.
    (
        (
            UNION_SYMBOL
            | {this.isServerVersionGe80031()}? (EXCEPT_SYMBOL | INTERSECT_SYMBOL)
        ) unionOption? queryExpressionBody
    )*
    ;

queryExpressionParens
    : OPEN_PAR_SYMBOL (queryExpressionParens | queryExpressionWithOptLockingClauses) CLOSE_PAR_SYMBOL
    ;

queryPrimary
    : querySpecification
    | {this.isServerVersionGe80019()}? tableValueConstructor
    | {this.isServerVersionGe80019()}? explicitTable
    ;

querySpecification
    : SELECT_SYMBOL selectOption* selectItemList intoClause? fromClause? whereClause? groupByClause? havingClause? windowClause?
        qualifyClause?
    ;

subquery
    : queryExpressionParens
    ;

querySpecOption
    : ALL_SYMBOL
    | DISTINCT_SYMBOL
    | STRAIGHT_JOIN_SYMBOL
    | HIGH_PRIORITY_SYMBOL
    | SQL_SMALL_RESULT_SYMBOL
    | SQL_BIG_RESULT_SYMBOL
    | SQL_BUFFER_RESULT_SYMBOL
    | SQL_CALC_FOUND_ROWS_SYMBOL
    ;

limitClause
    : LIMIT_SYMBOL limitOptions
    ;

simpleLimitClause
    : LIMIT_SYMBOL limitOption
    ;

limitOptions
    : limitOption ((COMMA_SYMBOL | OFFSET_SYMBOL) limitOption)?
    ;

limitOption
    : identifier
    | (PARAM_MARKER | ULONGLONG_NUMBER | LONG_NUMBER | INT_NUMBER)
    ;

intoClause
    : INTO_SYMBOL (
        OUTFILE_SYMBOL textStringLiteral charsetClause? fieldsClause? linesClause?
        | DUMPFILE_SYMBOL textStringLiteral
        | (textOrIdentifier | userVariable) (
            COMMA_SYMBOL (textOrIdentifier | userVariable)
        )*
    )
    ;

procedureAnalyseClause
    : PROCEDURE_SYMBOL OPEN_PAR_SYMBOL (INT_NUMBER (COMMA_SYMBOL INT_NUMBER)?)? CLOSE_PAR_SYMBOL
    ;

havingClause
    : HAVING_SYMBOL expr
    ;

qualifyClause
    : {this.isServerVersionGe80200()}? QUALIFY_SYMBOL expr
    ;

windowClause
    : WINDOW_SYMBOL windowDefinition (COMMA_SYMBOL windowDefinition)*
    ;

windowDefinition
    : windowName AS_SYMBOL windowSpec
    ;

windowSpec
    : OPEN_PAR_SYMBOL windowSpecDetails CLOSE_PAR_SYMBOL
    ;

windowSpecDetails
    : windowName? (PARTITION_SYMBOL BY_SYMBOL orderList)? orderClause? windowFrameClause?
    ;

windowFrameClause
    : windowFrameUnits windowFrameExtent windowFrameExclusion?
    ;

windowFrameUnits
    : ROWS_SYMBOL
    | RANGE_SYMBOL
    | GROUPS_SYMBOL
    ;

windowFrameExtent
    : windowFrameStart
    | windowFrameBetween
    ;

windowFrameStart
    : UNBOUNDED_SYMBOL PRECEDING_SYMBOL
    | ulonglongNumber PRECEDING_SYMBOL
    | PARAM_MARKER PRECEDING_SYMBOL
    | INTERVAL_SYMBOL expr interval PRECEDING_SYMBOL
    | CURRENT_SYMBOL ROW_SYMBOL
    ;

windowFrameBetween
    : BETWEEN_SYMBOL windowFrameBound AND_SYMBOL windowFrameBound
    ;

windowFrameBound
    : windowFrameStart
    | UNBOUNDED_SYMBOL FOLLOWING_SYMBOL
    | ulonglongNumber FOLLOWING_SYMBOL
    | PARAM_MARKER FOLLOWING_SYMBOL
    | INTERVAL_SYMBOL expr interval FOLLOWING_SYMBOL
    ;

windowFrameExclusion
    : EXCLUDE_SYMBOL (
        CURRENT_SYMBOL ROW_SYMBOL
        | GROUP_SYMBOL
        | TIES_SYMBOL
        | NO_SYMBOL OTHERS_SYMBOL
    )
    ;

withClause
    : WITH_SYMBOL RECURSIVE_SYMBOL? commonTableExpression (
        COMMA_SYMBOL commonTableExpression
    )*
    ;

commonTableExpression
    : identifier columnInternalRefList? AS_SYMBOL subquery
    ;

groupByClause
    : GROUP_SYMBOL BY_SYMBOL orderList olapOption?
    | {this.isServerVersionGe80032()}? GROUP_SYMBOL BY_SYMBOL (
        ROLLUP_SYMBOL
        | CUBE_SYMBOL
    ) OPEN_PAR_SYMBOL groupList CLOSE_PAR_SYMBOL
    ;

olapOption
    : WITH_SYMBOL ROLLUP_SYMBOL
    ;

orderClause
    : ORDER_SYMBOL BY_SYMBOL orderList
    ;

direction
    : ASC_SYMBOL
    | DESC_SYMBOL
    ;

fromClause
    : FROM_SYMBOL (DUAL_SYMBOL | tableReferenceList)
    ;

tableReferenceList
    : tableReference (COMMA_SYMBOL tableReference)*
    ;

tableValueConstructor
    : VALUES_SYMBOL rowValueExplicit (COMMA_SYMBOL rowValueExplicit)*
    ;

explicitTable
    : TABLE_SYMBOL tableRef
    ;

rowValueExplicit
    : ROW_SYMBOL OPEN_PAR_SYMBOL values? CLOSE_PAR_SYMBOL
    ;

selectOption
    : querySpecOption
    | SQL_NO_CACHE_SYMBOL // Deprecated and ignored in 8.0.
    ;

lockingClauseList
    : {this.isServerVersionGe80031()}? lockingClause+
    ;

lockingClause
    : FOR_SYMBOL lockStrengh (OF_SYMBOL tableAliasRefList)? lockedRowAction?
    | LOCK_SYMBOL IN_SYMBOL SHARE_SYMBOL MODE_SYMBOL
    ;

lockStrengh
    : UPDATE_SYMBOL
    | SHARE_SYMBOL
    ;

lockedRowAction
    : SKIP_SYMBOL LOCKED_SYMBOL
    | NOWAIT_SYMBOL
    ;

selectItemList
    : (selectItem | MULT_OPERATOR) (COMMA_SYMBOL selectItem)*
    ;

selectItem
    : tableWild
    | expr selectAlias?
    ;

selectAlias
    : AS_SYMBOL? (identifier | textStringLiteral)
    ;

whereClause
    : WHERE_SYMBOL expr
    ;

tableReference
    : // Note: we have also a tableRef rule for identifiers that reference a table anywhere.
    (
        tableFactor
        // ODBC syntax
        | OPEN_CURLY_SYMBOL ({this.isServerVersionGe80017()}? identifier | OJ_SYMBOL) escapedTableReference CLOSE_CURLY_SYMBOL
    ) joinedTable*
    ;

escapedTableReference
    : tableFactor joinedTable*
    ;

joinedTable
    : // Same as joined_table in sql_yacc.yy, but with removed left recursion.
    innerJoinType tableReference (
        ON_SYMBOL expr
        | USING_SYMBOL identifierListWithParentheses
    )?
    | outerJoinType tableReference (
        ON_SYMBOL expr
        | USING_SYMBOL identifierListWithParentheses
    )
    | naturalJoinType tableFactor
    ;

naturalJoinType
    : NATURAL_SYMBOL INNER_SYMBOL? JOIN_SYMBOL
    | NATURAL_SYMBOL (LEFT_SYMBOL | RIGHT_SYMBOL) OUTER_SYMBOL? JOIN_SYMBOL
    ;

innerJoinType
    : type = (INNER_SYMBOL | CROSS_SYMBOL)? JOIN_SYMBOL
    | type = STRAIGHT_JOIN_SYMBOL
    ;

outerJoinType
    : type = (LEFT_SYMBOL | RIGHT_SYMBOL) OUTER_SYMBOL? JOIN_SYMBOL
    ;

tableFactor
    : singleTable
    | singleTableParens
    | derivedTable
    | tableReferenceListParens
    | {this.isServerVersionGe80004()}? tableFunction
    ;

singleTable
    : tableRef usePartition? tableAlias? indexHintList? tablesampleClause?
    ;

singleTableParens
    : OPEN_PAR_SYMBOL (singleTable | singleTableParens) CLOSE_PAR_SYMBOL
    ;

derivedTable
    : subquery tableAlias? columnInternalRefList?
    | {this.isServerVersionGe80014()}? LATERAL_SYMBOL subquery tableAlias? columnInternalRefList?
    ;

// This rule covers both: joined_table_parens and table_reference_list_parens from sql_yacc.yy.
// We can simplify that because we have unrolled the indirect left recursion in joined_table <-> table_reference.
tableReferenceListParens
    : OPEN_PAR_SYMBOL (tableReferenceList | tableReferenceListParens) CLOSE_PAR_SYMBOL
    ;

tableFunction
    : JSON_TABLE_SYMBOL OPEN_PAR_SYMBOL expr COMMA_SYMBOL textStringLiteral columnsClause CLOSE_PAR_SYMBOL tableAlias?
    ;

columnsClause
    : COLUMNS_SYMBOL OPEN_PAR_SYMBOL jtColumn (COMMA_SYMBOL jtColumn)* CLOSE_PAR_SYMBOL
    ;

jtColumn
    : identifier FOR_SYMBOL ORDINALITY_SYMBOL
    | identifier dataType ({this.isServerVersionGe80014()}? collate)? EXISTS_SYMBOL? PATH_SYMBOL textStringLiteral
        onEmptyOrErrorJsonTable?
    | NESTED_SYMBOL PATH_SYMBOL textStringLiteral columnsClause
    ;

onEmptyOrError
    : onEmpty onError?
    | onError
    ;

// One variation of the empty/error syntax that is going to be deprecated, but currently still allowed.
onEmptyOrErrorJsonTable
    : onEmptyOrError
    | onError onEmpty
    ;

onEmpty
    : jsonOnResponse ON_SYMBOL EMPTY_SYMBOL
    ;

onError
    : jsonOnResponse ON_SYMBOL ERROR_SYMBOL
    ;

jsonOnResponse
    : ERROR_SYMBOL
    | NULL_SYMBOL
    | DEFAULT_SYMBOL textStringLiteral
    ;

unionOption
    : DISTINCT_SYMBOL
    | ALL_SYMBOL
    ;

tableAlias
    : (AS_SYMBOL | {this.isServerVersionGe80017()}? EQUAL_OPERATOR)? identifier
    ;

indexHintList
    : indexHint (COMMA_SYMBOL indexHint)*
    ;

indexHint
    : indexHintType keyOrIndex indexHintClause? OPEN_PAR_SYMBOL indexList CLOSE_PAR_SYMBOL
    | USE_SYMBOL keyOrIndex indexHintClause? OPEN_PAR_SYMBOL indexList? CLOSE_PAR_SYMBOL
    ;

indexHintType
    : FORCE_SYMBOL
    | IGNORE_SYMBOL
    ;

keyOrIndex
    : KEY_SYMBOL
    | INDEX_SYMBOL
    ;

constraintKeyType
    : PRIMARY_SYMBOL KEY_SYMBOL
    | UNIQUE_SYMBOL keyOrIndex?
    ;

indexHintClause
    : FOR_SYMBOL (JOIN_SYMBOL | ORDER_SYMBOL BY_SYMBOL | GROUP_SYMBOL BY_SYMBOL)
    ;

indexList
    : indexListElement (COMMA_SYMBOL indexListElement)*
    ;

indexListElement
    : identifier
    | PRIMARY_SYMBOL
    ;

//----------------------------------------------------------------------------------------------------------------------

updateStatement
    : withClause? UPDATE_SYMBOL LOW_PRIORITY_SYMBOL? IGNORE_SYMBOL? tableReferenceList SET_SYMBOL updateList whereClause?
        orderClause? simpleLimitClause?
    ;

//----------------------------------------------------------------------------------------------------------------------

transactionOrLockingStatement
    : transactionStatement
    | savepointStatement
    | lockStatement
    | xaStatement
    ;

transactionStatement
    : START_SYMBOL TRANSACTION_SYMBOL startTransactionOptionList*
    | COMMIT_SYMBOL WORK_SYMBOL? (AND_SYMBOL NO_SYMBOL? CHAIN_SYMBOL)? (
        NO_SYMBOL? RELEASE_SYMBOL
    )?
    // SET TRANSACTION is part of setStatement.
    ;

// BEGIN WORK is separated from transactional statements as it must not appear as part of a stored program.
beginWork
    : BEGIN_SYMBOL WORK_SYMBOL?
    ;

startTransactionOptionList
    : WITH_SYMBOL CONSISTENT_SYMBOL SNAPSHOT_SYMBOL
    | READ_SYMBOL (WRITE_SYMBOL | ONLY_SYMBOL)
    ;

savepointStatement
    : SAVEPOINT_SYMBOL identifier
    | ROLLBACK_SYMBOL WORK_SYMBOL? (
        TO_SYMBOL SAVEPOINT_SYMBOL? identifier
        | (AND_SYMBOL NO_SYMBOL? CHAIN_SYMBOL)? (NO_SYMBOL? RELEASE_SYMBOL)?
    )
    | RELEASE_SYMBOL SAVEPOINT_SYMBOL identifier
    ;

lockStatement
    : LOCK_SYMBOL (TABLES_SYMBOL | TABLE_SYMBOL) lockItem (COMMA_SYMBOL lockItem)*
    | LOCK_SYMBOL INSTANCE_SYMBOL FOR_SYMBOL BACKUP_SYMBOL
    | UNLOCK_SYMBOL ( TABLES_SYMBOL | TABLE_SYMBOL | INSTANCE_SYMBOL)
    ;

lockItem
    : tableRef tableAlias? lockOption
    ;

lockOption
    : READ_SYMBOL LOCAL_SYMBOL?
    | LOW_PRIORITY_SYMBOL? WRITE_SYMBOL // low priority deprecated since 5.7
    ;

xaStatement
    : XA_SYMBOL (
        (START_SYMBOL | BEGIN_SYMBOL) xid (JOIN_SYMBOL | RESUME_SYMBOL)?
        | END_SYMBOL xid (SUSPEND_SYMBOL (FOR_SYMBOL MIGRATE_SYMBOL)?)?
        | PREPARE_SYMBOL xid
        | COMMIT_SYMBOL xid (ONE_SYMBOL PHASE_SYMBOL)?
        | ROLLBACK_SYMBOL xid
        | RECOVER_SYMBOL xaConvert?
    )
    ;

xaConvert
    : CONVERT_SYMBOL XID_SYMBOL
    ;

xid
    : textString (COMMA_SYMBOL textString (COMMA_SYMBOL ulong_number)?)?
    ;

//----------------------------------------------------------------------------------------------------------------------

replicationStatement
    : PURGE_SYMBOL purgeOptions
    | changeSource
    | RESET_SYMBOL resetOption (COMMA_SYMBOL resetOption)*
    | RESET_SYMBOL PERSIST_SYMBOL ifExistsIdentifier?
    | startReplicaStatement
    | stopReplicaStatement
    | changeReplication
    | replicationLoad
    | groupReplication
    ;

purgeOptions
    : (BINARY_SYMBOL | MASTER_SYMBOL) LOGS_SYMBOL (
        TO_SYMBOL textLiteral
        | BEFORE_SYMBOL expr
    )
    ;

resetOption
    : masterOrBinaryLogsAndGtids sourceResetOptions?
    | replica ALL_SYMBOL? channel?
    ;

masterOrBinaryLogsAndGtids
    : MASTER_SYMBOL
    | {this.isServerVersionGe80032()}? BINARY_SYMBOL LOGS_SYMBOL AND_SYMBOL GTIDS_SYMBOL
    ;

sourceResetOptions
    : TO_SYMBOL real_ulonglong_number
    ;

replicationLoad
    : LOAD_SYMBOL (DATA_SYMBOL | TABLE_SYMBOL tableRef) FROM_SYMBOL MASTER_SYMBOL
    ;

changeReplicationSource
    : MASTER_SYMBOL
    | {this.isServerVersionGe80024()}? REPLICATION_SYMBOL SOURCE_SYMBOL
    ;

changeSource
    : CHANGE_SYMBOL changeReplicationSource TO_SYMBOL sourceDefinitions channel?
    ;

sourceDefinitions
    : sourceDefinition (COMMA_SYMBOL sourceDefinition)*
    ;

sourceDefinition
    : // source_def in sql_yacc.yy
    changeReplicationSourceHost EQUAL_OPERATOR textStringNoLinebreak
    | NETWORK_NAMESPACE_SYMBOL EQUAL_OPERATOR textStringNoLinebreak
    | changeReplicationSourceBind EQUAL_OPERATOR textStringNoLinebreak
    | changeReplicationSourceUser EQUAL_OPERATOR textStringNoLinebreak
    | changeReplicationSourcePassword EQUAL_OPERATOR textStringNoLinebreak
    | changeReplicationSourcePort EQUAL_OPERATOR ulong_number
    | changeReplicationSourceConnectRetry EQUAL_OPERATOR ulong_number
    | changeReplicationSourceRetryCount EQUAL_OPERATOR ulong_number
    | changeReplicationSourceDelay EQUAL_OPERATOR ulong_number
    | changeReplicationSourceSSL EQUAL_OPERATOR ulong_number
    | changeReplicationSourceSSLCA EQUAL_OPERATOR textStringNoLinebreak
    | changeReplicationSourceSSLCApath EQUAL_OPERATOR textStringNoLinebreak
    | changeReplicationSourceTLSVersion EQUAL_OPERATOR textStringNoLinebreak
    | changeReplicationSourceSSLCert EQUAL_OPERATOR textStringNoLinebreak
    | changeReplicationSourceTLSCiphersuites EQUAL_OPERATOR sourceTlsCiphersuitesDef
    | changeReplicationSourceSSLCipher EQUAL_OPERATOR textStringNoLinebreak
    | changeReplicationSourceSSLKey EQUAL_OPERATOR textStringNoLinebreak
    | changeReplicationSourceSSLVerifyServerCert EQUAL_OPERATOR ulong_number
    | changeReplicationSourceSSLCLR EQUAL_OPERATOR textLiteral
    | changeReplicationSourceSSLCLRpath EQUAL_OPERATOR textStringNoLinebreak
    | changeReplicationSourcePublicKey EQUAL_OPERATOR textStringNoLinebreak
    | changeReplicationSourceGetSourcePublicKey EQUAL_OPERATOR ulong_number
    | changeReplicationSourceHeartbeatPeriod EQUAL_OPERATOR ulong_number
    | IGNORE_SERVER_IDS_SYMBOL EQUAL_OPERATOR serverIdList
    | changeReplicationSourceCompressionAlgorithm EQUAL_OPERATOR textStringLiteral
    | changeReplicationSourceZstdCompressionLevel EQUAL_OPERATOR ulong_number
    | changeReplicationSourceAutoPosition EQUAL_OPERATOR ulong_number
    | PRIVILEGE_CHECKS_USER_SYMBOL EQUAL_OPERATOR privilegeCheckDef
    | REQUIRE_ROW_FORMAT_SYMBOL EQUAL_OPERATOR ulong_number
    | REQUIRE_TABLE_PRIMARY_KEY_CHECK_SYMBOL EQUAL_OPERATOR tablePrimaryKeyCheckDef
    | {this.isServerVersionGe80024()}? SOURCE_CONNECTION_AUTO_FAILOVER_SYMBOL EQUAL_OPERATOR real_ulong_number
    | {this.isServerVersionGe80024()}? ASSIGN_GTIDS_TO_ANONYMOUS_TRANSACTIONS_SYMBOL EQUAL_OPERATOR
        assignGtidsToAnonymousTransactionsDefinition
    | {this.isServerVersionGe80027()}? GTID_ONLY_SYMBOL EQUAL_OPERATOR real_ulong_number
    | sourceFileDef
    ;

changeReplicationSourceAutoPosition
    : MASTER_AUTO_POSITION_SYMBOL
    | SOURCE_AUTO_POSITION_SYMBOL
    ;

changeReplicationSourceHost
    : MASTER_HOST_SYMBOL
    | SOURCE_HOST_SYMBOL
    ;

changeReplicationSourceBind
    : MASTER_BIND_SYMBOL
    | SOURCE_BIND_SYMBOL
    ;

changeReplicationSourceUser
    : MASTER_USER_SYMBOL
    | SOURCE_USER_SYMBOL
    ;

changeReplicationSourcePassword
    : MASTER_PASSWORD_SYMBOL
    | SOURCE_PASSWORD_SYMBOL
    ;

changeReplicationSourcePort
    : MASTER_PORT_SYMBOL
    | SOURCE_PORT_SYMBOL
    ;

changeReplicationSourceConnectRetry
    : MASTER_CONNECT_RETRY_SYMBOL
    | SOURCE_CONNECT_RETRY_SYMBOL
    ;

changeReplicationSourceRetryCount
    : MASTER_RETRY_COUNT_SYMBOL
    | SOURCE_RETRY_COUNT_SYMBOL
    ;

changeReplicationSourceDelay
    : MASTER_DELAY_SYMBOL
    | SOURCE_DELAY_SYMBOL
    ;

changeReplicationSourceSSL
    : MASTER_SSL_SYMBOL
    | SOURCE_SSL_SYMBOL
    ;

changeReplicationSourceSSLCA
    : MASTER_SSL_CA_SYMBOL
    | SOURCE_SSL_CA_SYMBOL
    ;

changeReplicationSourceSSLCApath
    : MASTER_SSL_CAPATH_SYMBOL
    | SOURCE_SSL_CAPATH_SYMBOL
    ;

changeReplicationSourceSSLCipher
    : MASTER_SSL_CIPHER_SYMBOL
    | SOURCE_SSL_CIPHER_SYMBOL
    ;

changeReplicationSourceSSLCLR
    : MASTER_SSL_CRL_SYMBOL
    | SOURCE_SSL_CRL_SYMBOL
    ;

changeReplicationSourceSSLCLRpath
    : MASTER_SSL_CRLPATH_SYMBOL
    | SOURCE_SSL_CRLPATH_SYMBOL
    ;

changeReplicationSourceSSLKey
    : MASTER_SSL_KEY_SYMBOL
    | SOURCE_SSL_KEY_SYMBOL
    ;

changeReplicationSourceSSLVerifyServerCert
    : MASTER_SSL_VERIFY_SERVER_CERT_SYMBOL
    | SOURCE_SSL_VERIFY_SERVER_CERT_SYMBOL
    ;

changeReplicationSourceTLSVersion
    : MASTER_TLS_VERSION_SYMBOL
    | SOURCE_TLS_VERSION_SYMBOL
    ;

changeReplicationSourceTLSCiphersuites
    : MASTER_TLS_CIPHERSUITES_SYMBOL
    | SOURCE_TLS_CIPHERSUITES_SYMBOL
    ;

changeReplicationSourceSSLCert
    : MASTER_SSL_CERT_SYMBOL
    | SOURCE_SSL_CERT_SYMBOL
    ;

changeReplicationSourcePublicKey
    : MASTER_PUBLIC_KEY_PATH_SYMBOL
    | SOURCE_PUBLIC_KEY_PATH_SYMBOL
    ;

changeReplicationSourceGetSourcePublicKey
    : GET_MASTER_PUBLIC_KEY_SYMBOL
    | GET_SOURCE_PUBLIC_KEY_SYMBOL
    ;

changeReplicationSourceHeartbeatPeriod
    : MASTER_HEARTBEAT_PERIOD_SYMBOL
    | SOURCE_HEARTBEAT_PERIOD_SYMBOL
    ;

changeReplicationSourceCompressionAlgorithm
    : MASTER_COMPRESSION_ALGORITHM_SYMBOL
    | SOURCE_COMPRESSION_ALGORITHM_SYMBOL
    ;

changeReplicationSourceZstdCompressionLevel
    : MASTER_ZSTD_COMPRESSION_LEVEL_SYMBOL
    | SOURCE_ZSTD_COMPRESSION_LEVEL_SYMBOL
    ;

privilegeCheckDef
    : userIdentifierOrText
    | NULL_SYMBOL
    ;

tablePrimaryKeyCheckDef
    : STREAM_SYMBOL
    | ON_SYMBOL
    | OFF_SYMBOL
    | GENERATE_SYMBOL
    ;

assignGtidsToAnonymousTransactionsDefinition
    : OFF_SYMBOL
    | LOCAL_SYMBOL
    | textStringLiteral
    ;

sourceTlsCiphersuitesDef
    : textStringNoLinebreak
    | NULL_SYMBOL
    ;

sourceFileDef
    : sourceLogFile EQUAL_OPERATOR textStringNoLinebreak
    | sourceLogPos EQUAL_OPERATOR ulonglongNumber
    | RELAY_LOG_FILE_SYMBOL EQUAL_OPERATOR textStringNoLinebreak
    | RELAY_LOG_POS_SYMBOL EQUAL_OPERATOR ulong_number
    ;

sourceLogFile
    : MASTER_LOG_FILE_SYMBOL
    | SOURCE_LOG_FILE_SYMBOL
    ;

sourceLogPos
    : MASTER_LOG_POS_SYMBOL
    | SOURCE_LOG_POS_SYMBOL
    ;

serverIdList
    : OPEN_PAR_SYMBOL (ulong_number (COMMA_SYMBOL ulong_number)*)? CLOSE_PAR_SYMBOL
    ;

changeReplication
    : CHANGE_SYMBOL REPLICATION_SYMBOL FILTER_SYMBOL filterDefinition (
        COMMA_SYMBOL filterDefinition
    )* channel?
    ;

filterDefinition
    : REPLICATE_DO_DB_SYMBOL EQUAL_OPERATOR OPEN_PAR_SYMBOL filterDbList? CLOSE_PAR_SYMBOL
    | REPLICATE_IGNORE_DB_SYMBOL EQUAL_OPERATOR OPEN_PAR_SYMBOL filterDbList? CLOSE_PAR_SYMBOL
    | REPLICATE_DO_TABLE_SYMBOL EQUAL_OPERATOR OPEN_PAR_SYMBOL filterTableList? CLOSE_PAR_SYMBOL
    | REPLICATE_IGNORE_TABLE_SYMBOL EQUAL_OPERATOR OPEN_PAR_SYMBOL filterTableList? CLOSE_PAR_SYMBOL
    | REPLICATE_WILD_DO_TABLE_SYMBOL EQUAL_OPERATOR OPEN_PAR_SYMBOL filterStringList? CLOSE_PAR_SYMBOL
    | REPLICATE_WILD_IGNORE_TABLE_SYMBOL EQUAL_OPERATOR OPEN_PAR_SYMBOL filterStringList? CLOSE_PAR_SYMBOL
    | REPLICATE_REWRITE_DB_SYMBOL EQUAL_OPERATOR OPEN_PAR_SYMBOL filterDbPairList? CLOSE_PAR_SYMBOL
    ;

filterDbList
    : schemaRef (COMMA_SYMBOL schemaRef)*
    ;

filterTableList
    : filterTableRef (COMMA_SYMBOL filterTableRef)*
    ;

filterStringList
    : filterWildDbTableString (COMMA_SYMBOL filterWildDbTableString)*
    ;

filterWildDbTableString
    : textStringNoLinebreak // sql_yacc.yy checks for the existance of at least one dot char in the string.
    ;

filterDbPairList
    : schemaIdentifierPair (COMMA_SYMBOL schemaIdentifierPair)*
    ;

startReplicaStatement
    : START_SYMBOL replica replicaThreadOptions? (UNTIL_SYMBOL replicaUntil)? userOption? passwordOption? defaultAuthOption?
        pluginDirOption? channel?
    ;

stopReplicaStatement
    : STOP_SYMBOL replica replicaThreadOptions? channel?
    ;

replicaUntil
    : (
        sourceFileDef
        | (SQL_BEFORE_GTIDS_SYMBOL | SQL_AFTER_GTIDS_SYMBOL) EQUAL_OPERATOR textString
        | SQL_AFTER_MTS_GAPS_SYMBOL
    ) (COMMA_SYMBOL sourceFileDef)*
    ;

userOption
    : USER_SYMBOL EQUAL_OPERATOR textString
    ;

passwordOption
    : PASSWORD_SYMBOL EQUAL_OPERATOR textString
    ;

defaultAuthOption
    : DEFAULT_AUTH_SYMBOL EQUAL_OPERATOR textString
    ;

pluginDirOption
    : PLUGIN_DIR_SYMBOL EQUAL_OPERATOR textString
    ;

replicaThreadOptions
    : replicaThreadOption (COMMA_SYMBOL replicaThreadOption)*
    ;

replicaThreadOption
    : SQL_THREAD_SYMBOL
    | RELAY_THREAD_SYMBOL
    ;

groupReplication
    : (START_SYMBOL groupReplicationStartOptions? | STOP_SYMBOL) GROUP_REPLICATION_SYMBOL
    ;

groupReplicationStartOptions
    : groupReplicationStartOption (COMMA_SYMBOL groupReplicationStartOption)*
    ;

groupReplicationStartOption
    : groupReplicationUser
    | groupReplicationPassword
    | groupReplicationPluginAuth
    ;

groupReplicationUser
    : USER_SYMBOL EQUAL_OPERATOR textStringNoLinebreak
    ;

groupReplicationPassword
    : PASSWORD_SYMBOL EQUAL_OPERATOR textStringNoLinebreak
    ;

groupReplicationPluginAuth
    : DEFAULT_AUTH_SYMBOL EQUAL_OPERATOR textStringNoLinebreak
    ;

replica
    : // Part of the terminology cleanup starting with 8.0.24.
    SLAVE_SYMBOL
    | REPLICA_SYMBOL
    ;

//----------------------------------------------------------------------------------------------------------------------

preparedStatement
    : type = PREPARE_SYMBOL identifier FROM_SYMBOL (textLiteral | userVariable)
    | executeStatement
    | type = (DEALLOCATE_SYMBOL | DROP_SYMBOL) PREPARE_SYMBOL identifier
    ;

executeStatement
    : EXECUTE_SYMBOL identifier (USING_SYMBOL executeVarList)?
    ;

executeVarList
    : userVariable (COMMA_SYMBOL userVariable)*
    ;

//----------------------------------------------------------------------------------------------------------------------

cloneStatement
    : CLONE_SYMBOL (
        LOCAL_SYMBOL DATA_SYMBOL DIRECTORY_SYMBOL equal? textStringLiteral
        // Clone remote has been removed in 8.0.14. This alt is taken out by the conditional REMOTE_SYMBOL.
        | REMOTE_SYMBOL (FOR_SYMBOL REPLICATION_SYMBOL)?
        | {this.isServerVersionGe80014()}? INSTANCE_SYMBOL FROM_SYMBOL user COLON_SYMBOL ulong_number IDENTIFIED_SYMBOL BY_SYMBOL
            textStringLiteral dataDirSSL?
    )
    ;

dataDirSSL
    : ssl
    | DATA_SYMBOL DIRECTORY_SYMBOL equal? textStringLiteral ssl?
    ;

ssl
    : REQUIRE_SYMBOL NO_SYMBOL? SSL_SYMBOL
    ;

//----------------------------------------------------------------------------------------------------------------------

// Note: SET PASSWORD is part of the SET statement.
accountManagementStatement
    : alterUserStatement
    | createUserStatement
    | dropUserStatement
    | grantStatement
    | renameUserStatement
    | revokeStatement
    | setRoleStatement
    ;

alterUserStatement
    : ALTER_SYMBOL USER_SYMBOL ifExists? (
        (
            {this.isServerVersionGe80014()}? createUserList
            | {this.isServerVersionGe80014()}? alterUserList
        ) createUserTail
        | userFunction (
            (identifiedByRandomPassword | identifiedByPassword) replacePassword? retainCurrentPassword?
            | DISCARD_SYMBOL OLD_SYMBOL PASSWORD_SYMBOL
            | userRegistration?
        )
        | user (
            DEFAULT_SYMBOL ROLE_SYMBOL (ALL_SYMBOL | NONE_SYMBOL | roleList)
            | userRegistration?
        )
    )
    ;

alterUserList
    : alterUser (COMMA_SYMBOL alterUser)*
    ;

alterUser
    : {this.isServerVersionLt80025()}? oldAlterUser
    | {this.isServerVersionGe80025()}? (
        user (
            identifiedByPassword (
                REPLACE_SYMBOL textStringLiteral retainCurrentPassword?
                | retainCurrentPassword?
            )
            | identifiedByRandomPassword (
                retainCurrentPassword?
                | REPLACE_SYMBOL textStringLiteral retainCurrentPassword?
            )
            | identifiedWithPlugin
            | identifiedWithPluginAsAuth retainCurrentPassword?
            | identifiedWithPluginByPassword (
                REPLACE_SYMBOL textStringLiteral retainCurrentPassword?
                | retainCurrentPassword?
            )
            | identifiedWithPluginByRandomPassword retainCurrentPassword?
            | discardOldPassword?
            | ADD_SYMBOL factor identification (ADD_SYMBOL factor identification)?
            | MODIFY_SYMBOL factor identification (
                MODIFY_SYMBOL factor identification
            )?
            | DROP_SYMBOL factor (DROP_SYMBOL factor)?
        )
    )
    ;

oldAlterUser
    : user IDENTIFIED_SYMBOL BY_SYMBOL (
        textString REPLACE_SYMBOL textString retainCurrentPassword?
        | textString retainCurrentPassword?
        | RANDOM_SYMBOL PASSWORD_SYMBOL (REPLACE_SYMBOL textString)? retainCurrentPassword?
    )
    | user IDENTIFIED_SYMBOL WITH_SYMBOL (
        textOrIdentifier (
            BY_SYMBOL textString REPLACE_SYMBOL textString retainCurrentPassword?
            | AS_SYMBOL textStringHash retainCurrentPassword?
            | BY_SYMBOL textString retainCurrentPassword?
            | BY_SYMBOL RANDOM_SYMBOL PASSWORD_SYMBOL retainCurrentPassword?
        )?
    )
    | user discardOldPassword?
    ;

userFunction
    : USER_SYMBOL parentheses
    ;

createUserStatement
    : CREATE_SYMBOL USER_SYMBOL ifNotExists? createUserList defaultRoleClause? createUserTail
    ;

createUserTail
    : requireClause? connectOptions? accountLockPasswordExpireOptions* (
        {this.isServerVersionGe80024()}? userAttributes
    )?
    ;

userAttributes
    : ATTRIBUTE_SYMBOL textStringLiteral
    | COMMENT_SYMBOL textStringLiteral
    ;

defaultRoleClause
    : DEFAULT_SYMBOL ROLE_SYMBOL roleList
    ;

requireClause
    : REQUIRE_SYMBOL (
        requireList
        | option = (SSL_SYMBOL | X509_SYMBOL | NONE_SYMBOL)
    )
    ;

connectOptions
    : WITH_SYMBOL (
        MAX_QUERIES_PER_HOUR_SYMBOL ulong_number
        | MAX_UPDATES_PER_HOUR_SYMBOL ulong_number
        | MAX_CONNECTIONS_PER_HOUR_SYMBOL ulong_number
        | MAX_USER_CONNECTIONS_SYMBOL ulong_number
    )+
    ;

accountLockPasswordExpireOptions
    : ACCOUNT_SYMBOL (LOCK_SYMBOL | UNLOCK_SYMBOL)
    | PASSWORD_SYMBOL (
        EXPIRE_SYMBOL (
            INTERVAL_SYMBOL real_ulong_number DAY_SYMBOL
            | NEVER_SYMBOL
            | DEFAULT_SYMBOL
        )?
        | HISTORY_SYMBOL (real_ulong_number | DEFAULT_SYMBOL)
        | REUSE_SYMBOL INTERVAL_SYMBOL (
            real_ulong_number DAY_SYMBOL
            | DEFAULT_SYMBOL
        )
        | {this.isServerVersionGe80014()}? REQUIRE_SYMBOL CURRENT_SYMBOL (
            DEFAULT_SYMBOL
            | OPTIONAL_SYMBOL
        )?
    )
    | FAILED_LOGIN_ATTEMPTS_SYMBOL real_ulong_number
    | PASSWORD_LOCK_TIME_SYMBOL (real_ulong_number | UNBOUNDED_SYMBOL)
    ;

userAttribute
    :
    | ATTRIBUTE_SYMBOL textStringLiteral
    | COMMENT_SYMBOL textStringLiteral
    ;

dropUserStatement
    : DROP_SYMBOL USER_SYMBOL ifExists? userList
    ;

grantStatement
    : GRANT_SYMBOL (
        roleOrPrivilegesList TO_SYMBOL userList (
            WITH_SYMBOL ADMIN_SYMBOL OPTION_SYMBOL
        )?
        | (roleOrPrivilegesList | ALL_SYMBOL PRIVILEGES_SYMBOL?) ON_SYMBOL aclType? grantIdentifier TO_SYMBOL grantTargetList
            versionedRequireClause? grantOptions? grantAs?
        | PROXY_SYMBOL ON_SYMBOL user TO_SYMBOL grantTargetList (
            WITH_SYMBOL GRANT_SYMBOL OPTION_SYMBOL
        )?
    )
    ;

grantTargetList
    : {this.isServerVersionLt80011()}? createUserList
    | {this.isServerVersionGe80011()}? userList
    ;

grantOptions
    : WITH_SYMBOL grantOption
    ;

exceptRoleList
    : EXCEPT_SYMBOL roleList
    ;

withRoles
    : WITH_SYMBOL ROLE_SYMBOL (
        roleList
        | ALL_SYMBOL exceptRoleList?
        | NONE_SYMBOL
        | DEFAULT_SYMBOL
    )
    ;

grantAs
    : AS_SYMBOL USER_SYMBOL withRoles?
    ;

versionedRequireClause
    : {this.isServerVersionLt80011()}? requireClause
    ;

renameUserStatement
    : RENAME_SYMBOL USER_SYMBOL user TO_SYMBOL user (
        COMMA_SYMBOL user TO_SYMBOL user
    )*
    ;

revokeStatement
    : REVOKE_SYMBOL ({this.isServerVersionGe80031()}? ifExists)? (
        roleOrPrivilegesList FROM_SYMBOL userList
        | roleOrPrivilegesList ON_SYMBOL aclType? grantIdentifier FROM_SYMBOL userList
        | ALL_SYMBOL PRIVILEGES_SYMBOL? (
            ON_SYMBOL aclType? grantIdentifier
            | COMMA_SYMBOL GRANT_SYMBOL OPTION_SYMBOL
        ) FROM_SYMBOL userList
        | PROXY_SYMBOL ON_SYMBOL user FROM_SYMBOL userList
    ) ({this.isServerVersionGe80031()}? ignoreUnknownUser)?
    ;

aclType
    : TABLE_SYMBOL
    | FUNCTION_SYMBOL
    | PROCEDURE_SYMBOL
    ;

roleOrPrivilegesList
    : roleOrPrivilege (COMMA_SYMBOL roleOrPrivilege)*
    ;

roleOrPrivilege
    : (
        roleIdentifierOrText columnInternalRefList?
        | roleIdentifierOrText (AT_TEXT_SUFFIX | AT_SIGN_SYMBOL textOrIdentifier)
    )
    | (SELECT_SYMBOL | INSERT_SYMBOL | UPDATE_SYMBOL | REFERENCES_SYMBOL) columnInternalRefList?
    | (
        DELETE_SYMBOL
        | USAGE_SYMBOL
        | INDEX_SYMBOL
        | DROP_SYMBOL
        | EXECUTE_SYMBOL
        | RELOAD_SYMBOL
        | SHUTDOWN_SYMBOL
        | PROCESS_SYMBOL
        | FILE_SYMBOL
        | PROXY_SYMBOL
        | SUPER_SYMBOL
        | EVENT_SYMBOL
        | TRIGGER_SYMBOL
    )
    | GRANT_SYMBOL OPTION_SYMBOL
    | SHOW_SYMBOL DATABASES_SYMBOL
    | CREATE_SYMBOL (
        TEMPORARY_SYMBOL object = TABLES_SYMBOL
        | object = (ROUTINE_SYMBOL | TABLESPACE_SYMBOL | USER_SYMBOL | VIEW_SYMBOL)
    )?
    | LOCK_SYMBOL TABLES_SYMBOL
    | REPLICATION_SYMBOL (CLIENT_SYMBOL | replica)
    | SHOW_SYMBOL VIEW_SYMBOL
    | ALTER_SYMBOL ROUTINE_SYMBOL?
    | (CREATE_SYMBOL | DROP_SYMBOL) ROLE_SYMBOL
    ;

grantIdentifier
    : MULT_OPERATOR (DOT_SYMBOL MULT_OPERATOR)?
    | schemaRef (DOT_SYMBOL MULT_OPERATOR)?
    | tableRef
    | {this.isServerVersionGe80017()}? schemaRef DOT_SYMBOL tableRef
    ;

requireList
    : requireListElement (AND_SYMBOL? requireListElement)*
    ;

requireListElement
    : element = CIPHER_SYMBOL textString
    | element = ISSUER_SYMBOL textString
    | element = SUBJECT_SYMBOL textString
    ;

grantOption
    : option = GRANT_SYMBOL OPTION_SYMBOL
    | {this.isServerVersionLt80011()}? (
        option = MAX_QUERIES_PER_HOUR_SYMBOL ulong_number
        | option = MAX_UPDATES_PER_HOUR_SYMBOL ulong_number
        | option = MAX_CONNECTIONS_PER_HOUR_SYMBOL ulong_number
        | option = MAX_USER_CONNECTIONS_SYMBOL ulong_number
    )
    ;

setRoleStatement
    : SET_SYMBOL ROLE_SYMBOL roleList
    | SET_SYMBOL ROLE_SYMBOL (NONE_SYMBOL | DEFAULT_SYMBOL)
    | SET_SYMBOL DEFAULT_SYMBOL ROLE_SYMBOL (roleList | NONE_SYMBOL | ALL_SYMBOL) TO_SYMBOL roleList
    | SET_SYMBOL ROLE_SYMBOL ALL_SYMBOL (EXCEPT_SYMBOL roleList)?
    ;

roleList
    : role (COMMA_SYMBOL role)*
    ;

role
    : roleIdentifierOrText userVariable?
    ;

//----------------------------------------------------------------------------------------------------------------------

tableAdministrationStatement
    : type = ANALYZE_SYMBOL noWriteToBinLog? TABLE_SYMBOL tableRefList histogram?
    | type = CHECK_SYMBOL TABLE_SYMBOL tableRefList checkOption*
    | type = CHECKSUM_SYMBOL TABLE_SYMBOL tableRefList (
        QUICK_SYMBOL
        | EXTENDED_SYMBOL
    )?
    | type = OPTIMIZE_SYMBOL noWriteToBinLog? (TABLE_SYMBOL | TABLES_SYMBOL) tableRefList
    | type = REPAIR_SYMBOL noWriteToBinLog? TABLE_SYMBOL tableRefList repairType*
    ;

histogramAutoUpdate
    : {this.isServerVersionGe80200()}? (MANUAL_SYMBOL | AUTO_SYMBOL) UPDATE_SYMBOL
    ;

histogramUpdateParam
    : histogramNumBuckets? histogramAutoUpdate?
    | {this.isServerVersionGe80031()}? USING_SYMBOL DATA_SYMBOL textStringLiteral
    ;

histogramNumBuckets
    : {this.isServerVersionGe80200()}? WITH_SYMBOL INT_NUMBER BUCKETS_SYMBOL
    ;

histogram
    : UPDATE_SYMBOL HISTOGRAM_SYMBOL ON_SYMBOL identifierList histogramUpdateParam
    | DROP_SYMBOL HISTOGRAM_SYMBOL ON_SYMBOL identifierList
    ;

checkOption
    : FOR_SYMBOL UPGRADE_SYMBOL
    | (QUICK_SYMBOL | FAST_SYMBOL | MEDIUM_SYMBOL | EXTENDED_SYMBOL | CHANGED_SYMBOL)
    ;

repairType
    : QUICK_SYMBOL
    | EXTENDED_SYMBOL
    | USE_FRM_SYMBOL
    ;

//----------------------------------------------------------------------------------------------------------------------

uninstallStatement
    : UNINSTALL_SYMBOL (
        PLUGIN_SYMBOL pluginRef
        | COMPONENT_SYMBOL componentRef (COMMA_SYMBOL componentRef)*
    )
    ;

installStatement
    : INSTALL_SYMBOL (
        PLUGIN_SYMBOL identifier SONAME_SYMBOL textStringLiteral
        | COMPONENT_SYMBOL textStringLiteralList installSetValueList?
    )
    ;

installOptionType
    : GLOBAL_SYMBOL
    | PERSIST_SYMBOL
    ;

installSetRvalue
    : expr
    | ON_SYMBOL
    ;

installSetValue
    : installOptionType lvalueVariable equal installSetRvalue
    ;

installSetValueList
    : {this.isServerVersionGe80032()}? SET_SYMBOL installSetValue (
        COMMA_SYMBOL installSetValue
    )*
    ;

//----------------------------------------------------------------------------------------------------------------------

setStatement
    : SET_SYMBOL startOptionValueList
    ;

startOptionValueList
    : optionValueNoOptionType optionValueListContinued
    | TRANSACTION_SYMBOL transactionCharacteristics
    | optionType startOptionValueListFollowingOptionType
    | PASSWORD_SYMBOL (FOR_SYMBOL user)? equal (
        textString replacePassword? retainCurrentPassword?
        | textString replacePassword? retainCurrentPassword?
        | {this.isServerVersionLt80014()}? PASSWORD_SYMBOL OPEN_PAR_SYMBOL textString CLOSE_PAR_SYMBOL
    )
    | {this.isServerVersionGe80018()}? PASSWORD_SYMBOL (FOR_SYMBOL user)? TO_SYMBOL RANDOM_SYMBOL replacePassword?
        retainCurrentPassword?
    ;

transactionCharacteristics
    : transactionAccessMode isolationLevel?
    | isolationLevel (COMMA_SYMBOL transactionAccessMode)?
    ;

transactionAccessMode
    : READ_SYMBOL (WRITE_SYMBOL | ONLY_SYMBOL)
    ;

isolationLevel
    : ISOLATION_SYMBOL LEVEL_SYMBOL (
        REPEATABLE_SYMBOL READ_SYMBOL
        | READ_SYMBOL (COMMITTED_SYMBOL | UNCOMMITTED_SYMBOL)
        | SERIALIZABLE_SYMBOL
    )
    ;

optionValueListContinued
    : (COMMA_SYMBOL optionValue)*
    ;

optionValueNoOptionType
    : lvalueVariable equal setExprOrDefault
    | charset (charsetName | DEFAULT_SYMBOL)
    | userVariable equal expr
    | AT_AT_SIGN_SYMBOL setVarIdentType? lvalueVariable equal setExprOrDefault
    | NAMES_SYMBOL (
        equal expr
        | charsetName collate?
        | {this.isServerVersionGe80011()}? DEFAULT_SYMBOL
    )
    ;

optionValue
    : optionType lvalueVariable equal setExprOrDefault
    | optionValueNoOptionType
    ;

setSystemVariable
    : AT_AT_SIGN_SYMBOL setVarIdentType? lvalueVariable
    ;

startOptionValueListFollowingOptionType
    : optionValueFollowingOptionType optionValueListContinued
    | TRANSACTION_SYMBOL transactionCharacteristics
    ;

optionValueFollowingOptionType
    : lvalueVariable equal setExprOrDefault
    ;

setExprOrDefault
    : expr
    | DEFAULT_SYMBOL
    | ON_SYMBOL
    | ALL_SYMBOL
    | BINARY_SYMBOL
    | ROW_SYMBOL
    | SYSTEM_SYMBOL
    ;

//----------------------------------------------------------------------------------------------------------------------

showDatabasesStatement
    : SHOW_SYMBOL DATABASES_SYMBOL likeOrWhere?
    ;

showTablesStatement
    : SHOW_SYMBOL showCommandType? value = TABLES_SYMBOL inDb? likeOrWhere?
    ;

showTriggersStatement
    : SHOW_SYMBOL FULL_SYMBOL? TRIGGERS_SYMBOL inDb? likeOrWhere?
    ;

showEventsStatement
    : SHOW_SYMBOL EVENTS_SYMBOL inDb? likeOrWhere?
    ;

showTableStatusStatement
    : SHOW_SYMBOL TABLE_SYMBOL STATUS_SYMBOL inDb? likeOrWhere?
    ;

showOpenTablesStatement
    : SHOW_SYMBOL OPEN_SYMBOL TABLES_SYMBOL inDb? likeOrWhere?
    ;

showParseTreeStatement
    : {this.isServerVersionGe80100()}? SHOW_SYMBOL PARSE_TREE_SYMBOL simpleStatement
    ;

showPluginsStatement
    : SHOW_SYMBOL PLUGINS_SYMBOL
    ;

showEngineLogsStatement
    : SHOW_SYMBOL ENGINE_SYMBOL engineOrAll LOGS_SYMBOL
    ;

showEngineMutexStatement
    : SHOW_SYMBOL ENGINE_SYMBOL engineOrAll MUTEX_SYMBOL
    ;

showEngineStatusStatement
    : SHOW_SYMBOL ENGINE_SYMBOL engineOrAll STATUS_SYMBOL
    ;

showColumnsStatement
    : SHOW_SYMBOL showCommandType? COLUMNS_SYMBOL (FROM_SYMBOL | IN_SYMBOL) tableRef inDb? likeOrWhere?
    ;

showBinaryLogsStatement
    : SHOW_SYMBOL (BINARY_SYMBOL | MASTER_SYMBOL) value = LOGS_SYMBOL
    ;

showBinaryLogStatusStatement
    : SHOW_SYMBOL BINARY_SYMBOL LOG_SYMBOL STATUS_SYMBOL
    ;

showReplicasStatement
    : SHOW_SYMBOL (replica HOSTS_SYMBOL | REPLICAS_SYMBOL)
    ;

showBinlogEventsStatement
    : SHOW_SYMBOL BINLOG_SYMBOL EVENTS_SYMBOL (IN_SYMBOL textString)? (
        FROM_SYMBOL ulonglongNumber
    )? limitClause? channel?
    ;

showRelaylogEventsStatement
    : SHOW_SYMBOL RELAYLOG_SYMBOL EVENTS_SYMBOL (IN_SYMBOL textString)? (
        FROM_SYMBOL ulonglongNumber
    )? limitClause? channel?
    ;

showKeysStatement
    : SHOW_SYMBOL EXTENDED_SYMBOL? (INDEX_SYMBOL | INDEXES_SYMBOL | KEYS_SYMBOL) fromOrIn tableRef inDb? whereClause?
    ;

showEnginesStatement
    : SHOW_SYMBOL STORAGE_SYMBOL? value = ENGINES_SYMBOL
    ;

showCountWarningsStatement
    : SHOW_SYMBOL COUNT_SYMBOL OPEN_PAR_SYMBOL MULT_OPERATOR CLOSE_PAR_SYMBOL WARNINGS_SYMBOL
    ;

showCountErrorsStatement
    : SHOW_SYMBOL COUNT_SYMBOL OPEN_PAR_SYMBOL MULT_OPERATOR CLOSE_PAR_SYMBOL ERRORS_SYMBOL
    ;

showWarningsStatement
    : SHOW_SYMBOL WARNINGS_SYMBOL limitClause?
    ;

showErrorsStatement
    : SHOW_SYMBOL ERRORS_SYMBOL limitClause?
    ;

showProfilesStatement
    : SHOW_SYMBOL PROFILES_SYMBOL
    ;

showProfileStatement
    : SHOW_SYMBOL PROFILE_SYMBOL profileDefinitions? (
        FOR_SYMBOL QUERY_SYMBOL INT_NUMBER
    )? limitClause?
    ;

showStatusStatement
    : SHOW_SYMBOL optionType? STATUS_SYMBOL likeOrWhere?
    ;

showProcessListStatement
    : SHOW_SYMBOL FULL_SYMBOL? PROCESSLIST_SYMBOL
    ;

showVariablesStatement
    : SHOW_SYMBOL optionType? VARIABLES_SYMBOL likeOrWhere?
    ;

showCharacterSetStatement
    : SHOW_SYMBOL charset likeOrWhere?
    ;

showCollationStatement
    : SHOW_SYMBOL COLLATION_SYMBOL likeOrWhere?
    ;

showPrivilegesStatement
    : SHOW_SYMBOL PRIVILEGES_SYMBOL
    ;

showGrantsStatement
    : SHOW_SYMBOL GRANTS_SYMBOL (FOR_SYMBOL user (USING_SYMBOL userList)?)?
    ;

showCreateDatabaseStatement
    : SHOW_SYMBOL CREATE_SYMBOL DATABASE_SYMBOL ifNotExists? schemaRef
    ;

showCreateTableStatement
    : SHOW_SYMBOL CREATE_SYMBOL TABLE_SYMBOL tableRef
    ;

showCreateViewStatement
    : SHOW_SYMBOL CREATE_SYMBOL VIEW_SYMBOL viewRef
    ;

showMasterStatusStatement
    : SHOW_SYMBOL MASTER_SYMBOL STATUS_SYMBOL
    ;

showReplicaStatusStatement
    : SHOW_SYMBOL replica STATUS_SYMBOL channel?
    ;

showCreateProcedureStatement
    : SHOW_SYMBOL CREATE_SYMBOL PROCEDURE_SYMBOL procedureRef
    ;

showCreateFunctionStatement
    : SHOW_SYMBOL CREATE_SYMBOL FUNCTION_SYMBOL functionRef
    ;

showCreateTriggerStatement
    : SHOW_SYMBOL CREATE_SYMBOL TRIGGER_SYMBOL triggerRef
    ;

showCreateProcedureStatusStatement
    : SHOW_SYMBOL CREATE_SYMBOL PROCEDURE_SYMBOL STATUS_SYMBOL likeOrWhere?
    ;

showCreateFunctionStatusStatement
    : SHOW_SYMBOL CREATE_SYMBOL FUNCTION_SYMBOL STATUS_SYMBOL likeOrWhere?
    ;

showCreateProcedureCodeStatement
    : SHOW_SYMBOL CREATE_SYMBOL PROCEDURE_SYMBOL CODE_SYMBOL procedureRef
    ;

showCreateFunctionCodeStatement
    : SHOW_SYMBOL CREATE_SYMBOL FUNCTION_SYMBOL CODE_SYMBOL functionRef
    ;

showCreateEventStatement
    : SHOW_SYMBOL CREATE_SYMBOL EVENT_SYMBOL eventRef
    ;

showCreateUserStatement
    : SHOW_SYMBOL CREATE_SYMBOL USER_SYMBOL user
    ;

showCommandType
    : FULL_SYMBOL
    | EXTENDED_SYMBOL FULL_SYMBOL?
    ;

engineOrAll
    : engineRef
    | ALL_SYMBOL
    ;

fromOrIn
    : FROM_SYMBOL
    | IN_SYMBOL
    ;

inDb
    : fromOrIn identifier
    ;

profileDefinitions
    : profileDefinition (COMMA_SYMBOL profileDefinition)*
    ;

profileDefinition
    : BLOCK_SYMBOL IO_SYMBOL
    | CONTEXT_SYMBOL SWITCHES_SYMBOL
    | PAGE_SYMBOL FAULTS_SYMBOL
    | (
        ALL_SYMBOL
        | CPU_SYMBOL
        | IPC_SYMBOL
        | MEMORY_SYMBOL
        | SOURCE_SYMBOL
        | SWAPS_SYMBOL
    )
    ;

//----------------------------------------------------------------------------------------------------------------------

otherAdministrativeStatement
    : type = BINLOG_SYMBOL textLiteral
    | type = CACHE_SYMBOL INDEX_SYMBOL keyCacheListOrParts IN_SYMBOL (
        identifier
        | DEFAULT_SYMBOL
    )
    | type = FLUSH_SYMBOL noWriteToBinLog? (
        flushTables
        | flushOption (COMMA_SYMBOL flushOption)*
    )
    | type = KILL_SYMBOL (CONNECTION_SYMBOL | QUERY_SYMBOL)? expr
    | type = LOAD_SYMBOL INDEX_SYMBOL INTO_SYMBOL CACHE_SYMBOL preloadTail
    | type = SHUTDOWN_SYMBOL
    ;

keyCacheListOrParts
    : keyCacheList
    | assignToKeycachePartition
    ;

keyCacheList
    : assignToKeycache (COMMA_SYMBOL assignToKeycache)*
    ;

assignToKeycache
    : tableRef cacheKeyList?
    ;

assignToKeycachePartition
    : tableRef PARTITION_SYMBOL OPEN_PAR_SYMBOL allOrPartitionNameList CLOSE_PAR_SYMBOL cacheKeyList?
    ;

cacheKeyList
    : keyOrIndex OPEN_PAR_SYMBOL keyUsageList? CLOSE_PAR_SYMBOL
    ;

keyUsageElement
    : identifier
    | PRIMARY_SYMBOL
    ;

keyUsageList
    : keyUsageElement (COMMA_SYMBOL keyUsageElement)*
    ;

flushOption
    : option = (
        HOSTS_SYMBOL
        | PRIVILEGES_SYMBOL
        | STATUS_SYMBOL
        | USER_RESOURCES_SYMBOL
    )
    | logType? option = LOGS_SYMBOL
    | option = RELAY_SYMBOL LOGS_SYMBOL channel?
    | option = OPTIMIZER_COSTS_SYMBOL
    ;

logType
    : BINARY_SYMBOL
    | ENGINE_SYMBOL
    | ERROR_SYMBOL
    | GENERAL_SYMBOL
    | SLOW_SYMBOL
    ;

flushTables
    : (TABLES_SYMBOL | TABLE_SYMBOL) (
        WITH_SYMBOL READ_SYMBOL LOCK_SYMBOL
        | identifierList flushTablesOptions?
    )?
    ;

flushTablesOptions
    : FOR_SYMBOL EXPORT_SYMBOL
    | WITH_SYMBOL READ_SYMBOL LOCK_SYMBOL
    ;

preloadTail
    : tableRef adminPartition cacheKeyList? (IGNORE_SYMBOL LEAVES_SYMBOL)?
    | preloadList
    ;

preloadList
    : preloadKeys (COMMA_SYMBOL preloadKeys)*
    ;

preloadKeys
    : tableRef cacheKeyList? (IGNORE_SYMBOL LEAVES_SYMBOL)?
    ;

adminPartition
    : PARTITION_SYMBOL OPEN_PAR_SYMBOL allOrPartitionNameList CLOSE_PAR_SYMBOL
    ;

//----------------------------------------------------------------------------------------------------------------------

resourceGroupManagement
    : createResourceGroup
    | alterResourceGroup
    | setResourceGroup
    | dropResourceGroup
    ;

createResourceGroup
    : CREATE_SYMBOL RESOURCE_SYMBOL GROUP_SYMBOL identifier TYPE_SYMBOL equal? (
        USER_SYMBOL
        | SYSTEM_SYMBOL
    ) resourceGroupVcpuList? resourceGroupPriority? resourceGroupEnableDisable?
    ;

resourceGroupVcpuList
    : VCPU_SYMBOL equal? vcpuNumOrRange (COMMA_SYMBOL? vcpuNumOrRange)*
    ;

vcpuNumOrRange
    : INT_NUMBER (MINUS_OPERATOR INT_NUMBER)?
    ;

resourceGroupPriority
    : THREAD_PRIORITY_SYMBOL equal? INT_NUMBER
    ;

resourceGroupEnableDisable
    : ENABLE_SYMBOL
    | DISABLE_SYMBOL
    ;

alterResourceGroup
    : ALTER_SYMBOL RESOURCE_SYMBOL GROUP_SYMBOL resourceGroupRef resourceGroupVcpuList? resourceGroupPriority?
        resourceGroupEnableDisable? FORCE_SYMBOL?
    ;

setResourceGroup
    : SET_SYMBOL RESOURCE_SYMBOL GROUP_SYMBOL identifier (FOR_SYMBOL threadIdList)?
    ;

threadIdList
    : real_ulong_number (COMMA_SYMBOL? real_ulong_number)*
    ;

dropResourceGroup
    : DROP_SYMBOL RESOURCE_SYMBOL GROUP_SYMBOL resourceGroupRef FORCE_SYMBOL?
    ;

//----------------------------------------------------------------------------------------------------------------------

utilityStatement
    : describeStatement
    | explainStatement
    | helpCommand
    | useCommand
    | {this.isServerVersionGe80011()}? restartServer
    ;

describeStatement
    : (EXPLAIN_SYMBOL | DESCRIBE_SYMBOL | DESC_SYMBOL) tableRef (
        textString
        | columnRef
    )?
    ;

explainStatement
    : (EXPLAIN_SYMBOL | DESCRIBE_SYMBOL | DESC_SYMBOL) explainOptions? (
        {this.isServerVersionGe80032()}? FOR_SYMBOL DATABASE_SYMBOL textOrIdentifier
    )? explainableStatement
    ;

explainOptions
    : FORMAT_SYMBOL EQUAL_OPERATOR textOrIdentifier (
        {this.isServerVersionGe80032()}? explainInto
    )?
    | {this.isServerVersionLt80012()}? EXTENDED_SYMBOL
    | {this.isServerVersionGe80018()}? ANALYZE_SYMBOL
    | {this.isServerVersionGe80019()}? ANALYZE_SYMBOL FORMAT_SYMBOL EQUAL_OPERATOR textOrIdentifier
    ;

explainableStatement
    : selectStatement
    | deleteStatement
    | insertStatement
    | replaceStatement
    | updateStatement
    | FOR_SYMBOL CONNECTION_SYMBOL real_ulong_number
    ;

explainInto
    : INTO_SYMBOL AT_SIGN_SYMBOL textOrIdentifier
    ;

helpCommand
    : HELP_SYMBOL textOrIdentifier
    ;

useCommand
    : USE_SYMBOL schemaRef
    ;

restartServer
    : RESTART_SYMBOL
    ;

//----------------- Expression support ---------------------------------------------------------------------------------

expr
    : boolPri (
        IS_SYMBOL notRule? type = (TRUE_SYMBOL | FALSE_SYMBOL | UNKNOWN_SYMBOL)
    )?                                                   # exprIs
    | NOT_SYMBOL expr                                    # exprNot
    | expr op = (AND_SYMBOL | LOGICAL_AND_OPERATOR) expr # exprAnd
    | expr XOR_SYMBOL expr                               # exprXor
    | expr op = (OR_SYMBOL | LOGICAL_OR_OPERATOR) expr   # exprOr
    ;

boolPri
    : predicate                                         # primaryExprPredicate
    | boolPri IS_SYMBOL notRule? NULL_SYMBOL            # primaryExprIsNull
    | boolPri compOp predicate                          # primaryExprCompare
    | boolPri compOp (ALL_SYMBOL | ANY_SYMBOL) subquery # primaryExprAllAny
    ;

compOp
    : EQUAL_OPERATOR
    | NULL_SAFE_EQUAL_OPERATOR
    | GREATER_OR_EQUAL_OPERATOR
    | GREATER_THAN_OPERATOR
    | LESS_OR_EQUAL_OPERATOR
    | LESS_THAN_OPERATOR
    | NOT_EQUAL_OPERATOR
    ;

predicate
    : bitExpr (
        notRule? predicateOperations
        | {this.isServerVersionGe80017()}? MEMBER_SYMBOL OF_SYMBOL? simpleExprWithParentheses
        | SOUNDS_SYMBOL LIKE_SYMBOL bitExpr
    )?
    ;

predicateOperations
    : IN_SYMBOL (subquery | OPEN_PAR_SYMBOL exprList CLOSE_PAR_SYMBOL) # predicateExprIn
    | BETWEEN_SYMBOL bitExpr AND_SYMBOL predicate                      # predicateExprBetween
    | LIKE_SYMBOL simpleExpr (ESCAPE_SYMBOL simpleExpr)?               # predicateExprLike
    | REGEXP_SYMBOL bitExpr                                            # predicateExprRegex
    ;

bitExpr
    : simpleExpr
    | bitExpr op = BITWISE_XOR_OPERATOR bitExpr
    | bitExpr op = (
        MULT_OPERATOR
        | DIV_OPERATOR
        | MOD_OPERATOR
        | DIV_SYMBOL
        | MOD_SYMBOL
    ) bitExpr
    | bitExpr op = (PLUS_OPERATOR | MINUS_OPERATOR) bitExpr
    | bitExpr op = (PLUS_OPERATOR | MINUS_OPERATOR) INTERVAL_SYMBOL expr interval
    | bitExpr op = (SHIFT_LEFT_OPERATOR | SHIFT_RIGHT_OPERATOR) bitExpr
    | bitExpr op = BITWISE_AND_OPERATOR bitExpr
    | bitExpr op = BITWISE_OR_OPERATOR bitExpr
    ;

// $antlr-format groupedAlignments off
simpleExpr
    : columnRef jsonOperator?                                                                                   # simpleExprColumnRef
    | runtimeFunctionCall                                                                                       # simpleExprRuntimeFunction
    | functionCall                                                                                              # simpleExprFunction
    | simpleExpr COLLATE_SYMBOL textOrIdentifier                                                                # simpleExprCollate
    | literalOrNull                                                                                             # simpleExprLiteral
    | PARAM_MARKER                                                                                              # simpleExprParamMarker
    | rvalueSystemOrUserVariable                                                                                # simpleExpressionRValue
    | inExpressionUserVariableAssignment                                                                        # simpleExprUserVariableAssignment
    | sumExpr                                                                                                   # simpleExprSum
    | groupingOperation                                                                                         # simpleExprGroupingOperation
    | windowFunctionCall                                                                                        # simpleExprWindowingFunction
    | simpleExpr CONCAT_PIPES_SYMBOL simpleExpr                                                                 # simpleExprConcat
    | op = (PLUS_OPERATOR | MINUS_OPERATOR | BITWISE_NOT_OPERATOR) simpleExpr                                   # simpleExprUnary
    | not2Rule simpleExpr                                                                                       # simpleExprNot
    | ROW_SYMBOL? OPEN_PAR_SYMBOL exprList CLOSE_PAR_SYMBOL                                                     # simpleExprList
    | EXISTS_SYMBOL? subquery                                                                                   # simpleExprSubQuery
    | OPEN_CURLY_SYMBOL identifier expr CLOSE_CURLY_SYMBOL                                                      # simpleExprOdbc
    | MATCH_SYMBOL identListArg AGAINST_SYMBOL OPEN_PAR_SYMBOL bitExpr fulltextOptions? CLOSE_PAR_SYMBOL        # simpleExprMatch
    | BINARY_SYMBOL simpleExpr                                                                                  # simpleExprBinary
    | CAST_SYMBOL OPEN_PAR_SYMBOL expr (AT_SYMBOL LOCAL_SYMBOL)? AS_SYMBOL castType arrayCast? CLOSE_PAR_SYMBOL # simpleExprCast
    | CAST_SYMBOL OPEN_PAR_SYMBOL expr AT_SYMBOL TIME_SYMBOL ZONE_SYMBOL INTERVAL_SYMBOL? textStringLiteral AS_SYMBOL
        DATETIME_SYMBOL typeDatetimePrecision CLOSE_PAR_SYMBOL                                                  # simpleExprCastTime
    | CASE_SYMBOL expr? (whenExpression thenExpression)+ elseExpression? END_SYMBOL                             # simpleExprCase
    | CONVERT_SYMBOL OPEN_PAR_SYMBOL expr COMMA_SYMBOL castType CLOSE_PAR_SYMBOL                                # simpleExprConvert
    | CONVERT_SYMBOL OPEN_PAR_SYMBOL expr USING_SYMBOL charsetName CLOSE_PAR_SYMBOL                             # simpleExprConvertUsing
    | DEFAULT_SYMBOL OPEN_PAR_SYMBOL simpleIdentifier CLOSE_PAR_SYMBOL                                          # simpleExprDefault
    | VALUES_SYMBOL OPEN_PAR_SYMBOL simpleIdentifier CLOSE_PAR_SYMBOL                                           # simpleExprValues
    | INTERVAL_SYMBOL expr interval PLUS_OPERATOR expr                                                          # simpleExprInterval
    ;

// $antlr-format groupedAlignments on

arrayCast
    : {this.isServerVersionGe80017()}? ARRAY_SYMBOL
    ;

jsonOperator
    : JSON_SEPARATOR_SYMBOL textStringLiteral
    | JSON_UNQUOTED_SEPARATOR_SYMBOL textStringLiteral
    ;

sumExpr
    : name = AVG_SYMBOL OPEN_PAR_SYMBOL DISTINCT_SYMBOL? inSumExpr CLOSE_PAR_SYMBOL windowingClause?
    | name = (BIT_AND_SYMBOL | BIT_OR_SYMBOL | BIT_XOR_SYMBOL) OPEN_PAR_SYMBOL inSumExpr CLOSE_PAR_SYMBOL windowingClause?
    | jsonFunction
    | name = ST_COLLECT_SYMBOL OPEN_PAR_SYMBOL DISTINCT_SYMBOL? inSumExpr CLOSE_PAR_SYMBOL windowingClause?
    | name = COUNT_SYMBOL OPEN_PAR_SYMBOL (
        ALL_SYMBOL? MULT_OPERATOR
        | inSumExpr
        | DISTINCT_SYMBOL exprList
    ) CLOSE_PAR_SYMBOL windowingClause?
    | name = (MIN_SYMBOL | MAX_SYMBOL) OPEN_PAR_SYMBOL DISTINCT_SYMBOL? inSumExpr CLOSE_PAR_SYMBOL windowingClause?
    | name = (
        STD_SYMBOL
        | VARIANCE_SYMBOL
        | STDDEV_SAMP_SYMBOL
        | VAR_SAMP_SYMBOL
        | SUM_SYMBOL
    ) OPEN_PAR_SYMBOL inSumExpr CLOSE_PAR_SYMBOL windowingClause?
    | name = SUM_SYMBOL OPEN_PAR_SYMBOL DISTINCT_SYMBOL inSumExpr CLOSE_PAR_SYMBOL windowingClause?
    | name = GROUP_CONCAT_SYMBOL OPEN_PAR_SYMBOL DISTINCT_SYMBOL? exprList orderClause? (
        SEPARATOR_SYMBOL textString
    )? CLOSE_PAR_SYMBOL windowingClause?
    ;

groupingOperation
    : GROUPING_SYMBOL OPEN_PAR_SYMBOL exprList CLOSE_PAR_SYMBOL
    ;

windowFunctionCall
    : (
        ROW_NUMBER_SYMBOL
        | RANK_SYMBOL
        | DENSE_RANK_SYMBOL
        | CUME_DIST_SYMBOL
        | PERCENT_RANK_SYMBOL
    ) parentheses windowingClause
    | NTILE_SYMBOL (
        OPEN_PAR_SYMBOL stableInteger CLOSE_PAR_SYMBOL
        | {this.isServerVersionLt80024()}? simpleExprWithParentheses
    ) windowingClause
    | (LEAD_SYMBOL | LAG_SYMBOL) OPEN_PAR_SYMBOL expr leadLagInfo? CLOSE_PAR_SYMBOL nullTreatment? windowingClause
    | (FIRST_VALUE_SYMBOL | LAST_VALUE_SYMBOL) exprWithParentheses nullTreatment? windowingClause
    | NTH_VALUE_SYMBOL OPEN_PAR_SYMBOL expr COMMA_SYMBOL simpleExpr CLOSE_PAR_SYMBOL (
        FROM_SYMBOL (FIRST_SYMBOL | LAST_SYMBOL)
    )? nullTreatment? windowingClause
    ;

samplingMethod
    : SYSTEM_SYMBOL
    | BERNOULLI_SYMBOL
    ;

samplingPercentage
    : ulonglongNumber
    | AT_SIGN_SYMBOL textOrIdentifier
    | PARAM_MARKER
    ;

tablesampleClause
    : {this.isServerVersionGe80200()}? TABLESAMPLE_SYMBOL samplingMethod OPEN_PAR_SYMBOL samplingPercentage CLOSE_PAR_SYMBOL
    ;

windowingClause
    : OVER_SYMBOL (windowName | windowSpec)
    ;

leadLagInfo
    : COMMA_SYMBOL (
        ulonglongNumber
        | PARAM_MARKER
        | {this.isServerVersionGe80024()}? stableInteger
    ) (COMMA_SYMBOL expr)?
    ;

// The stable_integer nonterminal symbol is not really constant, but constant for the duration of an execution.
stableInteger
    : int64Literal
    | paramOrVar
    ;

paramOrVar
    : PARAM_MARKER
    | identifier
    | AT_SIGN_SYMBOL textOrIdentifier
    ;

nullTreatment
    : (RESPECT_SYMBOL | IGNORE_SYMBOL) NULLS_SYMBOL
    ;

jsonFunction
    : JSON_ARRAYAGG_SYMBOL OPEN_PAR_SYMBOL inSumExpr CLOSE_PAR_SYMBOL windowingClause?
    | JSON_OBJECTAGG_SYMBOL OPEN_PAR_SYMBOL inSumExpr COMMA_SYMBOL inSumExpr CLOSE_PAR_SYMBOL windowingClause?
    ;

inSumExpr
    : ALL_SYMBOL? expr
    ;

identListArg
    : identList
    | OPEN_PAR_SYMBOL identList CLOSE_PAR_SYMBOL
    ;

identList
    : simpleIdentifier (COMMA_SYMBOL simpleIdentifier)*
    ;

fulltextOptions
    : IN_SYMBOL BOOLEAN_SYMBOL MODE_SYMBOL
    | IN_SYMBOL NATURAL_SYMBOL LANGUAGE_SYMBOL MODE_SYMBOL (
        WITH_SYMBOL QUERY_SYMBOL EXPANSION_SYMBOL
    )?
    | WITH_SYMBOL QUERY_SYMBOL EXPANSION_SYMBOL
    ;

// function_call_keyword and function_call_nonkeyword in sql_yacc.yy.
runtimeFunctionCall
    :
    // Function names that are keywords.
    CHAR_SYMBOL OPEN_PAR_SYMBOL exprList (USING_SYMBOL charsetName)? CLOSE_PAR_SYMBOL
    | CURRENT_USER_SYMBOL parentheses?
    | DATE_SYMBOL exprWithParentheses
    | DAY_SYMBOL exprWithParentheses
    | HOUR_SYMBOL exprWithParentheses
    | INSERT_SYMBOL OPEN_PAR_SYMBOL expr COMMA_SYMBOL expr COMMA_SYMBOL expr COMMA_SYMBOL expr CLOSE_PAR_SYMBOL
    | INTERVAL_SYMBOL OPEN_PAR_SYMBOL expr (COMMA_SYMBOL expr)+ CLOSE_PAR_SYMBOL
    | JSON_VALUE_SYMBOL OPEN_PAR_SYMBOL simpleExpr COMMA_SYMBOL textLiteral returningType? onEmptyOrError CLOSE_PAR_SYMBOL
    | LEFT_SYMBOL OPEN_PAR_SYMBOL expr COMMA_SYMBOL expr CLOSE_PAR_SYMBOL
    | MINUTE_SYMBOL exprWithParentheses
    | MONTH_SYMBOL exprWithParentheses
    | RIGHT_SYMBOL OPEN_PAR_SYMBOL expr COMMA_SYMBOL expr CLOSE_PAR_SYMBOL
    | SECOND_SYMBOL exprWithParentheses
    | TIME_SYMBOL exprWithParentheses
    | TIMESTAMP_SYMBOL OPEN_PAR_SYMBOL expr (COMMA_SYMBOL expr)? CLOSE_PAR_SYMBOL
    | trimFunction
    | userFunction
    | VALUES_SYMBOL exprWithParentheses
    | YEAR_SYMBOL exprWithParentheses

    // Function names that are not keywords.
    | (ADDDATE_SYMBOL | SUBDATE_SYMBOL) OPEN_PAR_SYMBOL expr COMMA_SYMBOL (
        expr
        | INTERVAL_SYMBOL expr interval
    ) CLOSE_PAR_SYMBOL
    | CURDATE_SYMBOL parentheses?
    | CURTIME_SYMBOL timeFunctionParameters?
    | (DATE_ADD_SYMBOL | DATE_SUB_SYMBOL) OPEN_PAR_SYMBOL expr COMMA_SYMBOL INTERVAL_SYMBOL expr interval CLOSE_PAR_SYMBOL
    | EXTRACT_SYMBOL OPEN_PAR_SYMBOL interval FROM_SYMBOL expr CLOSE_PAR_SYMBOL
    | GET_FORMAT_SYMBOL OPEN_PAR_SYMBOL dateTimeTtype COMMA_SYMBOL expr CLOSE_PAR_SYMBOL
    | {this.isServerVersionGe80032()}? LOG_SYMBOL OPEN_PAR_SYMBOL expr (
        COMMA_SYMBOL expr
    )? CLOSE_PAR_SYMBOL
    | NOW_SYMBOL timeFunctionParameters?
    | POSITION_SYMBOL OPEN_PAR_SYMBOL bitExpr IN_SYMBOL expr CLOSE_PAR_SYMBOL
    | substringFunction
    | SYSDATE_SYMBOL timeFunctionParameters?
    | (TIMESTAMPADD_SYMBOL | TIMESTAMPDIFF_SYMBOL) OPEN_PAR_SYMBOL intervalTimeStamp COMMA_SYMBOL expr COMMA_SYMBOL expr
        CLOSE_PAR_SYMBOL
    | UTC_DATE_SYMBOL parentheses?
    | UTC_TIME_SYMBOL timeFunctionParameters?
    | UTC_TIMESTAMP_SYMBOL timeFunctionParameters?

    // Function calls with other conflicts.
    | ASCII_SYMBOL exprWithParentheses
    | CHARSET_SYMBOL exprWithParentheses
    | COALESCE_SYMBOL exprListWithParentheses
    | COLLATION_SYMBOL exprWithParentheses
    | DATABASE_SYMBOL parentheses
    | IF_SYMBOL OPEN_PAR_SYMBOL expr COMMA_SYMBOL expr COMMA_SYMBOL expr CLOSE_PAR_SYMBOL
    | FORMAT_SYMBOL OPEN_PAR_SYMBOL expr COMMA_SYMBOL expr (COMMA_SYMBOL expr)? CLOSE_PAR_SYMBOL
    | MICROSECOND_SYMBOL exprWithParentheses
    | MOD_SYMBOL OPEN_PAR_SYMBOL expr COMMA_SYMBOL expr CLOSE_PAR_SYMBOL
    | {this.isServerVersionLt80011()}? PASSWORD_SYMBOL exprWithParentheses
    | QUARTER_SYMBOL exprWithParentheses
    | REPEAT_SYMBOL OPEN_PAR_SYMBOL expr COMMA_SYMBOL expr CLOSE_PAR_SYMBOL
    | REPLACE_SYMBOL OPEN_PAR_SYMBOL expr COMMA_SYMBOL expr COMMA_SYMBOL expr CLOSE_PAR_SYMBOL
    | REVERSE_SYMBOL exprWithParentheses
    | ROW_COUNT_SYMBOL parentheses
    | TRUNCATE_SYMBOL OPEN_PAR_SYMBOL expr COMMA_SYMBOL expr CLOSE_PAR_SYMBOL
    | WEEK_SYMBOL OPEN_PAR_SYMBOL expr (COMMA_SYMBOL expr)? CLOSE_PAR_SYMBOL
    | WEIGHT_STRING_SYMBOL OPEN_PAR_SYMBOL expr (
        (AS_SYMBOL CHAR_SYMBOL wsNumCodepoints)?
        | AS_SYMBOL BINARY_SYMBOL wsNumCodepoints
        | COMMA_SYMBOL ulong_number COMMA_SYMBOL ulong_number COMMA_SYMBOL ulong_number
    ) CLOSE_PAR_SYMBOL
    | geometryFunction
    ;

// JSON_VALUE's optional JSON returning clause.
returningType
    :
    // The default returning type is CHAR(512). (The max length of 512
    // is chosen so that the returned values are not handled as BLOBs
    // internally. See CONVERT_IF_BIGGER_TO_BLOB.)
    RETURNING_SYMBOL castType
    ;

geometryFunction
    : GEOMETRYCOLLECTION_SYMBOL OPEN_PAR_SYMBOL exprList? CLOSE_PAR_SYMBOL
    | LINESTRING_SYMBOL exprListWithParentheses
    | MULTILINESTRING_SYMBOL exprListWithParentheses
    | MULTIPOINT_SYMBOL exprListWithParentheses
    | MULTIPOLYGON_SYMBOL exprListWithParentheses
    | POINT_SYMBOL OPEN_PAR_SYMBOL expr COMMA_SYMBOL expr CLOSE_PAR_SYMBOL
    | POLYGON_SYMBOL exprListWithParentheses
    ;

timeFunctionParameters
    : OPEN_PAR_SYMBOL fractionalPrecision? CLOSE_PAR_SYMBOL
    ;

fractionalPrecision
    : INT_NUMBER
    ;

weightStringLevels
    : LEVEL_SYMBOL (
        real_ulong_number MINUS_OPERATOR real_ulong_number
        | weightStringLevelListItem (COMMA_SYMBOL weightStringLevelListItem)*
    )
    ;

weightStringLevelListItem
    : real_ulong_number ((ASC_SYMBOL | DESC_SYMBOL) REVERSE_SYMBOL? | REVERSE_SYMBOL)?
    ;

dateTimeTtype
    : DATE_SYMBOL
    | TIME_SYMBOL
    | DATETIME_SYMBOL
    | TIMESTAMP_SYMBOL
    ;

trimFunction
    : TRIM_SYMBOL OPEN_PAR_SYMBOL (
        expr (FROM_SYMBOL expr)?
        | LEADING_SYMBOL expr? FROM_SYMBOL expr
        | TRAILING_SYMBOL expr? FROM_SYMBOL expr
        | BOTH_SYMBOL expr? FROM_SYMBOL expr
    ) CLOSE_PAR_SYMBOL
    ;

substringFunction
    : SUBSTRING_SYMBOL OPEN_PAR_SYMBOL expr (
        COMMA_SYMBOL expr (COMMA_SYMBOL expr)?
        | FROM_SYMBOL expr (FOR_SYMBOL expr)?
    ) CLOSE_PAR_SYMBOL
    ;

functionCall
    : pureIdentifier OPEN_PAR_SYMBOL udfExprList? CLOSE_PAR_SYMBOL   // For both UDF + other functions.
    | qualifiedIdentifier OPEN_PAR_SYMBOL exprList? CLOSE_PAR_SYMBOL // Other functions only.
    ;

udfExprList
    : udfExpr (COMMA_SYMBOL udfExpr)*
    ;

udfExpr
    : expr selectAlias?
    ;

userVariable
    : AT_SIGN_SYMBOL textOrIdentifier
    | AT_TEXT_SUFFIX
    ;

inExpressionUserVariableAssignment
    : userVariable ASSIGN_OPERATOR expr
    ;

rvalueSystemOrUserVariable
    : userVariable
    | AT_AT_SIGN_SYMBOL rvalueSystemVariableType? rvalueSystemVariable
    ;

lvalueVariable
    : (
        // Check in semantic phase that the first id is not global/local/session/default.
        identifier dotIdentifier?
        | {this.isServerVersionGe80017()}? lValueIdentifier dotIdentifier?
    )
    | DEFAULT_SYMBOL dotIdentifier
    ;

rvalueSystemVariable
    : textOrIdentifier dotIdentifier?
    ;

whenExpression
    : WHEN_SYMBOL expr
    ;

thenExpression
    : THEN_SYMBOL expr
    ;

elseExpression
    : ELSE_SYMBOL expr
    ;

castType
    : BINARY_SYMBOL fieldLength?
    | CHAR_SYMBOL fieldLength? charsetWithOptBinary?
    | nchar fieldLength?
    | SIGNED_SYMBOL INT_SYMBOL?
    | UNSIGNED_SYMBOL INT_SYMBOL?
    | DATE_SYMBOL
    | {this.isServerVersionGe80024()}? YEAR_SYMBOL
    | TIME_SYMBOL typeDatetimePrecision?
    | DATETIME_SYMBOL typeDatetimePrecision?
    | DECIMAL_SYMBOL floatOptions?
    | JSON_SYMBOL
    | {this.isServerVersionGe80017()}? realType
    | {this.isServerVersionGe80017()}? FLOAT_SYMBOL standardFloatOptions?
    | {this.isServerVersionGe80027()}? (
        POINT_SYMBOL
        | LINESTRING_SYMBOL
        | POLYGON_SYMBOL
        | MULTIPOINT_SYMBOL
        | MULTILINESTRING_SYMBOL
        | MULTIPOLYGON_SYMBOL
        | GEOMETRYCOLLECTION_SYMBOL
    )
    ;

exprList
    : expr (COMMA_SYMBOL expr)*
    ;

charset
    : CHAR_SYMBOL SET_SYMBOL
    | CHARSET_SYMBOL
    ;

notRule
    : NOT_SYMBOL
    | NOT2_SYMBOL // A NOT with a different (higher) operator precedence.
    ;

not2Rule
    : LOGICAL_NOT_OPERATOR
    | NOT2_SYMBOL
    ;

// None of the microsecond variants can be used in schedules (e.g. events).
interval
    : intervalTimeStamp
    | (
        SECOND_MICROSECOND_SYMBOL
        | MINUTE_MICROSECOND_SYMBOL
        | MINUTE_SECOND_SYMBOL
        | HOUR_MICROSECOND_SYMBOL
        | HOUR_SECOND_SYMBOL
        | HOUR_MINUTE_SYMBOL
        | DAY_MICROSECOND_SYMBOL
        | DAY_SECOND_SYMBOL
        | DAY_MINUTE_SYMBOL
        | DAY_HOUR_SYMBOL
        | YEAR_MONTH_SYMBOL
    )
    ;

// Support for SQL_TSI_* units is added by mapping those to tokens without SQL_TSI_ prefix.
intervalTimeStamp
    : MICROSECOND_SYMBOL
    | SECOND_SYMBOL
    | MINUTE_SYMBOL
    | HOUR_SYMBOL
    | DAY_SYMBOL
    | WEEK_SYMBOL
    | MONTH_SYMBOL
    | QUARTER_SYMBOL
    | YEAR_SYMBOL
    ;

exprListWithParentheses
    : OPEN_PAR_SYMBOL exprList CLOSE_PAR_SYMBOL
    ;

exprWithParentheses
    : OPEN_PAR_SYMBOL expr CLOSE_PAR_SYMBOL
    ;

simpleExprWithParentheses
    : OPEN_PAR_SYMBOL simpleExpr CLOSE_PAR_SYMBOL
    ;

orderList
    : orderExpression (COMMA_SYMBOL orderExpression)*
    ;

orderExpression
    : expr direction?
    ;

groupList
    : groupingExpression (COMMA_SYMBOL groupingExpression)*
    ;

groupingExpression
    : expr
    ;

channel
    : FOR_SYMBOL CHANNEL_SYMBOL textStringNoLinebreak
    ;

//----------------- Stored routines rules ------------------------------------------------------------------------------

// Compound syntax for stored procedures, stored functions, triggers and events.
// Implements both, sp_proc_stmt and ev_sql_stmt_inner from the server grammar.
compoundStatement
    : simpleStatement
    | returnStatement
    | ifStatement
    | caseStatement
    | labeledBlock
    | unlabeledBlock
    | labeledControl
    | unlabeledControl
    | leaveStatement
    | iterateStatement
    | cursorOpen
    | cursorFetch
    | cursorClose
    ;

returnStatement
    : RETURN_SYMBOL expr
    ;

ifStatement
    : IF_SYMBOL ifBody END_SYMBOL IF_SYMBOL
    ;

ifBody
    : expr thenStatement (ELSEIF_SYMBOL ifBody | ELSE_SYMBOL compoundStatementList)?
    ;

thenStatement
    : THEN_SYMBOL compoundStatementList
    ;

compoundStatementList
    : (compoundStatement SEMICOLON_SYMBOL)+
    ;

caseStatement
    : CASE_SYMBOL expr? (whenExpression thenStatement)+ elseStatement? END_SYMBOL CASE_SYMBOL
    ;

elseStatement
    : ELSE_SYMBOL compoundStatementList
    ;

labeledBlock
    : label beginEndBlock labelRef?
    ;

unlabeledBlock
    : beginEndBlock
    ;

label
    :
    // Block labels can only be up to 16 characters long.
    labelIdentifier COLON_SYMBOL
    ;

beginEndBlock
    : BEGIN_SYMBOL spDeclarations? compoundStatementList? END_SYMBOL
    ;

labeledControl
    : label unlabeledControl labelRef?
    ;

unlabeledControl
    : loopBlock
    | whileDoBlock
    | repeatUntilBlock
    ;

loopBlock
    : LOOP_SYMBOL compoundStatementList END_SYMBOL LOOP_SYMBOL
    ;

whileDoBlock
    : WHILE_SYMBOL expr DO_SYMBOL compoundStatementList END_SYMBOL WHILE_SYMBOL
    ;

repeatUntilBlock
    : REPEAT_SYMBOL compoundStatementList UNTIL_SYMBOL expr END_SYMBOL REPEAT_SYMBOL
    ;

spDeclarations
    : (spDeclaration SEMICOLON_SYMBOL)+
    ;

spDeclaration
    : variableDeclaration
    | conditionDeclaration
    | handlerDeclaration
    | cursorDeclaration
    ;

variableDeclaration
    : DECLARE_SYMBOL identifierList dataType collate? (DEFAULT_SYMBOL expr)?
    ;

conditionDeclaration
    : DECLARE_SYMBOL identifier CONDITION_SYMBOL FOR_SYMBOL spCondition
    ;

spCondition
    : ulong_number
    | sqlstate
    ;

sqlstate
    : SQLSTATE_SYMBOL VALUE_SYMBOL? textLiteral
    ;

handlerDeclaration
    : DECLARE_SYMBOL (CONTINUE_SYMBOL | EXIT_SYMBOL | UNDO_SYMBOL) HANDLER_SYMBOL FOR_SYMBOL handlerCondition (
        COMMA_SYMBOL handlerCondition
    )* compoundStatement
    ;

handlerCondition
    : spCondition
    | identifier
    | SQLWARNING_SYMBOL
    | notRule FOUND_SYMBOL
    | SQLEXCEPTION_SYMBOL
    ;

cursorDeclaration
    : DECLARE_SYMBOL identifier CURSOR_SYMBOL FOR_SYMBOL selectStatement
    ;

iterateStatement
    : ITERATE_SYMBOL labelRef
    ;

leaveStatement
    : LEAVE_SYMBOL labelRef
    ;

getDiagnosticsStatement
    : GET_SYMBOL (CURRENT_SYMBOL | STACKED_SYMBOL)? DIAGNOSTICS_SYMBOL (
        statementInformationItem (COMMA_SYMBOL statementInformationItem)*
        | CONDITION_SYMBOL signalAllowedExpr conditionInformationItem (
            COMMA_SYMBOL conditionInformationItem
        )*
    )
    ;

// Only a limited subset of expr is allowed in SIGNAL/RESIGNAL/CONDITIONS.
signalAllowedExpr
    : literal
    | rvalueSystemOrUserVariable
    | qualifiedIdentifier
    ;

statementInformationItem
    : (userVariable | identifier) EQUAL_OPERATOR (NUMBER_SYMBOL | ROW_COUNT_SYMBOL)
    ;

conditionInformationItem
    : (userVariable | identifier) EQUAL_OPERATOR (
        signalInformationItemName
        | RETURNED_SQLSTATE_SYMBOL
    )
    ;

signalInformationItemName
    : CLASS_ORIGIN_SYMBOL
    | SUBCLASS_ORIGIN_SYMBOL
    | CONSTRAINT_CATALOG_SYMBOL
    | CONSTRAINT_SCHEMA_SYMBOL
    | CONSTRAINT_NAME_SYMBOL
    | CATALOG_NAME_SYMBOL
    | SCHEMA_NAME_SYMBOL
    | TABLE_NAME_SYMBOL
    | COLUMN_NAME_SYMBOL
    | CURSOR_NAME_SYMBOL
    | MESSAGE_TEXT_SYMBOL
    | MYSQL_ERRNO_SYMBOL
    ;

signalStatement
    : SIGNAL_SYMBOL (identifier | sqlstate) (
        SET_SYMBOL signalInformationItem (COMMA_SYMBOL signalInformationItem)*
    )?
    ;

resignalStatement
    : RESIGNAL_SYMBOL (identifier | sqlstate)? (
        SET_SYMBOL signalInformationItem (COMMA_SYMBOL signalInformationItem)*
    )?
    ;

signalInformationItem
    : signalInformationItemName EQUAL_OPERATOR signalAllowedExpr
    ;

cursorOpen
    : OPEN_SYMBOL identifier
    ;

cursorClose
    : CLOSE_SYMBOL identifier
    ;

cursorFetch
    : FETCH_SYMBOL (NEXT_SYMBOL? FROM_SYMBOL)? identifier INTO_SYMBOL identifierList
    ;

//----------------- Supplemental rules ---------------------------------------------------------------------------------

// Schedules in CREATE/ALTER EVENT.
schedule
    : AT_SYMBOL expr
    | EVERY_SYMBOL expr interval (STARTS_SYMBOL expr)? (ENDS_SYMBOL expr)?
    ;

columnDefinition
    : columnName fieldDefinition checkOrReferences?
    ;

checkOrReferences
    : {this.isServerVersionLt80016()}? checkConstraint
    | references
    ;

checkConstraint
    : CHECK_SYMBOL exprWithParentheses
    ;

constraintEnforcement
    : {this.isServerVersionGe80017()}? NOT_SYMBOL? ENFORCED_SYMBOL
    ;

tableConstraintDef
    : type = (KEY_SYMBOL | INDEX_SYMBOL) indexNameAndType? keyListWithExpression indexOption*
    | type = FULLTEXT_SYMBOL keyOrIndex? indexName? keyListWithExpression fulltextIndexOption*
    | type = SPATIAL_SYMBOL keyOrIndex? indexName? keyListWithExpression spatialIndexOption*
    | constraintName? (
        (type = PRIMARY_SYMBOL KEY_SYMBOL | type = UNIQUE_SYMBOL keyOrIndex?) indexNameAndType? keyListWithExpression indexOption*
        | type = FOREIGN_SYMBOL KEY_SYMBOL indexName? keyList references
        | checkConstraint constraintEnforcement?
    )
    ;

constraintName
    : CONSTRAINT_SYMBOL identifier?
    ;

fieldDefinition
    : dataType (
        columnAttribute*
        | collate? (GENERATED_SYMBOL ALWAYS_SYMBOL)? AS_SYMBOL exprWithParentheses (
            VIRTUAL_SYMBOL
            | STORED_SYMBOL
        )? columnAttribute*
    )
    ;

columnAttribute
    : NOT_SYMBOL? nullLiteral
    | {this.isServerVersionGe80014()}? NOT_SYMBOL SECONDARY_SYMBOL
    | value = DEFAULT_SYMBOL (
        nowOrSignedLiteral
        | {this.isServerVersionGe80013()}? exprWithParentheses
    )
    | value = ON_SYMBOL UPDATE_SYMBOL NOW_SYMBOL timeFunctionParameters?
    | value = AUTO_INCREMENT_SYMBOL
    | value = SERIAL_SYMBOL DEFAULT_SYMBOL VALUE_SYMBOL
    | PRIMARY_SYMBOL? value = KEY_SYMBOL
    | value = UNIQUE_SYMBOL KEY_SYMBOL?
    | value = COMMENT_SYMBOL textLiteral
    | collate
    | value = COLUMN_FORMAT_SYMBOL columnFormat
    | value = STORAGE_SYMBOL storageMedia
    | value = SRID_SYMBOL real_ulonglong_number
    | {this.isServerVersionGe80017()}? constraintName? checkConstraint
    | {this.isServerVersionGe80017()}? constraintEnforcement
    | {this.isServerVersionGe80024()}? value = ENGINE_ATTRIBUTE_SYMBOL EQUAL_OPERATOR? jsonAttribute
    | {this.isServerVersionGe80024()}? value = SECONDARY_ENGINE_ATTRIBUTE_SYMBOL EQUAL_OPERATOR? jsonAttribute
    | {this.isServerVersionGe80024()}? visibility
    ;

columnFormat
    : FIXED_SYMBOL
    | DYNAMIC_SYMBOL
    | DEFAULT_SYMBOL
    ;

storageMedia
    : DISK_SYMBOL
    | MEMORY_SYMBOL
    | DEFAULT_SYMBOL
    ;

now
    : NOW_SYMBOL functionDatetimePrecision
    ;

nowOrSignedLiteral
    : now
    | signedLiteralOrNull
    ;

gcolAttribute
    : UNIQUE_SYMBOL KEY_SYMBOL?
    | COMMENT_SYMBOL textString
    | notRule? NULL_SYMBOL
    | PRIMARY_SYMBOL? KEY_SYMBOL
    ;

references
    : REFERENCES_SYMBOL tableRef identifierListWithParentheses? (
        MATCH_SYMBOL match = (FULL_SYMBOL | PARTIAL_SYMBOL | SIMPLE_SYMBOL)
    )? (
        ON_SYMBOL option = UPDATE_SYMBOL deleteOption (
            ON_SYMBOL DELETE_SYMBOL deleteOption
        )?
        | ON_SYMBOL option = DELETE_SYMBOL deleteOption (
            ON_SYMBOL UPDATE_SYMBOL deleteOption
        )?
    )?
    ;

deleteOption
    : (RESTRICT_SYMBOL | CASCADE_SYMBOL)
    | SET_SYMBOL nullLiteral
    | SET_SYMBOL DEFAULT_SYMBOL
    | NO_SYMBOL ACTION_SYMBOL
    ;

keyList
    : OPEN_PAR_SYMBOL keyPart (COMMA_SYMBOL keyPart)* CLOSE_PAR_SYMBOL
    ;

keyPart
    : identifier fieldLength? direction?
    ;

keyListWithExpression
    : OPEN_PAR_SYMBOL keyPartOrExpression (COMMA_SYMBOL keyPartOrExpression)* CLOSE_PAR_SYMBOL
    ;

keyPartOrExpression
    : // key_part_with_expression in sql_yacc.yy.
    keyPart
    | {this.isServerVersionGe80013()}? exprWithParentheses direction?
    ;

indexType
    : algorithm = (BTREE_SYMBOL | RTREE_SYMBOL | HASH_SYMBOL)
    ;

indexOption
    : commonIndexOption
    | indexTypeClause
    ;

// These options are common for all index types.
commonIndexOption
    : KEY_BLOCK_SIZE_SYMBOL EQUAL_OPERATOR? ulong_number
    | COMMENT_SYMBOL textLiteral
    | visibility
    | {this.isServerVersionGe80024()}? ENGINE_ATTRIBUTE_SYMBOL EQUAL_OPERATOR? jsonAttribute
    | {this.isServerVersionGe80024()}? SECONDARY_ENGINE_ATTRIBUTE_SYMBOL EQUAL_OPERATOR? jsonAttribute
    ;

visibility
    : VISIBLE_SYMBOL
    | INVISIBLE_SYMBOL
    ;

indexTypeClause
    : (USING_SYMBOL | TYPE_SYMBOL) indexType
    ;

fulltextIndexOption
    : commonIndexOption
    | WITH_SYMBOL PARSER_SYMBOL identifier
    ;

spatialIndexOption
    : commonIndexOption
    ;

dataTypeDefinition
    : // For external use only. Don't reference this in the normal grammar.
    dataType EOF
    ;

dataType
    : // type in sql_yacc.yy
    type = (
        INT_SYMBOL
        | TINYINT_SYMBOL
        | SMALLINT_SYMBOL
        | MEDIUMINT_SYMBOL
        | BIGINT_SYMBOL
    ) fieldLength? fieldOptions?
    | (type = REAL_SYMBOL | type = DOUBLE_SYMBOL PRECISION_SYMBOL?) precision? fieldOptions?
    | type = (FLOAT_SYMBOL | DECIMAL_SYMBOL | NUMERIC_SYMBOL | FIXED_SYMBOL) floatOptions? fieldOptions?
    | type = BIT_SYMBOL fieldLength?
    | type = (BOOL_SYMBOL | BOOLEAN_SYMBOL)
    | type = CHAR_SYMBOL fieldLength? charsetWithOptBinary?
    | nchar fieldLength? BINARY_SYMBOL?
    | type = BINARY_SYMBOL fieldLength?
    | (type = CHAR_SYMBOL VARYING_SYMBOL | type = VARCHAR_SYMBOL) fieldLength charsetWithOptBinary?
    | (
        type = NATIONAL_SYMBOL VARCHAR_SYMBOL
        | type = NVARCHAR_SYMBOL
        | type = NCHAR_SYMBOL VARCHAR_SYMBOL
        | type = NATIONAL_SYMBOL CHAR_SYMBOL VARYING_SYMBOL
        | type = NCHAR_SYMBOL VARYING_SYMBOL
    ) fieldLength BINARY_SYMBOL?
    | type = VARBINARY_SYMBOL fieldLength
    | type = YEAR_SYMBOL fieldLength? fieldOptions?
    | type = DATE_SYMBOL
    | type = TIME_SYMBOL typeDatetimePrecision?
    | type = TIMESTAMP_SYMBOL typeDatetimePrecision?
    | type = DATETIME_SYMBOL typeDatetimePrecision?
    | type = TINYBLOB_SYMBOL
    | type = BLOB_SYMBOL fieldLength?
    | type = (MEDIUMBLOB_SYMBOL | LONGBLOB_SYMBOL)
    | type = LONG_SYMBOL VARBINARY_SYMBOL
    | type = LONG_SYMBOL (CHAR_SYMBOL VARYING_SYMBOL | VARCHAR_SYMBOL)? charsetWithOptBinary?
    | type = TINYTEXT_SYMBOL charsetWithOptBinary?
    | type = TEXT_SYMBOL fieldLength? charsetWithOptBinary?
    | type = MEDIUMTEXT_SYMBOL charsetWithOptBinary?
    | type = LONGTEXT_SYMBOL charsetWithOptBinary?
    | type = ENUM_SYMBOL stringList charsetWithOptBinary?
    | type = SET_SYMBOL stringList charsetWithOptBinary?
    | type = SERIAL_SYMBOL
    | type = JSON_SYMBOL
    | type = VECTOR_SYMBOL fieldLength?
    | type = (
        GEOMETRY_SYMBOL
        | GEOMETRYCOLLECTION_SYMBOL
        | POINT_SYMBOL
        | MULTIPOINT_SYMBOL
        | LINESTRING_SYMBOL
        | MULTILINESTRING_SYMBOL
        | POLYGON_SYMBOL
        | MULTIPOLYGON_SYMBOL
    )
    ;

nchar
    : type = NCHAR_SYMBOL
    | type = NATIONAL_SYMBOL CHAR_SYMBOL
    ;

realType
    : type = REAL_SYMBOL
    | type = DOUBLE_SYMBOL PRECISION_SYMBOL?
    ;

fieldLength
    : OPEN_PAR_SYMBOL (real_ulonglong_number | DECIMAL_NUMBER) CLOSE_PAR_SYMBOL
    ;

fieldOptions
    : (SIGNED_SYMBOL | UNSIGNED_SYMBOL | ZEROFILL_SYMBOL)+
    ;

charsetWithOptBinary
    : ascii
    | unicode
    | BYTE_SYMBOL
    | charset charsetName BINARY_SYMBOL?
    | BINARY_SYMBOL (charset charsetName)?
    ;

ascii
    : ASCII_SYMBOL BINARY_SYMBOL?
    | BINARY_SYMBOL ASCII_SYMBOL
    ;

unicode
    : UNICODE_SYMBOL BINARY_SYMBOL?
    | BINARY_SYMBOL UNICODE_SYMBOL
    ;

wsNumCodepoints
    : OPEN_PAR_SYMBOL real_ulong_number CLOSE_PAR_SYMBOL
    ;

typeDatetimePrecision
    : OPEN_PAR_SYMBOL INT_NUMBER CLOSE_PAR_SYMBOL
    ;

functionDatetimePrecision
    :
    | parentheses
    | OPEN_PAR_SYMBOL INT_NUMBER CLOSE_PAR_SYMBOL
    ;

charsetName
    : textOrIdentifier
    | BINARY_SYMBOL
    ;

collationName
    : textOrIdentifier
    | {this.isServerVersionGe80018()}? BINARY_SYMBOL
    ;

createTableOptions
    : createTableOption (COMMA_SYMBOL? createTableOption)*
    ;

createTableOptionsEtc
    : createTableOptions createPartitioningEtc?
    | createPartitioningEtc
    ;

createPartitioningEtc
    : partitionClause duplicateAsQe?
    | duplicateAsQe
    ;

createTableOptionsSpaceSeparated
    : createTableOption+
    ;

createTableOption
    : // In the order as they appear in the server grammar.
    option = ENGINE_SYMBOL EQUAL_OPERATOR? engineRef
    | {this.isServerVersionGe80014()}? option = SECONDARY_ENGINE_SYMBOL equal? (
        NULL_SYMBOL
        | textOrIdentifier
    )
    | option = MAX_ROWS_SYMBOL EQUAL_OPERATOR? ulonglongNumber
    | option = MIN_ROWS_SYMBOL EQUAL_OPERATOR? ulonglongNumber
    | option = AVG_ROW_LENGTH_SYMBOL EQUAL_OPERATOR? ulonglongNumber
    | option = PASSWORD_SYMBOL EQUAL_OPERATOR? textStringLiteral
    | option = COMMENT_SYMBOL EQUAL_OPERATOR? textStringLiteral
    | option = COMPRESSION_SYMBOL EQUAL_OPERATOR? textString
    | option = ENCRYPTION_SYMBOL EQUAL_OPERATOR? textString
    | option = AUTO_INCREMENT_SYMBOL EQUAL_OPERATOR? ulonglongNumber
    | option = PACK_KEYS_SYMBOL EQUAL_OPERATOR? ternaryOption
    | option = (
        STATS_AUTO_RECALC_SYMBOL
        | STATS_PERSISTENT_SYMBOL
        | STATS_SAMPLE_PAGES_SYMBOL
    ) EQUAL_OPERATOR? ternaryOption
    | option = (CHECKSUM_SYMBOL | TABLE_CHECKSUM_SYMBOL) EQUAL_OPERATOR? ulong_number
    | option = DELAY_KEY_WRITE_SYMBOL EQUAL_OPERATOR? ulong_number
    | option = ROW_FORMAT_SYMBOL EQUAL_OPERATOR? format = (
        DEFAULT_SYMBOL
        | DYNAMIC_SYMBOL
        | FIXED_SYMBOL
        | COMPRESSED_SYMBOL
        | REDUNDANT_SYMBOL
        | COMPACT_SYMBOL
    )
    | option = UNION_SYMBOL EQUAL_OPERATOR? OPEN_PAR_SYMBOL tableRefList CLOSE_PAR_SYMBOL
    | defaultCharset
    | defaultCollation
    | option = INSERT_METHOD_SYMBOL EQUAL_OPERATOR? method = (
        NO_SYMBOL
        | FIRST_SYMBOL
        | LAST_SYMBOL
    )
    | option = DATA_SYMBOL DIRECTORY_SYMBOL EQUAL_OPERATOR? textString
    | option = INDEX_SYMBOL DIRECTORY_SYMBOL EQUAL_OPERATOR? textString
    | option = TABLESPACE_SYMBOL EQUAL_OPERATOR? identifier
    | option = STORAGE_SYMBOL (DISK_SYMBOL | MEMORY_SYMBOL)
    | option = CONNECTION_SYMBOL EQUAL_OPERATOR? textString
    | option = KEY_BLOCK_SIZE_SYMBOL EQUAL_OPERATOR? ulonglongNumber
    | {this.isServerVersionGe80024()}? option = START_SYMBOL TRANSACTION_SYMBOL
    | {this.isServerVersionGe80024()}? option = ENGINE_ATTRIBUTE_SYMBOL EQUAL_OPERATOR? jsonAttribute
    | {this.isServerVersionGe80024()}? option = SECONDARY_ENGINE_ATTRIBUTE_SYMBOL EQUAL_OPERATOR? jsonAttribute
    | {this.isServerVersionGe80024()}? tsOptionAutoextendSize
    | identifier EQUAL_OPERATOR? textOrIdentifier  // Engine-specific options
    ;

ternaryOption
    : ulong_number
    | DEFAULT_SYMBOL
    ;

defaultCollation
    : DEFAULT_SYMBOL? COLLATE_SYMBOL EQUAL_OPERATOR? collationName
    ;

defaultEncryption
    : DEFAULT_SYMBOL? ENCRYPTION_SYMBOL EQUAL_OPERATOR? textStringLiteral
    ;

defaultCharset
    : DEFAULT_SYMBOL? charset EQUAL_OPERATOR? charsetName
    ;

// Partition rules for CREATE/ALTER TABLE.
partitionClause
    : PARTITION_SYMBOL BY_SYMBOL partitionTypeDef (
        PARTITIONS_SYMBOL real_ulong_number
    )? subPartitions? partitionDefinitions?
    ;

partitionTypeDef
    : LINEAR_SYMBOL? KEY_SYMBOL partitionKeyAlgorithm? OPEN_PAR_SYMBOL identifierList? CLOSE_PAR_SYMBOL # partitionDefKey
    | LINEAR_SYMBOL? HASH_SYMBOL OPEN_PAR_SYMBOL bitExpr CLOSE_PAR_SYMBOL                               # partitionDefHash
    | (RANGE_SYMBOL | LIST_SYMBOL) (
        OPEN_PAR_SYMBOL bitExpr CLOSE_PAR_SYMBOL
        | COLUMNS_SYMBOL OPEN_PAR_SYMBOL identifierList? CLOSE_PAR_SYMBOL
    ) # partitionDefRangeList
    ;

subPartitions
    : SUBPARTITION_SYMBOL BY_SYMBOL LINEAR_SYMBOL? (
        HASH_SYMBOL OPEN_PAR_SYMBOL bitExpr CLOSE_PAR_SYMBOL
        | KEY_SYMBOL partitionKeyAlgorithm? identifierListWithParentheses
    ) (SUBPARTITIONS_SYMBOL real_ulong_number)?
    ;

partitionKeyAlgorithm
    : // Actually only 1 and 2 are allowed. Needs a semantic check.
    ALGORITHM_SYMBOL EQUAL_OPERATOR real_ulong_number
    ;

partitionDefinitions
    : OPEN_PAR_SYMBOL partitionDefinition (COMMA_SYMBOL partitionDefinition)* CLOSE_PAR_SYMBOL
    ;

partitionDefinition
    : PARTITION_SYMBOL identifier (
        VALUES_SYMBOL LESS_SYMBOL THAN_SYMBOL (
            partitionValueItemListParen
            | MAXVALUE_SYMBOL
        )
        | VALUES_SYMBOL IN_SYMBOL partitionValuesIn
    )? partitionOption* (
        OPEN_PAR_SYMBOL subpartitionDefinition (COMMA_SYMBOL subpartitionDefinition)* CLOSE_PAR_SYMBOL
    )?
    ;

partitionValuesIn
    : partitionValueItemListParen
    | OPEN_PAR_SYMBOL partitionValueItemListParen (
        COMMA_SYMBOL partitionValueItemListParen
    )* CLOSE_PAR_SYMBOL
    ;

partitionOption
    : option = TABLESPACE_SYMBOL EQUAL_OPERATOR? identifier
    | STORAGE_SYMBOL? option = ENGINE_SYMBOL EQUAL_OPERATOR? engineRef
    | option = NODEGROUP_SYMBOL EQUAL_OPERATOR? real_ulong_number
    | option = (MAX_ROWS_SYMBOL | MIN_ROWS_SYMBOL) EQUAL_OPERATOR? real_ulong_number
    | option = (DATA_SYMBOL | INDEX_SYMBOL) DIRECTORY_SYMBOL EQUAL_OPERATOR? textLiteral
    | option = COMMENT_SYMBOL EQUAL_OPERATOR? textLiteral
    ;

subpartitionDefinition
    : SUBPARTITION_SYMBOL textOrIdentifier partitionOption*
    ;

partitionValueItemListParen
    : OPEN_PAR_SYMBOL partitionValueItem (COMMA_SYMBOL partitionValueItem)* CLOSE_PAR_SYMBOL
    ;

partitionValueItem
    : bitExpr
    | MAXVALUE_SYMBOL
    ;

definerClause
    : DEFINER_SYMBOL EQUAL_OPERATOR user
    ;

ifExists
    : IF_SYMBOL EXISTS_SYMBOL
    ;

ifExistsIdentifier
    : ifExists persistedVariableIdentifier
    ;

persistedVariableIdentifier
    : identifier
    | {this.isServerVersionGe80032()}? (
        qualifiedIdentifier
        | DEFAULT_SYMBOL dotIdentifier
    )
    ;

ifNotExists
    : IF_SYMBOL notRule EXISTS_SYMBOL
    ;

ignoreUnknownUser
    : IGNORE_SYMBOL UNKNOWN_SYMBOL USER_SYMBOL
    ;

procedureParameter
    : type = (IN_SYMBOL | OUT_SYMBOL | INOUT_SYMBOL)? functionParameter
    ;

functionParameter
    : parameterName typeWithOptCollate
    ;

collate
    : COLLATE_SYMBOL collationName
    ;

typeWithOptCollate
    : dataType collate?
    ;

schemaIdentifierPair
    : OPEN_PAR_SYMBOL schemaRef COMMA_SYMBOL schemaRef CLOSE_PAR_SYMBOL
    ;

viewRefList
    : viewRef (COMMA_SYMBOL viewRef)*
    ;

updateList
    : updateElement (COMMA_SYMBOL updateElement)*
    ;

updateElement
    : columnRef EQUAL_OPERATOR (expr | DEFAULT_SYMBOL)
    ;

charsetClause
    : charset charsetName
    ;

fieldsClause
    : COLUMNS_SYMBOL fieldTerm+
    ;

fieldTerm
    : TERMINATED_SYMBOL BY_SYMBOL textString
    | OPTIONALLY_SYMBOL? ENCLOSED_SYMBOL BY_SYMBOL textString
    | ESCAPED_SYMBOL BY_SYMBOL textString
    ;

linesClause
    : LINES_SYMBOL lineTerm+
    ;

lineTerm
    : (TERMINATED_SYMBOL | STARTING_SYMBOL) BY_SYMBOL textString
    ;

userList
    : user (COMMA_SYMBOL user)*
    ;

createUserList
    : createUser (COMMA_SYMBOL createUser)*
    ;

createUser
    : user (
        identification createUserWithMfa?
        | identifiedWithPlugin initialAuth?
        | createUserWithMfa
    )?
    ;

createUserWithMfa
    : AND_SYMBOL identification (AND_SYMBOL identification)?
    ;

identification
    : identifiedByPassword
    | identifiedByRandomPassword
    | identifiedWithPlugin
    | identifiedWithPluginAsAuth
    | identifiedWithPluginByPassword
    | identifiedWithPluginByRandomPassword
    ;

identifiedByPassword
    : IDENTIFIED_SYMBOL BY_SYMBOL textStringLiteral
    ;

identifiedByRandomPassword
    : IDENTIFIED_SYMBOL BY_SYMBOL RANDOM_SYMBOL PASSWORD_SYMBOL
    ;

identifiedWithPlugin
    : IDENTIFIED_SYMBOL WITH_SYMBOL textOrIdentifier
    ;

identifiedWithPluginAsAuth
    : IDENTIFIED_SYMBOL WITH_SYMBOL textOrIdentifier AS_SYMBOL textStringHash
    ;

identifiedWithPluginByPassword
    : IDENTIFIED_SYMBOL WITH_SYMBOL textOrIdentifier BY_SYMBOL textStringLiteral
    ;

identifiedWithPluginByRandomPassword
    : IDENTIFIED_SYMBOL WITH_SYMBOL textOrIdentifier BY_SYMBOL RANDOM_SYMBOL PASSWORD_SYMBOL
    ;

initialAuth
    : INITIAL_SYMBOL AUTHENTICATION_SYMBOL (
        identifiedByRandomPassword
        | identifiedWithPluginAsAuth
        | identifiedByPassword
    )
    ;

retainCurrentPassword
    : RETAIN_SYMBOL CURRENT_SYMBOL PASSWORD_SYMBOL
    ;

discardOldPassword
    : DISCARD_SYMBOL OLD_SYMBOL PASSWORD_SYMBOL
    ;

userRegistration
    : factor INITIATE_SYMBOL REGISTRATION_SYMBOL
    | factor UNREGISTER_SYMBOL
    | factor FINISH_SYMBOL REGISTRATION_SYMBOL SET_SYMBOL CHALLENGE_RESPONSE_SYMBOL AS_SYMBOL textStringHash
    ;

factor
    : numLiteral FACTOR_SYMBOL
    ;

replacePassword
    : REPLACE_SYMBOL textString
    ;

userIdentifierOrText
    : textOrIdentifier userVariable?
    ;

user
    : userIdentifierOrText
    | CURRENT_USER_SYMBOL parentheses?
    ;

likeClause
    : LIKE_SYMBOL textStringLiteral
    ;

likeOrWhere
    : // opt_wild_or_where in sql_yacc.yy
    likeClause
    | whereClause
    ;

onlineOption
    : ONLINE_SYMBOL
    | OFFLINE_SYMBOL
    ;

noWriteToBinLog
    : LOCAL_SYMBOL
    | NO_WRITE_TO_BINLOG_SYMBOL
    ;

usePartition
    : PARTITION_SYMBOL identifierListWithParentheses
    ;

//----------------- Object names and references ------------------------------------------------------------------------

// For each object we have at least 2 rules here:
// 1) The name when creating that object.
// 2) The name when used to reference it from other rules.
//
// Sometimes we need additional reference rules with different form, depending on the place such a reference is used.

// A name for a field (column/index). Can be qualified with the current schema + table (although it's not a reference).
fieldIdentifier
    : dotIdentifier
    | qualifiedIdentifier dotIdentifier?
    ;

columnName
    : identifier
    ;

// A reference to a column of the object we are working on.
columnInternalRef
    : identifier
    ;

columnInternalRefList
    : // column_list (+ parentheses) + opt_derived_column_list in sql_yacc.yy
    OPEN_PAR_SYMBOL columnInternalRef (COMMA_SYMBOL columnInternalRef)* CLOSE_PAR_SYMBOL
    ;

columnRef
    : // A field identifier that can reference any schema/table.
    fieldIdentifier
    ;

insertIdentifier
    : columnRef
    | tableWild
    ;

indexName
    : identifier
    ;

indexRef
    : // Always internal reference. Still all qualification variations are accepted.
    fieldIdentifier
    ;

tableWild
    : identifier DOT_SYMBOL (identifier DOT_SYMBOL)? MULT_OPERATOR
    ;

schemaName
    : identifier
    ;

schemaRef
    : identifier
    ;

procedureName
    : qualifiedIdentifier
    ;

procedureRef
    : qualifiedIdentifier
    ;

functionName
    : qualifiedIdentifier
    ;

functionRef
    : qualifiedIdentifier
    ;

triggerName
    : qualifiedIdentifier
    ;

triggerRef
    : qualifiedIdentifier
    ;

viewName
    : qualifiedIdentifier
    | dotIdentifier
    ;

viewRef
    : qualifiedIdentifier
    | dotIdentifier
    ;

tablespaceName
    : identifier
    ;

tablespaceRef
    : identifier
    ;

logfileGroupName
    : identifier
    ;

logfileGroupRef
    : identifier
    ;

eventName
    : qualifiedIdentifier
    ;

eventRef
    : qualifiedIdentifier
    ;

udfName
    : // UDFs are referenced at the same places as any other function. So, no dedicated *_ref here.
    identifier
    ;

serverName
    : textOrIdentifier
    ;

serverRef
    : textOrIdentifier
    ;

engineRef
    : textOrIdentifier
    ;

tableName
    : qualifiedIdentifier
    | dotIdentifier
    ;

filterTableRef
    : // Always qualified.
    schemaRef dotIdentifier
    ;

tableRefWithWildcard
    : identifier (
        DOT_SYMBOL MULT_OPERATOR
        | dotIdentifier (DOT_SYMBOL MULT_OPERATOR)?
    )?
    ;

tableRef
    : qualifiedIdentifier
    | dotIdentifier
    ;

tableRefList
    : tableRef (COMMA_SYMBOL tableRef)*
    ;

tableAliasRefList
    : tableRefWithWildcard (COMMA_SYMBOL tableRefWithWildcard)*
    ;

parameterName
    : identifier
    ;

labelIdentifier
    : pureIdentifier
    | labelKeyword
    ;

labelRef
    : labelIdentifier
    ;

roleIdentifier
    : pureIdentifier
    | roleKeyword
    ;

pluginRef
    : identifier
    ;

componentRef
    : textStringLiteral
    ;

resourceGroupRef
    : identifier
    ;

windowName
    : identifier
    ;

//----------------- Common basic rules ---------------------------------------------------------------------------------

// Identifiers excluding keywords (except if they are quoted). IDENT_sys in sql_yacc.yy.
pureIdentifier
    : (IDENTIFIER | BACK_TICK_QUOTED_ID)
    | {this.isPureIdentifier()}? DOUBLE_QUOTED_TEXT
    ;

// Identifiers including a certain set of keywords, which are allowed also if not quoted.
// ident in sql_yacc.yy
identifier
    : pureIdentifier
    | identifierKeyword
    ;

identifierList
    : // ident_string_list in sql_yacc.yy.
    identifier (COMMA_SYMBOL identifier)*
    ;

identifierListWithParentheses
    : OPEN_PAR_SYMBOL identifierList CLOSE_PAR_SYMBOL
    ;

qualifiedIdentifier
    : identifier dotIdentifier?
    ;

simpleIdentifier
    : // simple_ident + simple_ident_q
    identifier (dotIdentifier dotIdentifier?)?
    ;

// This rule encapsulates the frequently used dot + identifier sequence, which also requires a special
// treatment in the lexer. See there in the DOT_IDENTIFIER rule.
dotIdentifier
    : DOT_SYMBOL identifier
    ;

ulong_number
    : INT_NUMBER
    | HEX_NUMBER
    | LONG_NUMBER
    | ULONGLONG_NUMBER
    | DECIMAL_NUMBER
    | FLOAT_NUMBER
    ;

real_ulong_number
    : INT_NUMBER
    | HEX_NUMBER
    | LONG_NUMBER
    | ULONGLONG_NUMBER
    ;

ulonglongNumber
    : INT_NUMBER
    | LONG_NUMBER
    | ULONGLONG_NUMBER
    | DECIMAL_NUMBER
    | FLOAT_NUMBER
    ;

real_ulonglong_number
    : INT_NUMBER
    | {this.isServerVersionGe80017()}? HEX_NUMBER
    | ULONGLONG_NUMBER
    | LONG_NUMBER
    ;

signedLiteral
    : literal
    | PLUS_OPERATOR ulong_number
    | MINUS_OPERATOR ulong_number
    ;

signedLiteralOrNull
    : signedLiteral
    | {this.isServerVersionGe80024()}? nullAsLiteral
    ;

literal
    : textLiteral
    | numLiteral
    | temporalLiteral
    | nullLiteral
    | boolLiteral
    | UNDERSCORE_CHARSET? (HEX_NUMBER | BIN_NUMBER)
    ;

literalOrNull
    : literal
    | {this.isServerVersionGe80024()}? nullAsLiteral
    ;

nullAsLiteral
    : NULL_SYMBOL
    ;

stringList
    : OPEN_PAR_SYMBOL textString (COMMA_SYMBOL textString)* CLOSE_PAR_SYMBOL
    ;

// TEXT_STRING_sys + TEXT_STRING_literal + TEXT_STRING_filesystem + TEXT_STRING + TEXT_STRING_password +
// TEXT_STRING_validated in sql_yacc.yy.
textStringLiteral
    : value = SINGLE_QUOTED_TEXT
    | {this.isTextStringLiteral()}? value = DOUBLE_QUOTED_TEXT
    ;

textString
    : textStringLiteral
    | HEX_NUMBER
    | BIN_NUMBER
    ;

textStringHash
    : textStringLiteral
    | {this.isServerVersionGe80017()}? HEX_NUMBER
    ;

textLiteral
    : (UNDERSCORE_CHARSET? textStringLiteral | NCHAR_TEXT) textStringLiteral*
    ;

// A special variant of a text string that must not contain a linebreak (TEXT_STRING_sys_nonewline in sql_yacc.yy).
// Check validity in semantic phase.
textStringNoLinebreak
    : textStringLiteral
    ;

textStringLiteralList
    : textStringLiteral (COMMA_SYMBOL textStringLiteral)*
    ;

numLiteral
    : int64Literal
    | DECIMAL_NUMBER
    | FLOAT_NUMBER
    ;

boolLiteral
    : TRUE_SYMBOL
    | FALSE_SYMBOL
    ;

nullLiteral
    : // In sql_yacc.cc both 'NULL' and '\N' are mapped to NULL_SYMBOL(which is our nullLiteral).
    NULL_SYMBOL
    | NULL2_SYMBOL
    ;

// int64Literal if for unsigned exact integer literals in a range of [0 .. 2^64-1].
int64Literal
    : INT_NUMBER
    | LONG_NUMBER
    | ULONGLONG_NUMBER
    ;

temporalLiteral
    : DATE_SYMBOL SINGLE_QUOTED_TEXT
    | TIME_SYMBOL SINGLE_QUOTED_TEXT
    | TIMESTAMP_SYMBOL SINGLE_QUOTED_TEXT
    ;

floatOptions
    : fieldLength
    | precision
    ;

standardFloatOptions
    : precision
    ;

precision
    : OPEN_PAR_SYMBOL INT_NUMBER COMMA_SYMBOL INT_NUMBER CLOSE_PAR_SYMBOL
    ;

textOrIdentifier
    : identifier
    | textStringLiteral
    ;

lValueIdentifier
    : pureIdentifier
    | lValueKeyword
    ;

roleIdentifierOrText
    : roleIdentifier
    | textStringLiteral
    ;

sizeNumber
    : real_ulonglong_number
    | pureIdentifier // Something like 10G. Semantic check needed for validity.
    ;

parentheses
    : OPEN_PAR_SYMBOL CLOSE_PAR_SYMBOL
    ;

equal
    : EQUAL_OPERATOR
    | ASSIGN_OPERATOR
    ;

optionType
    : PERSIST_SYMBOL
    | PERSIST_ONLY_SYMBOL
    | GLOBAL_SYMBOL
    | LOCAL_SYMBOL
    | SESSION_SYMBOL
    ;

rvalueSystemVariableType
    : GLOBAL_SYMBOL DOT_SYMBOL
    | LOCAL_SYMBOL DOT_SYMBOL
    | SESSION_SYMBOL DOT_SYMBOL
    ;

setVarIdentType
    : (
        PERSIST_SYMBOL
        | PERSIST_ONLY_SYMBOL
        | GLOBAL_SYMBOL
        | LOCAL_SYMBOL
        | SESSION_SYMBOL
    ) DOT_SYMBOL
    ;

jsonAttribute
    : textStringLiteral
    ;

// Note: rules for non-reserved keywords have changed significantly with MySQL 8.0.17, which make their
//       version dependent handling complicated.
//       Comments for keyword rules are taken over directly from the server grammar, but usually don't apply here
//       since we don't have something like shift/reduce conflicts in ANTLR4 (which those ugly rules try to overcome).

// Non-reserved keywords are allowed as unquoted identifiers in general.
//
// OTOH, in a few particular cases statement-specific rules are used
// instead of `ident_keyword` to avoid grammar ambiguities:
//
//  * `label_keyword` for SP label names
//  * `role_keyword` for role names
//  * `lvalue_keyword` for variable prefixes and names in left sides of
//                     assignments in SET statements
//
// Normally, new non-reserved words should be added to the
// the rule `ident_keywords_unambiguous`. If they cause grammar conflicts, try
// one of `ident_keywords_ambiguous_...` rules instead.
identifierKeyword
    : {this.isServerVersionLt80017()}? (
        labelKeyword
        | roleOrIdentifierKeyword
        | EXECUTE_SYMBOL
        | SHUTDOWN_SYMBOL // Previously allowed as SP label as well.
        | {this.isServerVersionGe80011()}? RESTART_SYMBOL
    )
    | (
        identifierKeywordsUnambiguous
        | identifierKeywordsAmbiguous1RolesAndLabels
        | identifierKeywordsAmbiguous2Labels
        | identifierKeywordsAmbiguous3Roles
        | identifierKeywordsAmbiguous4SystemVariables
    )
    ;

// These non-reserved words cannot be used as role names and SP label names:
identifierKeywordsAmbiguous1RolesAndLabels
    : EXECUTE_SYMBOL
    | RESTART_SYMBOL
    | SHUTDOWN_SYMBOL
    ;

// These non-reserved keywords cannot be used as unquoted SP label names:
identifierKeywordsAmbiguous2Labels
    : ASCII_SYMBOL
    | BEGIN_SYMBOL
    | BYTE_SYMBOL
    | CACHE_SYMBOL
    | CHARSET_SYMBOL
    | CHECKSUM_SYMBOL
    | CLONE_SYMBOL
    | COMMENT_SYMBOL
    | COMMIT_SYMBOL
    | CONTAINS_SYMBOL
    | DEALLOCATE_SYMBOL
    | DO_SYMBOL
    | END_SYMBOL
    | FLUSH_SYMBOL
    | FOLLOWS_SYMBOL
    | HANDLER_SYMBOL
    | HELP_SYMBOL
    | IMPORT_SYMBOL
    | INSTALL_SYMBOL
    | LANGUAGE_SYMBOL
    | NO_SYMBOL
    | PRECEDES_SYMBOL
    | PREPARE_SYMBOL
    | REPAIR_SYMBOL
    | RESET_SYMBOL
    | ROLLBACK_SYMBOL
    | SAVEPOINT_SYMBOL
    | SIGNED_SYMBOL
    | SLAVE_SYMBOL
    | START_SYMBOL
    | STOP_SYMBOL
    | TRUNCATE_SYMBOL
    | UNICODE_SYMBOL
    | UNINSTALL_SYMBOL
    | XA_SYMBOL
    ;

// Keywords that we allow for labels in SPs in the unquoted form.
// Any keyword that is allowed to begin a statement or routine characteristics
// must be in `ident_keywords_ambiguous_2_labels` above, otherwise
// we get (harmful) shift/reduce conflicts.
//
// Not allowed:
//
//   ident_keywords_ambiguous_1_roles_and_labels
//   ident_keywords_ambiguous_2_labels
labelKeyword
    : {this.isServerVersionLt80017()}? (
        roleOrLabelKeyword
        | EVENT_SYMBOL
        | FILE_SYMBOL
        | NONE_SYMBOL
        | PROCESS_SYMBOL
        | PROXY_SYMBOL
        | RELOAD_SYMBOL
        | REPLICATION_SYMBOL
        | RESOURCE_SYMBOL
        | SUPER_SYMBOL
    )
    | (
        identifierKeywordsUnambiguous
        | identifierKeywordsAmbiguous3Roles
        | identifierKeywordsAmbiguous4SystemVariables
    )
    ;

// These non-reserved keywords cannot be used as unquoted role names:
identifierKeywordsAmbiguous3Roles
    : EVENT_SYMBOL
    | FILE_SYMBOL
    | NONE_SYMBOL
    | PROCESS_SYMBOL
    | PROXY_SYMBOL
    | RELOAD_SYMBOL
    | REPLICATION_SYMBOL
    | RESOURCE_SYMBOL
    | SUPER_SYMBOL
    ;

// These are the non-reserved keywords which may be used for unquoted
// identifiers everywhere without introducing grammar conflicts:
identifierKeywordsUnambiguous
    : (
        ACTION_SYMBOL
        | ACCOUNT_SYMBOL
        | ACTIVE_SYMBOL
        | ADDDATE_SYMBOL
        | ADMIN_SYMBOL
        | AFTER_SYMBOL
        | AGAINST_SYMBOL
        | AGGREGATE_SYMBOL
        | ALGORITHM_SYMBOL
        | ALWAYS_SYMBOL
        | ANY_SYMBOL
        | AT_SYMBOL
        | ATTRIBUTE_SYMBOL
        | AUTHENTICATION_SYMBOL
        | AUTOEXTEND_SIZE_SYMBOL
        | AUTO_INCREMENT_SYMBOL
        | AVG_ROW_LENGTH_SYMBOL
        | AVG_SYMBOL
        | BACKUP_SYMBOL
        | BINLOG_SYMBOL
        | BIT_SYMBOL
        | BLOCK_SYMBOL
        | BOOLEAN_SYMBOL
        | BOOL_SYMBOL
        | BTREE_SYMBOL
        | BUCKETS_SYMBOL
        | CASCADED_SYMBOL
        | CATALOG_NAME_SYMBOL
        | CHAIN_SYMBOL
        | CHALLENGE_RESPONSE_SYMBOL
        | CHANGED_SYMBOL
        | CHANNEL_SYMBOL
        | CIPHER_SYMBOL
        | CLASS_ORIGIN_SYMBOL
        | CLIENT_SYMBOL
        | CLOSE_SYMBOL
        | COALESCE_SYMBOL
        | CODE_SYMBOL
        | COLLATION_SYMBOL
        | COLUMNS_SYMBOL
        | COLUMN_FORMAT_SYMBOL
        | COLUMN_NAME_SYMBOL
        | COMMITTED_SYMBOL
        | COMPACT_SYMBOL
        | COMPLETION_SYMBOL
        | COMPONENT_SYMBOL
        | COMPRESSED_SYMBOL
        | COMPRESSION_SYMBOL
        | CONCURRENT_SYMBOL
        | CONNECTION_SYMBOL
        | CONSISTENT_SYMBOL
        | CONSTRAINT_CATALOG_SYMBOL
        | CONSTRAINT_NAME_SYMBOL
        | CONSTRAINT_SCHEMA_SYMBOL
        | CONTEXT_SYMBOL
        | CPU_SYMBOL
        | CURRENT_SYMBOL
        | CURSOR_NAME_SYMBOL
        | DATAFILE_SYMBOL
        | DATA_SYMBOL
        | DATETIME_SYMBOL
        | DATE_SYMBOL
        | DAY_SYMBOL
        | DEFAULT_AUTH_SYMBOL
        | DEFINER_SYMBOL
        | DEFINITION_SYMBOL
        | DELAY_KEY_WRITE_SYMBOL
        | DESCRIPTION_SYMBOL
        | DIAGNOSTICS_SYMBOL
        | DIRECTORY_SYMBOL
        | DISABLE_SYMBOL
        | DISCARD_SYMBOL
        | DISK_SYMBOL
        | DUMPFILE_SYMBOL
        | DUPLICATE_SYMBOL
        | DYNAMIC_SYMBOL
        | ENABLE_SYMBOL
        | ENCRYPTION_SYMBOL
        | ENDS_SYMBOL
        | ENFORCED_SYMBOL
        | ENGINES_SYMBOL
        | ENGINE_SYMBOL
        | ENUM_SYMBOL
        | ERRORS_SYMBOL
        | ERROR_SYMBOL
        | ESCAPE_SYMBOL
        | EVENTS_SYMBOL
        | EVERY_SYMBOL
        | EXCHANGE_SYMBOL
        | EXCLUDE_SYMBOL
        | EXPANSION_SYMBOL
        | EXPIRE_SYMBOL
        | EXPORT_SYMBOL
        | EXTENDED_SYMBOL
        | EXTENT_SIZE_SYMBOL
        | FACTOR_SYMBOL
        | FAST_SYMBOL
        | FAULTS_SYMBOL
        | FILE_BLOCK_SIZE_SYMBOL
        | FILTER_SYMBOL
        | FINISH_SYMBOL
        | FIRST_SYMBOL
        | FIXED_SYMBOL
        | FOLLOWING_SYMBOL
        | FORMAT_SYMBOL
        | FOUND_SYMBOL
        | FULL_SYMBOL
        | GENERAL_SYMBOL
        | GEOMETRYCOLLECTION_SYMBOL
        | GEOMETRY_SYMBOL
        | GET_FORMAT_SYMBOL
        | GET_MASTER_PUBLIC_KEY_SYMBOL
        | GET_SOURCE_PUBLIC_KEY_SYMBOL
        | GRANTS_SYMBOL
        | GROUP_REPLICATION_SYMBOL
        | GTID_ONLY_SYMBOL
        | HASH_SYMBOL
        | HISTOGRAM_SYMBOL
        | HISTORY_SYMBOL
        | HOSTS_SYMBOL
        | HOST_SYMBOL
        | HOUR_SYMBOL
        | IDENTIFIED_SYMBOL
        | IGNORE_SERVER_IDS_SYMBOL
        | INACTIVE_SYMBOL
        | INDEXES_SYMBOL
        | INITIAL_SIZE_SYMBOL
        | INITIAL_SYMBOL
        | INITIATE_SYMBOL
        | INSERT_METHOD_SYMBOL
        | INSTANCE_SYMBOL
        | INVISIBLE_SYMBOL
        | INVOKER_SYMBOL
        | IO_SYMBOL
        | IPC_SYMBOL
        | ISOLATION_SYMBOL
        | ISSUER_SYMBOL
        | JSON_SYMBOL
        | JSON_VALUE_SYMBOL
        | KEY_BLOCK_SIZE_SYMBOL
        | KEYRING_SYMBOL
        | LAST_SYMBOL
        | LEAVES_SYMBOL
        | LESS_SYMBOL
        | LEVEL_SYMBOL
        | LINESTRING_SYMBOL
        | LIST_SYMBOL
        | LOCKED_SYMBOL
        | LOCKS_SYMBOL
        | LOGFILE_SYMBOL
        | LOGS_SYMBOL
        | MASTER_AUTO_POSITION_SYMBOL
        | MASTER_COMPRESSION_ALGORITHM_SYMBOL
        | MASTER_CONNECT_RETRY_SYMBOL
        | MASTER_DELAY_SYMBOL
        | MASTER_HEARTBEAT_PERIOD_SYMBOL
        | MASTER_HOST_SYMBOL
        | NETWORK_NAMESPACE_SYMBOL
        | MASTER_LOG_FILE_SYMBOL
        | MASTER_LOG_POS_SYMBOL
        | MASTER_PASSWORD_SYMBOL
        | MASTER_PORT_SYMBOL
        | MASTER_PUBLIC_KEY_PATH_SYMBOL
        | MASTER_RETRY_COUNT_SYMBOL
        | MASTER_SSL_CAPATH_SYMBOL
        | MASTER_SSL_CA_SYMBOL
        | MASTER_SSL_CERT_SYMBOL
        | MASTER_SSL_CIPHER_SYMBOL
        | MASTER_SSL_CRLPATH_SYMBOL
        | MASTER_SSL_CRL_SYMBOL
        | MASTER_SSL_KEY_SYMBOL
        | MASTER_SSL_SYMBOL
        | MASTER_SYMBOL
        | MASTER_TLS_CIPHERSUITES_SYMBOL
        | MASTER_TLS_VERSION_SYMBOL
        | MASTER_USER_SYMBOL
        | MASTER_ZSTD_COMPRESSION_LEVEL_SYMBOL
        | MAX_CONNECTIONS_PER_HOUR_SYMBOL
        | MAX_QUERIES_PER_HOUR_SYMBOL
        | MAX_ROWS_SYMBOL
        | MAX_SIZE_SYMBOL
        | MAX_UPDATES_PER_HOUR_SYMBOL
        | MAX_USER_CONNECTIONS_SYMBOL
        | MEDIUM_SYMBOL
        | MEMORY_SYMBOL
        | MERGE_SYMBOL
        | MESSAGE_TEXT_SYMBOL
        | MICROSECOND_SYMBOL
        | MIGRATE_SYMBOL
        | MINUTE_SYMBOL
        | MIN_ROWS_SYMBOL
        | MODE_SYMBOL
        | MODIFY_SYMBOL
        | MONTH_SYMBOL
        | MULTILINESTRING_SYMBOL
        | MULTIPOINT_SYMBOL
        | MULTIPOLYGON_SYMBOL
        | MUTEX_SYMBOL
        | MYSQL_ERRNO_SYMBOL
        | NAMES_SYMBOL
        | NAME_SYMBOL
        | NATIONAL_SYMBOL
        | NCHAR_SYMBOL
        | NDBCLUSTER_SYMBOL
        | NESTED_SYMBOL
        | NEVER_SYMBOL
        | NEW_SYMBOL
        | NEXT_SYMBOL
        | NODEGROUP_SYMBOL
        | NOWAIT_SYMBOL
        | NO_WAIT_SYMBOL
        | NULLS_SYMBOL
        | NUMBER_SYMBOL
        | NVARCHAR_SYMBOL
        | OFFSET_SYMBOL
        | OJ_SYMBOL
        | OLD_SYMBOL
        | ONE_SYMBOL
        | ONLY_SYMBOL
        | OPEN_SYMBOL
        | OPTIONAL_SYMBOL
        | OPTIONS_SYMBOL
        | ORDINALITY_SYMBOL
        | ORGANIZATION_SYMBOL
        | OTHERS_SYMBOL
        | OWNER_SYMBOL
        | PACK_KEYS_SYMBOL
        | PAGE_SYMBOL
        | PARSER_SYMBOL
        | PARTIAL_SYMBOL
        | PARTITIONING_SYMBOL
        | PARTITIONS_SYMBOL
        | PASSWORD_SYMBOL
        | PATH_SYMBOL
        | PHASE_SYMBOL
        | PLUGINS_SYMBOL
        | PLUGIN_DIR_SYMBOL
        | PLUGIN_SYMBOL
        | POINT_SYMBOL
        | POLYGON_SYMBOL
        | PORT_SYMBOL
        | PRECEDING_SYMBOL
        | PRESERVE_SYMBOL
        | PREV_SYMBOL
        | PRIVILEGES_SYMBOL
        | PRIVILEGE_CHECKS_USER_SYMBOL
        | PROCESSLIST_SYMBOL
        | PROFILES_SYMBOL
        | PROFILE_SYMBOL
        | QUARTER_SYMBOL
        | QUERY_SYMBOL
        | QUICK_SYMBOL
        | READ_ONLY_SYMBOL
        | REBUILD_SYMBOL
        | RECOVER_SYMBOL
        | REDO_BUFFER_SIZE_SYMBOL
        | REDUNDANT_SYMBOL
        | REFERENCE_SYMBOL
        | REGISTRATION_SYMBOL
        | RELAY_SYMBOL
        | RELAYLOG_SYMBOL
        | RELAY_LOG_FILE_SYMBOL
        | RELAY_LOG_POS_SYMBOL
        | RELAY_THREAD_SYMBOL
        | REMOVE_SYMBOL
        | ASSIGN_GTIDS_TO_ANONYMOUS_TRANSACTIONS_SYMBOL
        | REORGANIZE_SYMBOL
        | REPEATABLE_SYMBOL
        | REPLICAS_SYMBOL
        | REPLICATE_DO_DB_SYMBOL
        | REPLICATE_DO_TABLE_SYMBOL
        | REPLICATE_IGNORE_DB_SYMBOL
        | REPLICATE_IGNORE_TABLE_SYMBOL
        | REPLICATE_REWRITE_DB_SYMBOL
        | REPLICATE_WILD_DO_TABLE_SYMBOL
        | REPLICATE_WILD_IGNORE_TABLE_SYMBOL
        | REPLICA_SYMBOL
        | USER_RESOURCES_SYMBOL
        | RESPECT_SYMBOL
        | RESTORE_SYMBOL
        | RESUME_SYMBOL
        | RETAIN_SYMBOL
        | RETURNED_SQLSTATE_SYMBOL
        | RETURNING_SYMBOL
        | RETURNS_SYMBOL
        | REUSE_SYMBOL
        | REVERSE_SYMBOL
        | ROLE_SYMBOL
        | ROLLUP_SYMBOL
        | ROTATE_SYMBOL
        | ROUTINE_SYMBOL
        | ROW_COUNT_SYMBOL
        | ROW_FORMAT_SYMBOL
        | RTREE_SYMBOL
        | SCHEDULE_SYMBOL
        | SCHEMA_NAME_SYMBOL
        | SECONDARY_ENGINE_SYMBOL
        | SECONDARY_ENGINE_ATTRIBUTE_SYMBOL
        | SECONDARY_LOAD_SYMBOL
        | SECONDARY_SYMBOL
        | SECONDARY_UNLOAD_SYMBOL
        | SECOND_SYMBOL
        | SECURITY_SYMBOL
        | SERIALIZABLE_SYMBOL
        | SERIAL_SYMBOL
        | SERVER_SYMBOL
        | SHARE_SYMBOL
        | SIMPLE_SYMBOL
        | SKIP_SYMBOL
        | SLOW_SYMBOL
        | SNAPSHOT_SYMBOL
        | SOCKET_SYMBOL
        | SONAME_SYMBOL
        | SOUNDS_SYMBOL
        | SOURCE_AUTO_POSITION_SYMBOL
        | SOURCE_BIND_SYMBOL
        | SOURCE_COMPRESSION_ALGORITHM_SYMBOL
        | SOURCE_CONNECTION_AUTO_FAILOVER_SYMBOL
        | SOURCE_CONNECT_RETRY_SYMBOL
        | SOURCE_DELAY_SYMBOL
        | SOURCE_HEARTBEAT_PERIOD_SYMBOL
        | SOURCE_HOST_SYMBOL
        | SOURCE_LOG_FILE_SYMBOL
        | SOURCE_LOG_POS_SYMBOL
        | SOURCE_PASSWORD_SYMBOL
        | SOURCE_PORT_SYMBOL
        | SOURCE_PUBLIC_KEY_PATH_SYMBOL
        | SOURCE_RETRY_COUNT_SYMBOL
        | SOURCE_SSL_CAPATH_SYMBOL
        | SOURCE_SSL_CA_SYMBOL
        | SOURCE_SSL_CERT_SYMBOL
        | SOURCE_SSL_CIPHER_SYMBOL
        | SOURCE_SSL_CRLPATH_SYMBOL
        | SOURCE_SSL_CRL_SYMBOL
        | SOURCE_SSL_KEY_SYMBOL
        | SOURCE_SSL_SYMBOL
        | SOURCE_SSL_VERIFY_SERVER_CERT_SYMBOL
        | SOURCE_SYMBOL
        | SOURCE_TLS_CIPHERSUITES_SYMBOL
        | SOURCE_TLS_VERSION_SYMBOL
        | SOURCE_USER_SYMBOL
        | SOURCE_ZSTD_COMPRESSION_LEVEL_SYMBOL
        | SQL_AFTER_GTIDS_SYMBOL
        | SQL_AFTER_MTS_GAPS_SYMBOL
        | SQL_BEFORE_GTIDS_SYMBOL
        | SQL_BUFFER_RESULT_SYMBOL
        | SQL_NO_CACHE_SYMBOL
        | SQL_THREAD_SYMBOL
        | SRID_SYMBOL
        | STACKED_SYMBOL
        | STARTS_SYMBOL
        | STATS_AUTO_RECALC_SYMBOL
        | STATS_PERSISTENT_SYMBOL
        | STATS_SAMPLE_PAGES_SYMBOL
        | STATUS_SYMBOL
        | STORAGE_SYMBOL
        | STRING_SYMBOL
        | ST_COLLECT_SYMBOL
        | SUBCLASS_ORIGIN_SYMBOL
        | SUBDATE_SYMBOL
        | SUBJECT_SYMBOL
        | SUBPARTITIONS_SYMBOL
        | SUBPARTITION_SYMBOL
        | SUSPEND_SYMBOL
        | SWAPS_SYMBOL
        | SWITCHES_SYMBOL
        | TABLES_SYMBOL
        | TABLESPACE_SYMBOL
        | TABLE_CHECKSUM_SYMBOL
        | TABLE_NAME_SYMBOL
        | TEMPORARY_SYMBOL
        | TEMPTABLE_SYMBOL
        | TEXT_SYMBOL
        | THAN_SYMBOL
        | THREAD_PRIORITY_SYMBOL
        | TIES_SYMBOL
        | TIMESTAMPADD_SYMBOL
        | TIMESTAMPDIFF_SYMBOL
        | TIMESTAMP_SYMBOL
        | TIME_SYMBOL
        | TLS_SYMBOL
        | TRANSACTION_SYMBOL
        | TRIGGERS_SYMBOL
        | TYPES_SYMBOL
        | TYPE_SYMBOL
        | UNBOUNDED_SYMBOL
        | UNCOMMITTED_SYMBOL
        | UNDEFINED_SYMBOL
        | UNDOFILE_SYMBOL
        | UNDO_BUFFER_SIZE_SYMBOL
        | UNKNOWN_SYMBOL
        | UNREGISTER_SYMBOL
        | UNTIL_SYMBOL
        | UPGRADE_SYMBOL
        | USER_SYMBOL
        | USE_FRM_SYMBOL
        | VALIDATION_SYMBOL
        | VALUE_SYMBOL
        | VARIABLES_SYMBOL
        | VCPU_SYMBOL
        | VIEW_SYMBOL
        | VISIBLE_SYMBOL
        | WAIT_SYMBOL
        | WARNINGS_SYMBOL
        | WEEK_SYMBOL
        | WEIGHT_STRING_SYMBOL
        | WITHOUT_SYMBOL
        | WORK_SYMBOL
        | WRAPPER_SYMBOL
        | X509_SYMBOL
        | XID_SYMBOL
        | XML_SYMBOL
        | YEAR_SYMBOL
        | ZONE_SYMBOL
    )
    | {this.isServerVersionGe80019()}? (
        ARRAY_SYMBOL
        | FAILED_LOGIN_ATTEMPTS_SYMBOL
        | MASTER_COMPRESSION_ALGORITHM_SYMBOL
        | MASTER_TLS_CIPHERSUITES_SYMBOL
        | MASTER_ZSTD_COMPRESSION_LEVEL_SYMBOL
        | MEMBER_SYMBOL
        | OFF_SYMBOL
        | PASSWORD_LOCK_TIME_SYMBOL
        | PRIVILEGE_CHECKS_USER_SYMBOL
        | RANDOM_SYMBOL
        | REQUIRE_ROW_FORMAT_SYMBOL
        | REQUIRE_TABLE_PRIMARY_KEY_CHECK_SYMBOL
        | STREAM_SYMBOL
        | TIMESTAMP_SYMBOL
        | TIME_SYMBOL
    )
    | {this.isServerVersionGe80200()}? (
        BULK_SYMBOL
        | GENERATE_SYMBOL
        | GTIDS_SYMBOL
        | LOG_SYMBOL
        | PARSE_TREE_SYMBOL
        | S3_SYMBOL
        | BERNOULLI_SYMBOL
    )
    /* INSERT OTHER KEYWORDS HERE */
    ;

// Non-reserved keywords that we allow for unquoted role names:
//
//  Not allowed:
//
//    ident_keywords_ambiguous_1_roles_and_labels
//    ident_keywords_ambiguous_3_roles
roleKeyword
    : {this.isServerVersionLt80017()}? (roleOrLabelKeyword | roleOrIdentifierKeyword)
    | (
        identifierKeywordsUnambiguous
        | identifierKeywordsAmbiguous2Labels
        | identifierKeywordsAmbiguous4SystemVariables
    )
    ;

// Non-reserved words allowed for unquoted unprefixed variable names and
// unquoted variable prefixes in the left side of assignments in SET statements:
//
// Not allowed:
//
//   ident_keywords_ambiguous_4_system_variables
lValueKeyword
    : identifierKeywordsUnambiguous
    | identifierKeywordsAmbiguous1RolesAndLabels
    | identifierKeywordsAmbiguous2Labels
    | identifierKeywordsAmbiguous3Roles
    ;

// These non-reserved keywords cannot be used as unquoted unprefixed
// variable names and unquoted variable prefixes in the left side of
// assignments in SET statements:
identifierKeywordsAmbiguous4SystemVariables
    : GLOBAL_SYMBOL
    | LOCAL_SYMBOL
    | PERSIST_SYMBOL
    | PERSIST_ONLY_SYMBOL
    | SESSION_SYMBOL
    ;

// $antlr-format groupedAlignments off

// These are the non-reserved keywords which may be used for roles or idents.
// Keywords defined only for specific server versions are handled at lexer level and so cannot match this rule
// if the current server version doesn't allow them. Hence we don't need predicates here for them.
roleOrIdentifierKeyword
    : (
        ACCOUNT_SYMBOL
        | ASCII_SYMBOL
        | ALWAYS_SYMBOL
        | BACKUP_SYMBOL
        | BEGIN_SYMBOL
        | BYTE_SYMBOL
        | CACHE_SYMBOL
        | CHARSET_SYMBOL
        | CHECKSUM_SYMBOL
        | CLONE_SYMBOL
        | CLOSE_SYMBOL
        | COMMENT_SYMBOL
        | COMMIT_SYMBOL
        | CONTAINS_SYMBOL
        | DEALLOCATE_SYMBOL
        | DO_SYMBOL
        | END_SYMBOL
        | FLUSH_SYMBOL
        | FOLLOWS_SYMBOL
        | FORMAT_SYMBOL
        | GROUP_REPLICATION_SYMBOL
        | HANDLER_SYMBOL
        | HELP_SYMBOL
        | HOST_SYMBOL
        | INSTALL_SYMBOL
        | INVISIBLE_SYMBOL
        | LANGUAGE_SYMBOL
        | NO_SYMBOL
        | OPEN_SYMBOL
        | OPTIONS_SYMBOL
        | OWNER_SYMBOL
        | PARSER_SYMBOL
        | PARTITION_SYMBOL
        | PORT_SYMBOL
        | PRECEDES_SYMBOL
        | PREPARE_SYMBOL
        | REMOVE_SYMBOL
        | REPAIR_SYMBOL
        | RESET_SYMBOL
        | RESTORE_SYMBOL
        | ROLE_SYMBOL
        | ROLLBACK_SYMBOL
        | SAVEPOINT_SYMBOL
        | SECONDARY_SYMBOL
        | SECONDARY_ENGINE_SYMBOL
        | SECONDARY_LOAD_SYMBOL
        | SECONDARY_UNLOAD_SYMBOL
        | SECURITY_SYMBOL
        | SERVER_SYMBOL
        | SIGNED_SYMBOL
        | SOCKET_SYMBOL
        | SLAVE_SYMBOL
        | SONAME_SYMBOL
        | START_SYMBOL
        | STOP_SYMBOL
        | TRUNCATE_SYMBOL
        | UNICODE_SYMBOL
        | UNINSTALL_SYMBOL
        | UPGRADE_SYMBOL
        | VISIBLE_SYMBOL
        | WRAPPER_SYMBOL
        | XA_SYMBOL
    )
    ;

roleOrLabelKeyword
    : (
        ACTION_SYMBOL
        | ACTIVE_SYMBOL
        | ADDDATE_SYMBOL
        | AFTER_SYMBOL
        | AGAINST_SYMBOL
        | AGGREGATE_SYMBOL
        | ALGORITHM_SYMBOL
        | ANY_SYMBOL
        | AT_SYMBOL
        | AUTO_INCREMENT_SYMBOL
        | AUTOEXTEND_SIZE_SYMBOL
        | AVG_ROW_LENGTH_SYMBOL
        | AVG_SYMBOL
        | BINLOG_SYMBOL
        | BIT_SYMBOL
        | BLOCK_SYMBOL
        | BOOL_SYMBOL
        | BOOLEAN_SYMBOL
        | BTREE_SYMBOL
        | BUCKETS_SYMBOL
        | CASCADED_SYMBOL
        | CATALOG_NAME_SYMBOL
        | CHAIN_SYMBOL
        | CHANGED_SYMBOL
        | CHANNEL_SYMBOL
        | CIPHER_SYMBOL
        | CLIENT_SYMBOL
        | CLASS_ORIGIN_SYMBOL
        | COALESCE_SYMBOL
        | CODE_SYMBOL
        | COLLATION_SYMBOL
        | COLUMN_NAME_SYMBOL
        | COLUMN_FORMAT_SYMBOL
        | COLUMNS_SYMBOL
        | COMMITTED_SYMBOL
        | COMPACT_SYMBOL
        | COMPLETION_SYMBOL
        | COMPONENT_SYMBOL
        | COMPRESSED_SYMBOL
        | COMPRESSION_SYMBOL
        | CONCURRENT_SYMBOL
        | CONNECTION_SYMBOL
        | CONSISTENT_SYMBOL
        | CONSTRAINT_CATALOG_SYMBOL
        | CONSTRAINT_SCHEMA_SYMBOL
        | CONSTRAINT_NAME_SYMBOL
        | CONTEXT_SYMBOL
        | CPU_SYMBOL
        | CURRENT_SYMBOL
        | CURSOR_NAME_SYMBOL
        | DATA_SYMBOL
        | DATAFILE_SYMBOL
        | DATETIME_SYMBOL
        | DATE_SYMBOL
        | DAY_SYMBOL
        | DEFAULT_AUTH_SYMBOL
        | DEFINER_SYMBOL
        | DELAY_KEY_WRITE_SYMBOL
        | DESCRIPTION_SYMBOL
        | DIAGNOSTICS_SYMBOL
        | DIRECTORY_SYMBOL
        | DISABLE_SYMBOL
        | DISCARD_SYMBOL
        | DISK_SYMBOL
        | DUMPFILE_SYMBOL
        | DUPLICATE_SYMBOL
        | DYNAMIC_SYMBOL
        | ENCRYPTION_SYMBOL
        | ENDS_SYMBOL
        | ENUM_SYMBOL
        | ENGINE_SYMBOL
        | ENGINES_SYMBOL
        | ENGINE_ATTRIBUTE_SYMBOL
        | ERROR_SYMBOL
        | ERRORS_SYMBOL
        | ESCAPE_SYMBOL
        | EVENTS_SYMBOL
        | EVERY_SYMBOL
        | EXCLUDE_SYMBOL
        | EXPANSION_SYMBOL
        | EXPORT_SYMBOL
        | EXTENDED_SYMBOL
        | EXTENT_SIZE_SYMBOL
        | FAULTS_SYMBOL
        | FAST_SYMBOL
        | FOLLOWING_SYMBOL
        | FOUND_SYMBOL
        | ENABLE_SYMBOL
        | FULL_SYMBOL
        | FILE_BLOCK_SIZE_SYMBOL
        | FILTER_SYMBOL
        | FIRST_SYMBOL
        | FIXED_SYMBOL
        | GENERAL_SYMBOL
        | GEOMETRY_SYMBOL
        | GEOMETRYCOLLECTION_SYMBOL
        | GET_FORMAT_SYMBOL
        | GRANTS_SYMBOL
        | GLOBAL_SYMBOL
        | HASH_SYMBOL
        | HISTOGRAM_SYMBOL
        | HISTORY_SYMBOL
        | HOSTS_SYMBOL
        | HOUR_SYMBOL
        | IDENTIFIED_SYMBOL
        | IGNORE_SERVER_IDS_SYMBOL
        | INVOKER_SYMBOL
        | INDEXES_SYMBOL
        | INITIAL_SIZE_SYMBOL
        | INSTANCE_SYMBOL
        | INACTIVE_SYMBOL
        | IO_SYMBOL
        | IPC_SYMBOL
        | ISOLATION_SYMBOL
        | ISSUER_SYMBOL
        | INSERT_METHOD_SYMBOL
        | JSON_SYMBOL
        | KEY_BLOCK_SIZE_SYMBOL
        | LAST_SYMBOL
        | LEAVES_SYMBOL
        | LESS_SYMBOL
        | LEVEL_SYMBOL
        | LINESTRING_SYMBOL
        | LIST_SYMBOL
        | LOCAL_SYMBOL
        | LOCKED_SYMBOL
        | LOCKS_SYMBOL
        | LOGFILE_SYMBOL
        | LOGS_SYMBOL
        | MAX_ROWS_SYMBOL
        | MASTER_SYMBOL
        | MASTER_HEARTBEAT_PERIOD_SYMBOL
        | MASTER_HOST_SYMBOL
        | MASTER_PORT_SYMBOL
        | MASTER_LOG_FILE_SYMBOL
        | MASTER_LOG_POS_SYMBOL
        | MASTER_USER_SYMBOL
        | MASTER_PASSWORD_SYMBOL
        | MASTER_PUBLIC_KEY_PATH_SYMBOL
        | MASTER_CONNECT_RETRY_SYMBOL
        | MASTER_RETRY_COUNT_SYMBOL
        | MASTER_DELAY_SYMBOL
        | MASTER_SSL_SYMBOL
        | MASTER_SSL_CA_SYMBOL
        | MASTER_SSL_CAPATH_SYMBOL
        | MASTER_TLS_VERSION_SYMBOL
        | MASTER_SSL_CERT_SYMBOL
        | MASTER_SSL_CIPHER_SYMBOL
        | MASTER_SSL_CRL_SYMBOL
        | MASTER_SSL_CRLPATH_SYMBOL
        | MASTER_SSL_KEY_SYMBOL
        | MASTER_AUTO_POSITION_SYMBOL
        | MAX_CONNECTIONS_PER_HOUR_SYMBOL
        | MAX_QUERIES_PER_HOUR_SYMBOL
        | MAX_SIZE_SYMBOL
        | MAX_UPDATES_PER_HOUR_SYMBOL
        | MAX_USER_CONNECTIONS_SYMBOL
        | MEDIUM_SYMBOL
        | MEMORY_SYMBOL
        | MERGE_SYMBOL
        | MESSAGE_TEXT_SYMBOL
        | MICROSECOND_SYMBOL
        | MIGRATE_SYMBOL
        | MINUTE_SYMBOL
        | MIN_ROWS_SYMBOL
        | MODIFY_SYMBOL
        | MODE_SYMBOL
        | MONTH_SYMBOL
        | MULTILINESTRING_SYMBOL
        | MULTIPOINT_SYMBOL
        | MULTIPOLYGON_SYMBOL
        | MUTEX_SYMBOL
        | MYSQL_ERRNO_SYMBOL
        | NAME_SYMBOL
        | NAMES_SYMBOL
        | NATIONAL_SYMBOL
        | NCHAR_SYMBOL
        | NDBCLUSTER_SYMBOL
        | NESTED_SYMBOL
        | NEVER_SYMBOL
        | NEXT_SYMBOL
        | NEW_SYMBOL
        | NO_WAIT_SYMBOL
        | NODEGROUP_SYMBOL
        | NULLS_SYMBOL
        | NOWAIT_SYMBOL
        | NUMBER_SYMBOL
        | NVARCHAR_SYMBOL
        | OFFSET_SYMBOL
        | OLD_SYMBOL
        | ONE_SYMBOL
        | OPTIONAL_SYMBOL
        | ORDINALITY_SYMBOL
        | ORGANIZATION_SYMBOL
        | OTHERS_SYMBOL
        | PACK_KEYS_SYMBOL
        | PAGE_SYMBOL
        | PARTIAL_SYMBOL
        | PARTITIONING_SYMBOL
        | PARTITIONS_SYMBOL
        | PASSWORD_SYMBOL
        | PATH_SYMBOL
        | PHASE_SYMBOL
        | PLUGIN_DIR_SYMBOL
        | PLUGIN_SYMBOL
        | PLUGINS_SYMBOL
        | POINT_SYMBOL
        | POLYGON_SYMBOL
        | PRECEDING_SYMBOL
        | PRESERVE_SYMBOL
        | PREV_SYMBOL
        | THREAD_PRIORITY_SYMBOL
        | PRIVILEGES_SYMBOL
        | PROCESSLIST_SYMBOL
        | PROFILE_SYMBOL
        | PROFILES_SYMBOL
        | QUARTER_SYMBOL
        | QUERY_SYMBOL
        | QUICK_SYMBOL
        | READ_ONLY_SYMBOL
        | REBUILD_SYMBOL
        | RECOVER_SYMBOL
        | REDO_BUFFER_SIZE_SYMBOL
        | REDUNDANT_SYMBOL
        | RELAY_SYMBOL
        | RELAYLOG_SYMBOL
        | RELAY_LOG_FILE_SYMBOL
        | RELAY_LOG_POS_SYMBOL
        | RELAY_THREAD_SYMBOL
        | REMOTE_SYMBOL
        | REORGANIZE_SYMBOL
        | REPEATABLE_SYMBOL
        | REPLICATE_DO_DB_SYMBOL
        | REPLICATE_IGNORE_DB_SYMBOL
        | REPLICATE_DO_TABLE_SYMBOL
        | REPLICATE_IGNORE_TABLE_SYMBOL
        | REPLICATE_WILD_DO_TABLE_SYMBOL
        | REPLICATE_WILD_IGNORE_TABLE_SYMBOL
        | REPLICATE_REWRITE_DB_SYMBOL
        | USER_RESOURCES_SYMBOL
        | RESPECT_SYMBOL
        | RESUME_SYMBOL
        | RETAIN_SYMBOL
        | RETURNED_SQLSTATE_SYMBOL
        | RETURNS_SYMBOL
        | REUSE_SYMBOL
        | REVERSE_SYMBOL
        | ROLLUP_SYMBOL
        | ROTATE_SYMBOL
        | ROUTINE_SYMBOL
        | ROW_COUNT_SYMBOL
        | ROW_FORMAT_SYMBOL
        | RTREE_SYMBOL
        | SCHEDULE_SYMBOL
        | SCHEMA_NAME_SYMBOL
        | SECOND_SYMBOL
        | SERIAL_SYMBOL
        | SERIALIZABLE_SYMBOL
        | SESSION_SYMBOL
        | SHARE_SYMBOL
        | SIMPLE_SYMBOL
        | SKIP_SYMBOL
        | SLOW_SYMBOL
        | SNAPSHOT_SYMBOL
        | SOUNDS_SYMBOL
        | SOURCE_SYMBOL
        | SQL_AFTER_GTIDS_SYMBOL
        | SQL_AFTER_MTS_GAPS_SYMBOL
        | SQL_BEFORE_GTIDS_SYMBOL
        | SQL_BUFFER_RESULT_SYMBOL
        | SQL_NO_CACHE_SYMBOL
        | SQL_THREAD_SYMBOL
        | SRID_SYMBOL
        | STACKED_SYMBOL
        | STARTS_SYMBOL
        | STATS_AUTO_RECALC_SYMBOL
        | STATS_PERSISTENT_SYMBOL
        | STATS_SAMPLE_PAGES_SYMBOL
        | STATUS_SYMBOL
        | STORAGE_SYMBOL
        | STRING_SYMBOL
        | SUBCLASS_ORIGIN_SYMBOL
        | SUBDATE_SYMBOL
        | SUBJECT_SYMBOL
        | SUBPARTITION_SYMBOL
        | SUBPARTITIONS_SYMBOL
        | SUPER_SYMBOL
        | SUSPEND_SYMBOL
        | SWAPS_SYMBOL
        | SWITCHES_SYMBOL
        | TABLE_NAME_SYMBOL
        | TABLES_SYMBOL
        | TABLE_CHECKSUM_SYMBOL
        | TABLESPACE_SYMBOL
        | TEMPORARY_SYMBOL
        | TEMPTABLE_SYMBOL
        | TEXT_SYMBOL
        | THAN_SYMBOL
        | TIES_SYMBOL
        | TRANSACTION_SYMBOL
        | TRIGGERS_SYMBOL
        | TIMESTAMP_SYMBOL
        | TIMESTAMPADD_SYMBOL
        | TIMESTAMPDIFF_SYMBOL
        | TIME_SYMBOL
        | TYPES_SYMBOL
        | TYPE_SYMBOL
        | UDF_RETURNS_SYMBOL
        | UNBOUNDED_SYMBOL
        | UNCOMMITTED_SYMBOL
        | UNDEFINED_SYMBOL
        | UNDO_BUFFER_SIZE_SYMBOL
        | UNDOFILE_SYMBOL
        | UNKNOWN_SYMBOL
        | UNTIL_SYMBOL
        | USER_SYMBOL
        | USE_FRM_SYMBOL
        | VARIABLES_SYMBOL
        | VCPU_SYMBOL
        | VIEW_SYMBOL
        | VALUE_SYMBOL
        | WARNINGS_SYMBOL
        | WAIT_SYMBOL
        | WEEK_SYMBOL
        | WORK_SYMBOL
        | WEIGHT_STRING_SYMBOL
        | X509_SYMBOL
        | XID_SYMBOL
        | XML_SYMBOL
        | YEAR_SYMBOL
    )
    // Tokens that entered or left this rule in specific versions and are not automatically
    // handled in the lexer.
    | {this.isServerVersionGe80014()}? ADMIN_SYMBOL
    ;