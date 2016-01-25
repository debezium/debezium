/*
 * Copyright 2016 Debezium Authors.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.ddl;

import java.util.Arrays;
import java.util.List;

public interface DdlConstants {

    public static final long KILO = 1024;
    public static final long MEGA = KILO * 1024;
    public static final long GIGA = MEGA * 1024;

    public static final String DEFAULT_TERMINATOR = ";";

    /*
     * Character Strings
     */
    public static final String SPACE = " ";
    public static final String PERIOD = ".";
    public static final String COMMA = ",";
    public static final String SEMICOLON = ";";
    public static final String L_PAREN = "(";
    public static final String R_PAREN = ")";
    public static final String L_SQUOTE = "’";
    public static final String LS_BRACE = "[";
    public static final String RS_BRACE = "]";

    /*
     * Table Constraint ID's
     */
    public static final int CONSTRAINT_UC = 0;
    public static final int CONSTRAINT_FK = 1;
    public static final int CONSTRAINT_PK = 2;
    public static final int CONSTRAINT_C = 3;

    public static final String PRIMARY_KEY = "PRIMARY KEY";
    public static final String FOREIGN_KEY = "FOREIGN KEY";

    /*
     * Common DDL Keywords
     */
    public static final String ALTER = "ALTER";
    public static final String CHECK = "CHECK";
    public static final String COLUMN = "COLUMN";
    public static final String CONSTRAINT = "CONSTRAINT";
    public static final String CREATE = "CREATE";
    public static final String DECLARE = "DECLARE";
    public static final String DROP = "DROP";
    public static final String FOREIGN = "FOREIGN";
    public static final String GRANT = "GRANT";
    public static final String REVOKE = "REVOKE";
    public static final String INDEX = "INDEX";
    public static final String INSERT = "INSERT";
    public static final String UPDATE = "UPDATE";
    public static final String DELETE = "DELETE";
    public static final String SELECT = "SELECT";
    public static final String KEY = "KEY";
    public static final String OFF = "OFF";
    public static final String ON = "ON";
    public static final String PRIMARY = "PRIMARY";
    public static final String SCHEMA = "SCHEMA";
    public static final String SET = "SET";
    public static final String TABLE = "TABLE";
    public static final String UNIQUE = "UNIQUE";
    public static final String VIEW = "VIEW";
    public static final String NOT = "NOT";
    public static final String NULL = "NULL";

    public static final String MISSING_TERMINATOR_NODE_LITERAL = "missingTerminator";

    interface DropBehavior {
        public static final String CASCADE = "CASCADE";
        public static final String RESTRICT = "RESTRICT";
    }

    interface MatchType {
        public static final String FULL = "FULL";
        public static final String PARTIAL = "PARTIAL";
    }

    interface ReferencialAction {
        public static final String CASCADE = "CASCADE";
        public static final String SET_NULL = "SET NULL";
        public static final String SET_DEFAULT = "SET DEFAULT";
        public static final String NO_ACTION = "NO ACTION";
    }

    // Basic SQL 92 statement start phrases
    interface StatementStartPhrases {
        public static final String[] STMT_CREATE_SCHEMA = {CREATE, SCHEMA};
        public static final String[] STMT_CREATE_TABLE = {CREATE, TABLE}; // { GLOBAL | LOCAL } TEMPORARY ] TABLE
        public static final String[] STMT_CREATE_GLOBAL_TEMPORARY_TABLE = {CREATE, "GLOBAL", "TEMPORARY", TABLE};
        public static final String[] STMT_CREATE_LOCAL_TEMPORARY_TABLE = {CREATE, "LOCAL", "TEMPORARY", TABLE};
        public static final String[] STMT_CREATE_VIEW = {CREATE, VIEW};
        public static final String[] STMT_CREATE_OR_REPLACE_VIEW = {CREATE, "OR", "REPLACE", VIEW};
        public static final String[] STMT_CREATE_ASSERTION = {CREATE, "ASSERTION"};
        public static final String[] STMT_CREATE_CHARACTER_SET = {CREATE, "CHARACTER", SET};
        public static final String[] STMT_CREATE_COLLATION = {CREATE, "COLLATION"};
        public static final String[] STMT_CREATE_DOMAIN = {CREATE, "DOMAIN"};
        public static final String[] STMT_CREATE_TRANSLATION = {CREATE, "TRANSLATION"};
        public static final String[] STMT_ALTER_TABLE = {ALTER, TABLE};
        public static final String[] STMT_ALTER_DOMAIN = {ALTER, "DOMAIN"};
        public static final String[] STMT_GRANT = {GRANT};
        public static final String[] STMT_REVOKE = {REVOKE};
        public static final String[] STMT_DROP_SCHEMA = {DROP, SCHEMA};
        public static final String[] STMT_DROP_TABLE = {DROP, TABLE};
        public static final String[] STMT_DROP_VIEW = {DROP, VIEW};
        public static final String[] STMT_DROP_DOMAIN = {DROP, "DOMAIN"};
        public static final String[] STMT_DROP_CHARACTER_SET = {DROP, "CHARACTER", SET};
        public static final String[] STMT_DROP_COLLATION = {DROP, "COLLATION"};
        public static final String[] STMT_DROP_TRANSLATION = {DROP, "TRANSLATION"};
        public static final String[] STMT_DROP_ASSERTION = {DROP, "ASSERTION"};
        public static final String[] STMT_INSERT_INTO = {"INSERT", "INTO"};
        public static final String[] STMT_SET_DEFINE = {"SET", "DEFINE"};

        public final static String[][] SQL_92_ALL_PHRASES = {STMT_CREATE_SCHEMA, STMT_CREATE_TABLE,
            STMT_CREATE_GLOBAL_TEMPORARY_TABLE, STMT_CREATE_LOCAL_TEMPORARY_TABLE, STMT_CREATE_VIEW, STMT_CREATE_OR_REPLACE_VIEW,
            STMT_CREATE_ASSERTION, STMT_CREATE_CHARACTER_SET, STMT_CREATE_COLLATION, STMT_CREATE_TRANSLATION, STMT_CREATE_DOMAIN,
            STMT_ALTER_TABLE, STMT_ALTER_DOMAIN, STMT_GRANT, STMT_REVOKE, STMT_DROP_SCHEMA, STMT_DROP_TABLE, STMT_DROP_VIEW,
            STMT_DROP_DOMAIN, STMT_DROP_CHARACTER_SET, STMT_DROP_COLLATION, STMT_DROP_TRANSLATION, STMT_DROP_ASSERTION,
            STMT_INSERT_INTO, STMT_SET_DEFINE};
    }

    /**
     * Constants related to Data Types
     */
    interface DataTypes {
        public static final int DTYPE_CODE_ANY = -1;
        public static final int DTYPE_CODE_CHAR_STRING = 0;
        public static final int DTYPE_CODE_NCHAR_STRING = 1;
        public static final int DTYPE_CODE_BIT_STRING = 2;
        public static final int DTYPE_CODE_EXACT_NUMERIC = 3;
        public static final int DTYPE_CODE_APROX_NUMERIC = 4;
        public static final int DTYPE_CODE_DATE_TIME = 5;
        public static final int DTYPE_CODE_MISC = 6;
        public static final int DTYPE_CODE_CUSTOM = 7;

        // CHAR
        public static final String[] DTYPE_CHARACTER = {"CHARACTER"};
        public static final String[] DTYPE_CHAR = {"CHAR"};
        public static final String[] DTYPE_CHARACTER_VARYING = {"CHARACTER", "VARYING"};
        public static final String[] DTYPE_CHAR_VARYING = {"CHAR", "VARYING"};
        public static final String[] DTYPE_VARCHAR = {"VARCHAR"};
        // NATIONAL CHAR
        public static final String[] DTYPE_NATIONAL_CHARACTER = {"NATIONAL", "CHARACTER"};
        public static final String[] DTYPE_NATIONAL_CHAR = {"NATIONAL", "CHAR"};
        public static final String[] DTYPE_NATIONAL_CHARACTER_VARYING = {"NATIONAL", "CHARACTER", "VARYING"};
        public static final String[] DTYPE_NATIONAL_CHAR_VARYING = {"NATIONAL", "CHAR", "VARYING"};
        public static final String[] DTYPE_NCHAR_VARYING = {"NCHAR", "VARYING"};
        public static final String[] DTYPE_NCHAR = {"NCHAR"};
        // BIT STRING
        public static final String[] DTYPE_BIT = {"BIT"};
        public static final String[] DTYPE_BIT_VARYING = {"BIT", "VARYING"};
        // EXACT NUMERIC
        public static final String[] DTYPE_NUMERIC = {"NUMERIC"};
        public static final String[] DTYPE_DEC = {"DEC"};
        public static final String[] DTYPE_DECIMAL = {"DECIMAL"};
        public static final String[] DTYPE_INTEGER = {"INTEGER"};
        public static final String[] DTYPE_INT = {"INT"};
        public static final String[] DTYPE_SMALLINT = {"SMALLINT"};
        // APPROXIMATE NUMERIC
        public static final String[] DTYPE_FLOAT = {"FLOAT"};
        public static final String[] DTYPE_REAL = {"REAL"};
        public static final String[] DTYPE_DOUBLE_PRECISION = {"DOUBLE", "PRECISION"};
        // DATE-TIME
        public static final String[] DTYPE_DATE = {"DATE"};
        public static final String[] DTYPE_TIME = {"TIME"};
        public static final String[] DTYPE_TIMESTAMP = {"TIMESTAMP"};
        // INTERVAL
        public static final String[] DTYPE_INTERVAL = {"INTERVAL"};

        public final static List<String> DATATYPE_START_WORDS = Arrays.asList(new String[] {"CHAR", "CHARACTER", "VARCHAR",
            "NATIONAL", "NCHAR", "BIT", "NUMERIC", "DEC", "DECIMAL", "INT", "INTEGER", "SMALLINT", "FLOAT", "REAL", "DOUBLE",
            "DATE", "TIME", "TIMESTAMP", "INTERVAL"});
    }

    @SuppressWarnings( "nls" )
    public final static String[] SQL_92_RESERVED_WORDS = {"ABSOLUTE", "ACTION", "ADD", "ALL", "ALLOCATE", ALTER, "AND", "ANY",
        "ARE", "AS", "ASC", "ASSERTION", "AT", "AUTHORIZATION", "AVG", "BEGIN", "BETWEEN", "BIT", "BIT_LENGTH", "BOTH", "BY",
        "CASCADE", "CASCADED", "CASE", "CAST", "CATALOG", "CHAR", "CHARACTER", "CHAR_LENGTH", "CHARACTER_LENGTH", CHECK, "CLOSE",
        "COALESCE", "COLLATE", "COLLATION", COLUMN, "COMMIT", "CONNECT", "CONNECTION", CONSTRAINT, "CONSTRAINTS", "CONTINUE",
        "CONVERT", "CORRESPONDING", "COUNT", CREATE, "CROSS", "CURRENT", "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP",
        "CURRENT_USER", "CURSOR", "DATE", "DAY", "DEALLOCATE", "DEC", "DECIMAL", DECLARE, "DEFAULT", "DEFERRABLE", "DEFERRED",
        "DELETE", "DESC", "DESCRIBE", "DESCRIPTOR", "DIAGNOSTICS", "DISCONNECT", "DISTINCT", "DOMAIN", "DOUBLE", DROP, "ELSE",
        "END", "END_EXEC", "ESCAPE", "EXCEPT", "EXCEPTION", "EXEC", "EXECUTE", "EXISTS", "EXTERNAL", "EXTRACT", "FALSE", "FETCH",
        "FIRST", "FLOAT", "FOR", FOREIGN, "FOUND", "FROM", "FULL", "GET", "GLOBAL", "GO", "GOTO", GRANT, "GROUP", "HAVING",
        "HOUR", "IDENTITY", "IMMEDIATE", "IN", "INDICATOR", "INITIALLY", "INNER", "INPUT", "INSENSITIVE", INSERT, "INT",
        "INTEGER", "INTERSECT", "INTERVAL", "INTO", "IS", "ISOLATION", "JOIN", KEY, "LANGUAGE", "LAST", "LEADING", "LEFT",
        "LEVEL", "LIKE", "LOCAL", "LOWER", "MATCH", "MAX", "MIN", "MINUTE", "MODULE", "MONTH", "NAMES", "NATIONAL", "NATURAL",
        "NCHAR", "NEXT", "NO", "NOT", "NULL", "NULLIF", "NUMERIC", "OCTET_LENGTH", "OF", ON, "ONLY", "OPEN", "OPTION", "OR",
        "ORDER", "OUTER", "OUTPUT", "OVERLAPS", "PAD", "PARTIAL", "POSITION", "PRECISION", "PREPARE", "PRESERVE", PRIMARY,
        "PRIOR", "PRIVILEGES", "PROCEDURE", "PUBLIC", "READ", "REAL", "REFERENCES", "RELATIVE", "RESTRICT", "REVOKE", "RIGHT",
        "ROLLBACK", "ROWS", SCHEMA, "SCROLL", "SECOND", "SECTION", "SELECT", "SESSION", "SESSION_USER", SET, "SIZE", "SMALLINT",
        "SOME", "SPACE", "SQL", "SQLCODE", "SQLERROR", "SQLSTATE", "SUBSTRING", "SUM", "SYSTEM_USER", TABLE, "TEMPORARY", "THEN",
        "TIME", "TIMESTAMP", "TIMEZONE_HOUR", "TIMEZONE_MINUTE", "TO", "TRAILING", "TRANSACTION", "TRANSLATE", "TRANSLATION",
        "TRIM", "TRUE", "UNION", UNIQUE, "UNKNOWN", "UPDATE", "UPPER", "USAGE", "USER", "USING", "VALUE", "VALUES", "VARCHAR",
        "VARYING", VIEW, "WHEN", "WHENEVER", "WHERE", "WITH", "WORK", "WRITE", "YEAR", "ZONE"};

    interface Problems {
        public static final int OK = 0;
        public static final int WARNING = 1;
        public static final int ERROR = 2;
    }

    interface AstNodeNames {
        public static final String TABLE_DEFINITION = "TABLE_DEFINITION";
        public static final String ALTER_TABLE_DEFINITION = "ALTER_TABLE_DEFINITION";
        public static final String COLUMN_DEFINITION = "COLUMN_DEFINITION";
        public static final String COLUMN_REFERENCE = "COLUMN_REFERENCE";
        public static final String TABLE_CONSTRAINT = "TABLE_CONSTRAINT";
        // public static final String TABLE_OPTION = "TABLE_OPTION";
        public static final String ADD_COLUMN_DEFINITION = "ADD_COLUMN_DEFINITION";
        public static final String ADD_TABLE_CONSTRAINT = "ADD_TABLE_CONSTRAINT";
        public static final String DROP_COLUMN_DEFINITION = "DROP_COLUMN_DEFINITION";
        public static final String DROP_TABLE_CONSTRAINT = "DROP_TABLE_CONSTRAINT";
        public static final String ALTER_COLUMN_DEFINITION = "ALTER_COLUMN_DEFINITION";

    }
}