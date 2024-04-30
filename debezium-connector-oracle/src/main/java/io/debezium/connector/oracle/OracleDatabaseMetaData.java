/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import oracle.jdbc.OracleCallableStatement;
import oracle.jdbc.driver.OracleConnection;

/**
 * Copy from oracle.jdbc.driver.OracleDatabaseMetaData
 * @author butioy
 */
public class OracleDatabaseMetaData {

    private static final String COMPLIANT_KEY = "oracle.jdbc.J2EE13Compliant";

    protected static final Pattern SQL_WILDCARD_PATTERN = Pattern.compile("^%|^_|[^/]%|[^/]_");

    protected static final Pattern SQL_ESCAPE_PATTERN = Pattern.compile("/");

    private final OracleConnection connection;

    private final boolean remarksReporting;

    private final boolean mapDateToTimestamp;

    private final boolean includeSynonyms;

    private final boolean j2ee13Compliant;

    private final Lock lock = new ReentrantLock();

    public OracleDatabaseMetaData(OracleConnection connection) {
        this.connection = connection;
        this.remarksReporting = connection.getRemarksReporting();
        this.mapDateToTimestamp = connection.getMapDateToTimestamp();
        this.includeSynonyms = connection.getIncludeSynonyms();
        this.j2ee13Compliant = setJ2EE13Compliant(connection.getProperties());
    }

    private boolean setJ2EE13Compliant(Properties prop) {
        String j2ee13Compliant = null;
        if (prop != null) {
            j2ee13Compliant = prop.getProperty(COMPLIANT_KEY);
        }
        if (j2ee13Compliant == null) {
            j2ee13Compliant = AccessController.doPrivileged((PrivilegedAction<String>) () -> System.getProperty(COMPLIANT_KEY, null));
        }
        if (j2ee13Compliant == null) {
            j2ee13Compliant = "false";
        }
        return j2ee13Compliant.equalsIgnoreCase("true");
    }

    /**
     * Copy from {@code oracle.jdbc.driver.OracleDatabaseMetaData#getColumns}
     */
    public ResultSet getColumns(String databaseCatalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        lock.lock();
        Throwable throwable = null;
        ResultSet resultSet;
        try {
            String querySql;
            short versionNumber = this.connection.getVersionNumber();
            if (this.includeSynonyms && schemaPattern != null && !this.hasSqlWildcard(schemaPattern) && tableNamePattern != null
                    && !this.hasSqlWildcard(tableNamePattern)) {
                querySql = this.getColumnsNoWildcardsPlsql(versionNumber);
                schemaPattern = this.stripSqlEscapes(schemaPattern);
                tableNamePattern = this.stripSqlEscapes(tableNamePattern);
            }
            else {
                querySql = this.getColumnsWithWildcardsPlsql(versionNumber);
                schemaPattern = null == schemaPattern ? "%" : schemaPattern;
                tableNamePattern = null == tableNamePattern ? "%" : tableNamePattern;
            }
            columnNamePattern = null == columnNamePattern ? "%" : columnNamePattern;

            Properties queryProp = new Properties();
            queryProp.setProperty("use_long_fetch", "true");
            CallableStatement stat = this.connection.prepareCall(querySql, queryProp);
            stat.setString(1, schemaPattern);
            stat.setString(2, tableNamePattern);
            stat.setString(3, columnNamePattern);
            stat.registerOutParameter(4, -10);
            stat.setPoolable(false);
            stat.closeOnCompletion();
            stat.execute();
            resultSet = ((OracleCallableStatement) stat).getCursor(4);
            if (resultSet.getFetchSize() < 4284) {
                resultSet.setFetchSize(4284);
            }
        }
        catch (Throwable e) {
            throwable = e;
            throw e;
        }
        finally {
            try {
                lock.unlock();
            }
            catch (Throwable e) {
                if (null != throwable) {
                    throwable.addSuppressed(e);
                }
            }
        }

        return resultSet;
    }

    /**
     * Copy from {@code oracle.jdbc.driver.OracleDatabaseMetaData#getColumnsNoWildcardsPlsql}
     */
    String getColumnsNoWildcardsPlsql(short versionNumber) {
        String var1 = "declare in_owner varchar2(256) := null; in_name varchar2(256) := null; my_user_name varchar2(256) := null; cnt number := 0; out_owner varchar2(256) := null; out_name varchar2(256):= null; xxx SYS_REFCURSOR; begin in_owner := ?; in_name := ?; select user into my_user_name from dual; if (my_user_name = in_owner) then select count(*) into cnt from user_tables where table_name = in_name; if (cnt = 1) then out_owner := in_owner; out_name := in_name; else select count(*) into cnt from user_views where view_name = in_name; if (cnt = 1) then out_owner := in_owner; out_name := in_name; else begin select table_owner, table_name into out_owner, out_name from all_synonyms where CONNECT_BY_ISLEAF = 1 and db_link is NULL start with owner = in_owner and synonym_name = in_name connect by prior table_name = synonym_name and prior table_owner = owner; exception when NO_DATA_FOUND then out_owner := null; out_name := null; end; end if; end if; else select count(*) into cnt from all_tables where owner = in_owner and table_name = in_name; if (cnt = 1) then out_owner := in_owner; out_name := in_name; else select count(*) into cnt from all_views where owner = in_owner and view_name = in_name; if (cnt = 1) then out_owner := in_owner; out_name := in_name; else begin select table_owner, table_name into out_owner, out_name from all_synonyms where CONNECT_BY_ISLEAF = 1 and db_link is NULL start with owner = in_owner and synonym_name = in_name connect by prior table_name = synonym_name and prior table_owner = owner; exception when NO_DATA_FOUND then out_owner := null; out_name := null; end; end if; end if; end if; ";
        String var3 = "open xxx for SELECT NULL AS table_cat, ";
        String var4 = " in_owner AS table_schem, in_name AS table_name, ";
        String var5 = " DECODE (t.data_type, 'CHAR', t.char_length, 'VARCHAR', t.char_length, 'VARCHAR2', t.char_length, 'NVARCHAR2', t.char_length, 'NCHAR', t.char_length, 'NUMBER', 0, t.data_length)";
        String var6 = " t.column_name AS column_name, " + this.datatypeQuery()
                + " t.data_type AS type_name, DECODE (t.data_precision, null, DECODE(t.data_type, 'NUMBER', DECODE(t.data_scale, null, "
                + (this.j2ee13Compliant ? "38" : "0") + ", 38), " + var5
                + "), t.data_precision) AS column_size, 0 AS buffer_length, DECODE (t.data_type, 'NUMBER', DECODE(t.data_precision, null, DECODE(t.data_scale, null, "
                + (this.j2ee13Compliant ? "0" : "-127")
                + ", t.data_scale), t.data_scale), t.data_scale) AS decimal_digits, 10 AS num_prec_radix, DECODE (t.nullable, 'N', 0, 1) AS nullable, ";
        String var7 = " c.comments AS remarks, ";
        String var8 = " NULL AS remarks,";
        String var9 = " t.data_default AS column_def, 0 AS sql_data_type, 0 AS sql_datetime_sub, t.data_length AS char_octet_length, t.column_id AS ordinal_position, DECODE (t.nullable, 'N', 'NO', 'YES') AS is_nullable, ";
        String var10 = " null as SCOPE_CATALOG, null as SCOPE_SCHEMA, null as SCOPE_TABLE, null as SOURCE_DATA_TYPE, 'NO' as IS_AUTOINCREMENT,\n"
                + (versionNumber >= 12000 ? " t.virtual_column as IS_GENERATEDCOLUMN," : " null as IS_GENERATEDCOLUMN,") + "t.segment_column_id as segment_position ";
        // String var11 = versionNumber >= 12000 ? "FROM all_tab_cols t" : "FROM all_tab_columns t";
        // This view already exists in oracle 11g
        String var11 = " FROM all_tab_cols t ";
        String var12 = ", all_col_comments c ";
        String var13 = " WHERE t.owner = out_owner AND t.table_name = out_name AND t.column_name LIKE ? ESCAPE '/' "
                + (versionNumber >= 12000 ? " AND t.user_generated = 'YES' " : "");
        String var14 = " AND t.owner = c.owner (+) AND t.table_name = c.table_name (+) AND t.column_name = c.column_name (+) ";
        String var15 = " ORDER BY table_schem, table_name, segment_position ";
        String var16 = var3 + var4;
        var16 = var16 + var6;
        if (this.remarksReporting) {
            var16 = var16 + var7;
        }
        else {
            var16 = var16 + var8;
        }

        var16 = var16 + var9 + var10 + var11;
        if (this.remarksReporting) {
            var16 = var16 + var12;
        }

        var16 = var16 + "\n" + var13;
        if (this.remarksReporting) {
            var16 = var16 + var14;
        }

        var16 = var16 + "\n" + var15;
        String var17 = "; \n ? := xxx;\n end;";
        String var18 = var1 + var16 + var17;
        return var18;
    }

    /**
     * copy {@link oracle.jdbc.driver.OracleDatabaseMetaData#getColumnsWithWildcardsPlsql}
     */
    String getColumnsWithWildcardsPlsql(short versionNumber) {
        String var3 = "declare in_owner varchar2(256) := null; in_name varchar2(256) := null; in_column varchar2(256) := null; xyzzy SYS_REFCURSOR; begin in_owner := ?; in_name := ?; in_column := ?; ";
        String var4 = " UNION ALL ";
        String var5 = " SELECT ";
        String var6 = " NULL AS table_cat, ";
        String var7 = "";
        if (versionNumber >= 10200 & versionNumber < 11100 & this.includeSynonyms) {
            var7 = "/*+ CHOOSE */";
        }

        String var8 = " t.owner AS table_schem, t.table_name AS table_name, ";
        String var9 = " REGEXP_SUBSTR(LTRIM(s.owner, '/'), '[^/]+') AS table_schem, REGEXP_SUBSTR(LTRIM(s.synonym_name, '/'), '[^/]+') AS table_name, ";
        String var10 = " DECODE (t.data_type, 'CHAR', t.char_length, 'VARCHAR', t.char_length, 'VARCHAR2', t.char_length, 'NVARCHAR2', t.char_length, 'NCHAR', t.char_length, 'NUMBER', 0, t.data_length) ";
        String var11 = " t.column_name AS column_name, " + this.datatypeQuery()
                + " t.data_type AS type_name, DECODE (t.data_precision, null, DECODE(t.data_type, 'NUMBER', DECODE(t.data_scale, null, "
                + (this.j2ee13Compliant ? "38" : "0") + ", 38), " + var10
                + "), t.data_precision) AS column_size, 0 AS buffer_length, DECODE (t.data_type, 'NUMBER', DECODE(t.data_precision, null, DECODE(t.data_scale, null, "
                + (this.j2ee13Compliant ? "0" : "-127")
                + ", t.data_scale), t.data_scale), t.data_scale) AS decimal_digits, 10 AS num_prec_radix, DECODE (t.nullable, 'N', 0, 1) AS nullable, ";
        String var12 = " c.comments AS remarks, ";
        String var13 = " NULL AS remarks, ";
        String var14 = " t.data_default AS column_def, 0 AS sql_data_type, 0 AS sql_datetime_sub, t.data_length AS char_octet_length, t.column_id AS ordinal_position, DECODE (t.nullable, 'N', 'NO', 'YES') AS is_nullable, ";
        String var15 = " null as SCOPE_CATALOG, null as SCOPE_SCHEMA, null as SCOPE_TABLE, null as SOURCE_DATA_TYPE, 'NO' as IS_AUTOINCREMENT, "
                + (versionNumber >= 12000 ? " t.virtual_column as IS_GENERATEDCOLUMN, " : " null as IS_GENERATEDCOLUMN, ") + "t.segment_column_id as segment_position ";
        // String var16 = versionNumber >= 12000 ? "FROM all_tab_cols t" : "FROM all_tab_columns t";
        // This view already exists in oracle 11g
        String var16 = " FROM all_tab_cols t ";
        String var17 = ", (SELECT SYS_CONNECT_BY_PATH(owner, '/') owner, SYS_CONNECT_BY_PATH(synonym_name, '/') synonym_name, table_owner, table_name FROM all_synonyms WHERE CONNECT_BY_ISLEAF = 1 AND db_link is NULL START WITH owner LIKE in_owner ESCAPE '/' AND synonym_name LIKE in_name ESCAPE '/' CONNECT BY PRIOR table_name = synonym_name AND PRIOR table_owner = owner) s ";
        String var18 = ", all_col_comments c ";
        String var19 = " WHERE t.owner LIKE in_owner ESCAPE '/' AND t.table_name LIKE in_name ESCAPE '/' AND t.column_name LIKE in_column ESCAPE '/' "
                + (versionNumber >= 12000 ? "  AND t.user_generated = 'YES' " : " ");
        String var20 = " WHERE t.owner = s.table_owner AND t.table_name = s.table_name AND t.column_name LIKE in_column ESCAPE '/' "
                + (versionNumber >= 12000 ? "  AND t.user_generated = 'YES' " : " ");
        String var21 = "  AND t.owner = c.owner (+) AND t.table_name = c.table_name (+) AND t.column_name = c.column_name (+) ";
        String var22 = "ORDER BY table_schem, table_name, segment_position ";
        String var23 = "open xyzzy for ";
        String var24 = var23 + var5 + var7 + var6 + var8;
        var24 = var24 + var11;
        if (this.remarksReporting) {
            var24 = var24 + var12;
        }
        else {
            var24 = var24 + var13;
        }

        var24 = var24 + var14 + var15 + var16;
        if (this.remarksReporting) {
            var24 = var24 + var18;
        }

        var24 = var24 + "\n" + var19;
        if (this.remarksReporting) {
            var24 = var24 + var21;
        }

        if (this.includeSynonyms) {
            var24 = var24 + var4 + var5 + var7 + var6;
            var24 = var24 + var9;
            var24 = var24 + var11;
            if (this.remarksReporting) {
                var24 = var24 + var12;
            }
            else {
                var24 = var24 + var13;
            }

            var24 = var24 + var14 + var15 + var16;
            var24 = var24 + var17;
            if (this.remarksReporting) {
                var24 = var24 + var18;
            }

            var24 = var24 + "\n" + var20;
            if (this.remarksReporting) {
                var24 = var24 + var21;
            }
        }

        var24 = var24 + var22;
        String var25 = "; \n ? := xyzzy;\n end;";
        String var26 = var3 + var24 + var25;
        return var26;
    }

    /**
     * copy {@link oracle.jdbc.driver.OracleDatabaseMetaData#datatypeQuery}
     */
    protected String datatypeQuery() {
        return " DECODE(substr(t.data_type, 1, 9), 'TIMESTAMP', DECODE(substr(t.data_type, 10, 1), '(', DECODE(substr(t.data_type, 19, 5), 'LOCAL', -102, 'TIME ', -101, 93), DECODE(substr(t.data_type, 16, 5), 'LOCAL', -102, 'TIME ', -101, 93)), 'INTERVAL ', DECODE(substr(t.data_type, 10, 3), 'DAY', -104, 'YEA', -103), DECODE(t.data_type, 'BINARY_DOUBLE', 101, 'BINARY_FLOAT', 100, 'BFILE', -13, 'BLOB', 2004, 'CHAR', 1, 'CLOB', 2005, 'COLLECTION', 2003, 'DATE', "
                + (this.mapDateToTimestamp ? "93" : "91")
                + ", 'FLOAT', 6, 'LONG', -1, 'LONG RAW', -4, 'NCHAR', -15, 'NCLOB', 2011, 'NUMBER', 2, 'NVARCHAR', -9, 'NVARCHAR2', -9, 'OBJECT', 2002, 'OPAQUE/XMLTYPE', 2009, 'RAW', -3, 'REF', 2006, 'ROWID', -8, 'SQLXML', 2009, 'UROWID', -8, 'VARCHAR2', 12, 'VARRAY', 2003, 'XMLTYPE', 2009, DECODE((SELECT a.typecode FROM ALL_TYPES a WHERE a.type_name = t.data_type AND ((a.owner IS NULL AND t.data_type_owner IS NULL) OR (a.owner = t.data_type_owner))), 'OBJECT', 2002, 'COLLECTION', 2003, 1111))) AS data_type, ";
    }

    /**
     * copy {@link oracle.jdbc.driver.OracleDatabaseMetaData#hasSqlWildcard}
     */
    protected boolean hasSqlWildcard(String text) {
        return SQL_WILDCARD_PATTERN.matcher(text).find();
    }

    /**
     * copy {@link oracle.jdbc.driver.OracleDatabaseMetaData#stripSqlEscapes}
     */
    protected String stripSqlEscapes(String text) {
        Matcher matcher = SQL_ESCAPE_PATTERN.matcher(text);
        StringBuilder sb = new StringBuilder();

        while (matcher.find()) {
            matcher.appendReplacement(sb, "");
        }

        matcher.appendTail(sb);
        return sb.toString();
    }

}
