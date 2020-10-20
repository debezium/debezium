/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.db2;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.db2.jcc.DB2Driver;

import io.debezium.config.Configuration;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.util.BoundedConcurrentHashMap;

/**
 * {@link JdbcConnection} extension to be used with IBM Db2
 *
 * @author Horia Chiorean (hchiorea@redhat.com), Jiri Pechanec, Peter Urbanetz
 *
 */
public class Db2Connection extends JdbcConnection {

    private static final String GET_DATABASE_NAME = "SELECT CURRENT_SERVER FROM SYSIBM.SYSDUMMY1"; // DB2

    private static Logger LOGGER = LoggerFactory.getLogger(Db2Connection.class);

    private static final String CDC_SCHEMA = "ASNCDC";

    private static final String STATEMENTS_PLACEHOLDER = "#";
    private static final String GET_MAX_LSN = "SELECT max(t.SYNCHPOINT) FROM ( SELECT CD_NEW_SYNCHPOINT AS SYNCHPOINT FROM " + CDC_SCHEMA
            + ".IBMSNAP_REGISTER UNION ALL SELECT SYNCHPOINT AS SYNCHPOINT FROM " + CDC_SCHEMA + ".IBMSNAP_REGISTER) t";

    private static final String LOCK_TABLE = "SELECT * FROM # WITH CS"; // DB2

    private static final String LSN_TO_TIMESTAMP = "SELECT CURRENT TIMEstamp FROM sysibm.sysdummy1  WHERE ? > X'00000000000000000000000000000000'";

    private static final String GET_ALL_CHANGES_FOR_TABLE = "SELECT "
            + "CASE "
            + "WHEN IBMSNAP_OPERATION = 'D' AND (LEAD(cdc.IBMSNAP_OPERATION,1,'X') OVER (PARTITION BY cdc.IBMSNAP_COMMITSEQ ORDER BY cdc.IBMSNAP_INTENTSEQ)) ='I' THEN 3 "
            + "WHEN IBMSNAP_OPERATION = 'I' AND (LAG(cdc.IBMSNAP_OPERATION,1,'X') OVER (PARTITION BY cdc.IBMSNAP_COMMITSEQ ORDER BY cdc.IBMSNAP_INTENTSEQ)) ='D' THEN 4 "
            + "WHEN IBMSNAP_OPERATION = 'D' THEN 1 "
            + "WHEN IBMSNAP_OPERATION = 'I' THEN 2 "
            + "END "
            + "OPCODE,"
            + "cdc.* "
            + "FROM ASNCDC.# cdc WHERE   IBMSNAP_COMMITSEQ >= ? AND IBMSNAP_COMMITSEQ <= ? "
            + "order by IBMSNAP_COMMITSEQ, IBMSNAP_INTENTSEQ";

    private static final String GET_LIST_OF_CDC_ENABLED_TABLES = "select r.SOURCE_OWNER, r.SOURCE_TABLE, r.CD_OWNER, r.CD_TABLE, r.CD_NEW_SYNCHPOINT, r.CD_OLD_SYNCHPOINT, t.TBSPACEID, t.TABLEID , CAST((t.TBSPACEID * 65536 +  t.TABLEID )AS INTEGER )from "
            + CDC_SCHEMA + ".IBMSNAP_REGISTER r left JOIN SYSCAT.TABLES t ON r.SOURCE_OWNER  = t.TABSCHEMA AND r.SOURCE_TABLE = t.TABNAME  WHERE r.SOURCE_OWNER <> ''";

    // No new Tabels 1=0
    private static final String GET_LIST_OF_NEW_CDC_ENABLED_TABLES = "select CAST((t.TBSPACEID * 65536 +  t.TABLEID )AS INTEGER ) AS OBJECTID, " +
            "       CD_OWNER CONCAT '.' CONCAT CD_TABLE, " +
            "       CD_NEW_SYNCHPOINT, " +
            "       CD_OLD_SYNCHPOINT " +
            "from ASNCDC.IBMSNAP_REGISTER  r left JOIN SYSCAT.TABLES t ON r.SOURCE_OWNER  = t.TABSCHEMA AND r.SOURCE_TABLE = t.TABNAME " +
            "WHERE r.SOURCE_OWNER <> '' AND 1=0 AND CD_NEW_SYNCHPOINT > ? AND CD_OLD_SYNCHPOINT < ? ";

    private static final String GET_LIST_OF_KEY_COLUMNS = "SELECT "
            + "CAST((t.TBSPACEID * 65536 +  t.TABLEID )AS INTEGER ) as objectid, "
            + "c.colname,c.colno,c.keyseq "
            + "FROM syscat.tables  as t "
            + "inner join syscat.columns as c  on t.tabname = c.tabname and t.tabschema = c.tabschema and c.KEYSEQ > 0 AND "
            + "t.tbspaceid = CAST(BITAND( ? , 4294901760) / 65536 AS SMALLINT) AND t.tableid=  CAST(BITAND( ? , 65535) AS SMALLINT)";

    private static final int CHANGE_TABLE_DATA_COLUMN_OFFSET = 4;

    private static final String URL_PATTERN = "jdbc:db2://${" + JdbcConfiguration.HOSTNAME + "}:${" + JdbcConfiguration.PORT + "}/${" + JdbcConfiguration.DATABASE + "}";

    private static final ConnectionFactory FACTORY = JdbcConnection.patternBasedFactory(URL_PATTERN,
            DB2Driver.class.getName(),
            Db2Connection.class.getClassLoader(),
            JdbcConfiguration.PORT.withDefault(Db2ConnectorConfig.PORT.defaultValueAsString()));

    /**
     * actual name of the database, which could differ in casing from the database name given in the connector config.
     */
    private final String realDatabaseName;

    private static interface ResultSetExtractor<T> {
        T apply(ResultSet rs) throws SQLException;
    }

    private final BoundedConcurrentHashMap<Lsn, Instant> lsnToInstantCache;

    /**
     * Creates a new connection using the supplied configuration.
     *
     * @param config {@link Configuration} instance, may not be null.
     */
    public Db2Connection(Configuration config) {
        super(config, FACTORY);
        lsnToInstantCache = new BoundedConcurrentHashMap<>(100);
        realDatabaseName = retrieveRealDatabaseName();
    }

    /**
     * @return the current largest log sequence number
     */
    public Lsn getMaxLsn() throws SQLException {
        return queryAndMap(GET_MAX_LSN, singleResultMapper(rs -> {
            final Lsn ret = Lsn.valueOf(rs.getBytes(1));
            LOGGER.trace("Current maximum lsn is {}", ret);
            return ret;
        }, "Maximum LSN query must return exactly one value"));
    }

    /**
     * Provides all changes recorded by the DB2 CDC capture process for a given table.
     *
     * @param tableId  - the requested table changes
     * @param fromLsn  - closed lower bound of interval of changes to be provided
     * @param toLsn    - closed upper bound of interval  of changes to be provided
     * @param consumer - the change processor
     * @throws SQLException
     */
    public void getChangesForTable(TableId tableId, Lsn fromLsn, Lsn toLsn, ResultSetConsumer consumer) throws SQLException {
        final String query = GET_ALL_CHANGES_FOR_TABLE.replace(STATEMENTS_PLACEHOLDER, cdcNameForTable(tableId));
        prepareQuery(query, statement -> {
            statement.setBytes(1, fromLsn.getBinary());
            statement.setBytes(2, toLsn.getBinary());

        }, consumer);
    }

    /**
     * Provides all changes recorder by the DB2 CDC capture process for a set of tables.
     *
     * @param changeTables    - the requested tables to obtain changes for
     * @param intervalFromLsn - closed lower bound of interval of changes to be provided
     * @param intervalToLsn   - closed upper bound of interval  of changes to be provided
     * @param consumer        - the change processor
     * @throws SQLException
     */
    public void getChangesForTables(Db2ChangeTable[] changeTables, Lsn intervalFromLsn, Lsn intervalToLsn, BlockingMultiResultSetConsumer consumer)
            throws SQLException, InterruptedException {
        final String[] queries = new String[changeTables.length];
        final StatementPreparer[] preparers = new StatementPreparer[changeTables.length];

        int idx = 0;
        for (Db2ChangeTable changeTable : changeTables) {
            final String query = GET_ALL_CHANGES_FOR_TABLE.replace(STATEMENTS_PLACEHOLDER, changeTable.getCaptureInstance());
            queries[idx] = query;
            // If the table was added in the middle of queried buffer we need
            // to adjust from to the first LSN available
            LOGGER.trace("Getting changes for table {} in range[{}, {}]", changeTable, intervalFromLsn, intervalToLsn);
            preparers[idx] = statement -> {
                statement.setBytes(1, intervalFromLsn.getBinary());
                statement.setBytes(2, intervalToLsn.getBinary());

            };

            idx++;
        }
        prepareQuery(queries, preparers, consumer);
    }

    /**
     * Obtain the next available position in the database log.
     *
     * @param lsn - LSN of the current position
     * @return LSN of the next position in the database
     * @throws SQLException
     */
    public Lsn incrementLsn(Lsn lsn) throws SQLException {
        return lsn.increment();
    }

    /**
     * Map a commit LSN to a point in time when the commit happened.
     *
     * @param lsn - LSN of the commit
     * @return time when the commit was recorded into the database log
     * @throws SQLException
     */
    public Instant timestampOfLsn(Lsn lsn) throws SQLException {
        final String query = LSN_TO_TIMESTAMP;

        if (lsn.getBinary() == null) {
            return null;
        }

        Instant cachedInstant = lsnToInstantCache.get(lsn);
        if (cachedInstant != null) {
            return cachedInstant;
        }

        return prepareQueryAndMap(query, statement -> {
            statement.setBytes(1, lsn.getBinary());
        }, singleResultMapper(rs -> {
            final Timestamp ts = rs.getTimestamp(1);
            final Instant ret = (ts == null) ? null : ts.toInstant();
            LOGGER.trace("Timestamp of lsn {} is {}", lsn, ret);
            if (ret != null) {
                lsnToInstantCache.put(lsn, ret);
            }
            return ret;
        }, "LSN to timestamp query must return exactly one value"));
    }

    /**
     * Creates an exclusive lock for a given table.
     *
     * @param tableId to be locked
     * @throws SQLException
     */
    public void lockTable(TableId tableId) throws SQLException {
        final String lockTableStmt = LOCK_TABLE.replace(STATEMENTS_PLACEHOLDER, tableId.table());
        execute(lockTableStmt);
    }

    private String cdcNameForTable(TableId tableId) {
        return tableId.schema() + '_' + tableId.table();
    }

    public static class CdcEnabledTable {
        private final String tableId;
        private final String captureName;
        private final Lsn fromLsn;

        private CdcEnabledTable(String tableId, String captureName, Lsn fromLsn) {
            this.tableId = tableId;
            this.captureName = captureName;
            this.fromLsn = fromLsn;
        }

        public String getTableId() {
            return tableId;
        }

        public String getCaptureName() {
            return captureName;
        }

        public Lsn getFromLsn() {
            return fromLsn;
        }
    }

    public Set<Db2ChangeTable> listOfChangeTables() throws SQLException {
        final String query = GET_LIST_OF_CDC_ENABLED_TABLES;

        return queryAndMap(query, rs -> {
            final Set<Db2ChangeTable> changeTables = new HashSet<>();
            while (rs.next()) {
                /**
                 changeTables.add(
                 new ChangeTable(
                 new TableId(realDatabaseName, rs.getString(1), rs.getString(2)),
                 rs.getString(3),
                 rs.getInt(4),
                 Lsn.valueOf(rs.getBytes(6)),
                 Lsn.valueOf(rs.getBytes(7))
                
                 )
                 **/
                changeTables.add(
                        new Db2ChangeTable(
                                new TableId("", rs.getString(1), rs.getString(2)),
                                rs.getString(4),
                                rs.getInt(9),
                                Lsn.valueOf(rs.getBytes(5)),
                                Lsn.valueOf(rs.getBytes(6))

                        ));
            }
            return changeTables;
        });
    }

    public Set<Db2ChangeTable> listOfNewChangeTables(Lsn fromLsn, Lsn toLsn) throws SQLException {
        final String query = GET_LIST_OF_NEW_CDC_ENABLED_TABLES;

        return prepareQueryAndMap(query,
                ps -> {
                    ps.setBytes(1, fromLsn.getBinary());
                    ps.setBytes(2, toLsn.getBinary());
                },
                rs -> {
                    final Set<Db2ChangeTable> changeTables = new HashSet<>();
                    while (rs.next()) {
                        changeTables.add(new Db2ChangeTable(
                                rs.getString(2),
                                rs.getInt(1),
                                Lsn.valueOf(rs.getBytes(3)),
                                Lsn.valueOf(rs.getBytes(4))));
                    }
                    return changeTables;
                });
    }

    public Table getTableSchemaFromTable(Db2ChangeTable changeTable) throws SQLException {
        final DatabaseMetaData metadata = connection().getMetaData();

        List<Column> columns = new ArrayList<>();
        try (ResultSet rs = metadata.getColumns(
                null,
                changeTable.getSourceTableId().schema(),
                changeTable.getSourceTableId().table(),
                null)) {
            while (rs.next()) {
                readTableColumn(rs, changeTable.getSourceTableId(), null).ifPresent(ce -> columns.add(ce.create()));
            }
        }

        final List<String> pkColumnNames = readPrimaryKeyNames(metadata, changeTable.getSourceTableId());
        Collections.sort(columns);
        return Table.editor()
                .tableId(changeTable.getSourceTableId())
                .addColumns(columns)
                .setPrimaryKeyNames(pkColumnNames)
                .create();
    }

    public Table getTableSchemaFromChangeTable(Db2ChangeTable changeTable) throws SQLException {
        final DatabaseMetaData metadata = connection().getMetaData();
        final TableId changeTableId = changeTable.getChangeTableId();

        List<ColumnEditor> columnEditors = new ArrayList<>();
        try (ResultSet rs = metadata.getColumns(null, changeTableId.schema(), changeTableId.table(), null)) {
            while (rs.next()) {
                readTableColumn(rs, changeTableId, null).ifPresent(columnEditors::add);
            }
        }

        // The first 5 columns and the last column of the change table are CDC metadata
        // final List<Column> columns = columnEditors.subList(CHANGE_TABLE_DATA_COLUMN_OFFSET, columnEditors.size() - 1).stream()
        final List<Column> columns = columnEditors.subList(CHANGE_TABLE_DATA_COLUMN_OFFSET, columnEditors.size()).stream()
                .map(c -> c.position(c.position() - CHANGE_TABLE_DATA_COLUMN_OFFSET).create())
                .collect(Collectors.toList());

        final List<String> pkColumnNames = new ArrayList<>();
        /**  URB
         prepareQuery(GET_LIST_OF_KEY_COLUMNS, ps -> ps.setInt(1, changeTable.getChangeTableObjectId()), rs -> {
         while (rs.next()) {
         pkColumnNames.add(rs.getString(2));
         }
         });
         **/
        prepareQuery(GET_LIST_OF_KEY_COLUMNS, ps -> {
            ps.setInt(1, changeTable.getChangeTableObjectId());
            ps.setInt(1, changeTable.getChangeTableObjectId());
        }, rs -> {
            while (rs.next()) {
                pkColumnNames.add(rs.getString(2));
            }
        });
        Collections.sort(columns);
        return Table.editor()
                .tableId(changeTable.getSourceTableId())
                .addColumns(columns)
                .setPrimaryKeyNames(pkColumnNames)
                .create();
    }

    public String getNameOfChangeTable(String captureName) {
        return captureName + "_CT";
    }

    public String getRealDatabaseName() {
        return realDatabaseName;
    }

    private String retrieveRealDatabaseName() {
        try {
            return queryAndMap(
                    GET_DATABASE_NAME,
                    singleResultMapper(rs -> rs.getString(1), "Could not retrieve database name"));
        }
        catch (SQLException e) {
            throw new RuntimeException("Couldn't obtain database name", e);
        }
    }
}
