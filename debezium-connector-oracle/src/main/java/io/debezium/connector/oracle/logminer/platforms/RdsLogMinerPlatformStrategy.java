/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.platforms;

import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.LogMinerPlatformStrategy;

/**
 * LogMiner platform strategy for AWS RDS Oracle deployments using CDB architecture.
 *
 * <p>AWS RDS Oracle CDB deployments do not permit direct access to CDB$ROOT via
 * {@code ALTER SESSION SET CONTAINER=cdb$root}. Instead, AWS provides custom PL/SQL
 * packages via {@code rdsadmin.rdsadmin_util} for LogMiner operations. This strategy
 * uses those RDS-specific packages and ensures the connector operates entirely within
 * the PDB context.
 *
 * <p>Key differences from the default strategy:
 * <ul>
 *   <li>{@code rdsadmin.rdsadmin_util.logmnr_add_logfile} replaces {@code sys.dbms_logmnr.add_logfile}</li>
 *   <li>{@code rdsadmin.rdsadmin_util.logmnr_start} replaces {@code sys.dbms_logmnr.start_logmnr}</li>
 *   <li>{@code rdsadmin.rdsadmin_util.logmnr_end} replaces {@code SYS.DBMS_LOGMNR.END_LOGMNR}</li>
 *   <li>{@code rdsadmin.rdsadmin_util.logmnr_remove_logfile} replaces {@code SYS.DBMS_LOGMNR.REMOVE_LOGFILE}</li>
 *   <li>CDB root container switching is disabled (no-op)</li>
 * </ul>
 *
 * @author Chris Cranford
 */
public class RdsLogMinerPlatformStrategy implements LogMinerPlatformStrategy {

    @Override
    public String getAddLogFileSql(String fileName) {
        return "BEGIN rdsadmin.rdsadmin_util.logmnr_add_logfile(LOGFILENAME => '" + fileName
                + "', OPTIONS => DBMS_LOGMNR.ADDFILE); END;";
    }

    @Override
    public String getStartSessionSql(Scn startScn, Scn endScn, String miningOptions, String dictionaryFilePath) {
        final var query = new StringBuilder(64);
        query.append("BEGIN rdsadmin.rdsadmin_util.logmnr_start(");
        if (!startScn.isNull()) {
            query.append("startScn => '").append(startScn).append("', ");
        }
        if (!endScn.isNull()) {
            query.append("endScn => '").append(endScn).append("', ");
        }
        query.append("options => ").append(miningOptions);
        if (dictionaryFilePath != null) {
            query.append(", DICTFILENAME => '").append(dictionaryFilePath).append("'");
        }
        query.append("); END;");
        return query.toString();
    }

    @Override
    public String getEndSessionSql() {
        return "BEGIN rdsadmin.rdsadmin_util.logmnr_end(); END;";
    }

    @Override
    public String getWriteDataDictionaryToRedoLogsSql() {
        return "BEGIN DBMS_LOGMNR_D.BUILD (options => DBMS_LOGMNR_D.STORE_IN_REDO_LOGS); END;";
    }

    @Override
    public String getRemoveLogFileSql(String fileName) {
        return "BEGIN rdsadmin.rdsadmin_util.logmnr_remove_logfile(LOGFILENAME => '" + fileName + "');END;";
    }

    @Override
    public boolean isCdbRootAccessible() {
        return false;
    }

    @Override
    public String getCurrentScnQuery() {
        return "SELECT CURRENT_SCN FROM V$DATABASE";
    }

    @Override
    public String getArchiveLogModeQuery() {
        return "SELECT LOG_MODE FROM V$DATABASE";
    }

    @Override
    public String getRedoThreadStateQuery() {
        return "SELECT * FROM V$THREAD";
    }
}
