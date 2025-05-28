/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb;

import io.debezium.connector.binlog.InnoDBMvccSnapshot;

public class MariaDbInnoDbMvccSnapshotIT extends InnoDBMvccSnapshot<MariaDbConnector> implements MariaDbCommon {

    @Override
    protected String snapshotMode() {
        return "single_transaction";
    }

}
