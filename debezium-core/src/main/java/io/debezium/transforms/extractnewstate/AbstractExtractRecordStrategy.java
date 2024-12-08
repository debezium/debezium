/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms.extractnewstate;

import static io.debezium.transforms.ExtractNewRecordStateConfigDefinition.DELETED_FIELD;

import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.ExtractField;
import org.apache.kafka.connect.transforms.InsertField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.transforms.ConnectRecordUtil;

/**
 * An abstract implementation of {@link ExtractRecordStrategy}.
 *
 * @author Harvey Yue
 */
public abstract class AbstractExtractRecordStrategy<R extends ConnectRecord<R>> implements ExtractRecordStrategy<R> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractExtractRecordStrategy.class);
    protected ExtractField<R> afterDelegate;
    protected ExtractField<R> beforeDelegate;
    protected InsertField<R> removedDelegate;
    protected InsertField<R> updatedDelegate;
    // for mongodb
    protected ExtractField<R> updateDescriptionDelegate;

    public AbstractExtractRecordStrategy(boolean replaceNullWithDefault) {
        afterDelegate = ConnectRecordUtil.extractAfterDelegate();
        beforeDelegate = ConnectRecordUtil.extractBeforeDelegate();
        removedDelegate = ConnectRecordUtil.insertStaticValueDelegate(DELETED_FIELD, "true", replaceNullWithDefault);
        updatedDelegate = ConnectRecordUtil.insertStaticValueDelegate(DELETED_FIELD, "false", replaceNullWithDefault);
        updateDescriptionDelegate = ConnectRecordUtil.extractUpdateDescriptionDelegate();
    }

    @Override
    public R handleTruncateRecord(R record) {
        // will drop truncate event as default behavior
        LOGGER.trace("Truncate event arrived and requested to be dropped");
        return null;
    }

    @Override
    public ExtractField<R> afterDelegate() {
        return afterDelegate;
    }

    @Override
    public ExtractField<R> beforeDelegate() {
        return beforeDelegate;
    }

    @Override
    public ExtractField<R> updateDescriptionDelegate() {
        return updateDescriptionDelegate;
    }

    @Override
    public void close() {
        beforeDelegate.close();
        afterDelegate.close();
        removedDelegate.close();
        updatedDelegate.close();
        updateDescriptionDelegate.close();
    }
}
