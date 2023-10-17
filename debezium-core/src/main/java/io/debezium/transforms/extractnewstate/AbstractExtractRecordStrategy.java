/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms.extractnewstate;

import static io.debezium.data.Envelope.FieldName.AFTER;
import static io.debezium.data.Envelope.FieldName.BEFORE;
import static io.debezium.transforms.ExtractNewRecordStateConfigDefinition.DELETED_FIELD;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.ExtractField;
import org.apache.kafka.connect.transforms.InsertField;

/**
 * An abstract implementation of {@link ExtractRecordStrategy}.
 *
 * @author Harvey Yue
 */
public abstract class AbstractExtractRecordStrategy<R extends ConnectRecord<R>> implements ExtractRecordStrategy<R> {

    private static final String UPDATE_DESCRIPTION = "updateDescription";
    protected final ExtractField<R> afterDelegate = new ExtractField.Value<>();
    protected final ExtractField<R> beforeDelegate = new ExtractField.Value<>();
    protected final InsertField<R> removedDelegate = new InsertField.Value<>();
    protected final InsertField<R> updatedDelegate = new InsertField.Value<>();
    // for mongodb
    protected final ExtractField<R> updateDescriptionDelegate = new ExtractField.Value<>();

    public AbstractExtractRecordStrategy() {
        init();
    }

    private void init() {
        Map<String, String> delegateConfig = new HashMap<>();
        delegateConfig.put("field", BEFORE);
        beforeDelegate.configure(delegateConfig);

        delegateConfig = new HashMap<>();
        delegateConfig.put("field", AFTER);
        afterDelegate.configure(delegateConfig);

        delegateConfig = new HashMap<>();
        delegateConfig.put("static.field", DELETED_FIELD);
        delegateConfig.put("static.value", "true");
        removedDelegate.configure(delegateConfig);

        delegateConfig = new HashMap<>();
        delegateConfig.put("static.field", DELETED_FIELD);
        delegateConfig.put("static.value", "false");
        updatedDelegate.configure(delegateConfig);

        delegateConfig = new HashMap<>();
        delegateConfig.put("field", UPDATE_DESCRIPTION);
        updateDescriptionDelegate.configure(delegateConfig);
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
