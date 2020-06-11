/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.s3;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine.RecordCommitter;
import io.debezium.server.s3.batchwriter.BatchRecordWriter;
import io.debezium.server.s3.batchwriter.JsonMapDbBatchRecordWriter;
import io.debezium.server.s3.objectkeymapper.ObjectKeyMapper;
import io.debezium.server.s3.objectkeymapper.TimeBasedDailyObjectKeyMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.Dependent;
import javax.inject.Named;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;

/**
 * Implementation of the consumer that delivers the messages into Amazon S3 destination.
 *
 * @author Jiri Pechanec
 */
@Named("s3batch")
@Dependent
public class S3BatchChangeConsumer extends AbstractS3ChangeConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(S3BatchChangeConsumer.class);
    private final ObjectKeyMapper objectKeyMapper = new TimeBasedDailyObjectKeyMapper();

    @Override
    public void handleBatch(List<ChangeEvent<Object, Object>> records, RecordCommitter<ChangeEvent<Object, Object>> committer)
            throws InterruptedException {
        try {
            BatchRecordWriter batchWriter = new JsonMapDbBatchRecordWriter(objectKeyMapper, s3client, bucket);
            for (ChangeEvent<Object, Object> record : records) {
                // print(record);
                batchWriter.append(record.destination(), getString(record));
                committer.markProcessed(record);
            }
            batchWriter.uploadBatch();
            committer.markBatchFinished();
        }
        catch (Exception e) {
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            LOGGER.error(sw.toString());
            throw new InterruptedException(e.getMessage());
        }
    }
}
