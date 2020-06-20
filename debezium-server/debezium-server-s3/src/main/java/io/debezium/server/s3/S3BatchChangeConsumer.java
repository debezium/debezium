/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.s3;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URISyntaxException;
import java.util.List;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.enterprise.context.Dependent;
import javax.inject.Named;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine.RecordCommitter;
import io.debezium.server.s3.batchwriter.BatchRecordWriter;
import io.debezium.server.s3.batchwriter.JsonMapDbBatchRecordWriter;
import io.debezium.server.s3.objectkeymapper.ObjectKeyMapper;
import io.debezium.server.s3.objectkeymapper.TimeBasedDailyObjectKeyMapper;

/**
 * Implementation of the consumer that delivers the messages into Amazon S3 destination.
 *
 * @author Ismail Simsek
 */
@Named("s3batch")
@Dependent
public class S3BatchChangeConsumer extends AbstractS3ChangeConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(S3BatchChangeConsumer.class);
    // private final ObjectKeyMapper objectKeyMapper = new TimeBasedDailyObjectKeyMapper();
    BatchRecordWriter batchWriter;

    @PostConstruct
    void connect() throws URISyntaxException {
        super.connect();
        batchWriter = new JsonMapDbBatchRecordWriter(new TimeBasedDailyObjectKeyMapper(), s3client, bucket);
    }

    @PreDestroy
    void close() {
        try {
            batchWriter.close();
        }
        catch (Exception e) {
            LOGGER.warn("Exception while closing batchWriter:{} ", e.getMessage());
        }
        super.close();
    }

    @Override
    public void handleBatch(List<ChangeEvent<Object, Object>> records, RecordCommitter<ChangeEvent<Object, Object>> committer)
            throws InterruptedException {
        try {
            for (ChangeEvent<Object, Object> record : records) {
                batchWriter.append(record.destination(), getString(record.value()));
                committer.markProcessed(record);
            }
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
