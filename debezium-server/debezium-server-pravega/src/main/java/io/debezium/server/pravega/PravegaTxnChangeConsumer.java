/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.pravega;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.enterprise.context.Dependent;
import javax.inject.Named;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine.ChangeConsumer;
import io.debezium.engine.DebeziumEngine.RecordCommitter;
import io.debezium.server.BaseChangeConsumer;
import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.Transaction;
import io.pravega.client.stream.TransactionalEventStreamWriter;
import io.pravega.client.stream.TxnFailedException;
import io.pravega.client.stream.impl.ByteArraySerializer;

@Named("pravega-txn")
@Dependent
public class PravegaTxnChangeConsumer extends BaseChangeConsumer implements ChangeConsumer<ChangeEvent<Object, Object>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(PravegaTxnChangeConsumer.class);

    private static final String PROP_PREFIX = "debezium.sink.pravega.";
    private static final String PROP_CONTROLLER = PROP_PREFIX + "controller.uri";
    private static final String PROP_SCOPE = PROP_PREFIX + "scope";

    private final Map<String, TransactionalEventStreamWriter<byte[]>> writers = new HashMap<>();
    private final Map<String, Transaction<byte[]>> txns = new HashMap<>();

    @ConfigProperty(name = PROP_CONTROLLER, defaultValue = "tcp://localhost:9090")
    URI controllerUri;

    @ConfigProperty(name = PROP_SCOPE)
    String scope;

    private ClientConfig clientConfig;
    private EventStreamClientFactory factory;
    private EventWriterConfig writerConfig;

    @PostConstruct
    void constructor() {
        clientConfig = ClientConfig.builder()
                .controllerURI(controllerUri)
                .build();
        LOGGER.debug("Creating client factory for scope {} with controller {}", scope, controllerUri);
        factory = EventStreamClientFactory.withScope(scope, clientConfig);
        writerConfig = EventWriterConfig.builder().build();
    }

    @PreDestroy
    void destructor() {
        LOGGER.debug("Closing {} writer(s)", writers.size());
        writers.values().forEach(TransactionalEventStreamWriter::close);
        LOGGER.debug("Closing client factory", writers.size());
        factory.close();
    }

    @Override
    public void handleBatch(List<ChangeEvent<Object, Object>> records, RecordCommitter<ChangeEvent<Object, Object>> committer) throws InterruptedException {
        for (ChangeEvent<Object, Object> changeEvent : records) {
            String streamName = streamNameMapper.map(changeEvent.destination());
            final Transaction<byte[]> txn = txns.computeIfAbsent(streamName, (stream) -> createTxn(stream));
            try {
                if (changeEvent.key() != null) {
                    txn.writeEvent(getString(changeEvent.key()), getBytes(changeEvent.value()));
                }
                else {
                    txn.writeEvent(getBytes(changeEvent.value()));
                }
            }
            catch (TxnFailedException e) {
                throw new RuntimeException(e);
            }
            committer.markProcessed(changeEvent);
        }
        txns.values().forEach(t -> {
            try {
                t.commit();
            }
            catch (TxnFailedException e) {
                throw new RuntimeException(e);
            }
        });
        committer.markBatchFinished();
        txns.clear();
    }

    private Transaction<byte[]> createTxn(String stream) {
        final TransactionalEventStreamWriter<byte[]> writer = writers.computeIfAbsent(stream, (s) -> createWriter(s));
        LOGGER.debug("Creating transaction for stream {}", stream);
        return writer.beginTxn();
    }

    private TransactionalEventStreamWriter<byte[]> createWriter(String stream) {
        LOGGER.debug("Creating writer for stream {}", stream);
        return factory.createTransactionalEventWriter(stream, new ByteArraySerializer(), writerConfig);
    }

}
