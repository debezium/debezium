/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.time.Duration;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.mongodb.MongoCommandException;
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;

import io.debezium.DebeziumException;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.util.LoggingContext;

public class MongoDbErrorHandlerTest {

    @ParameterizedTest
    @ValueSource(strings = { "pre-image", "post-image" })
    void shouldStopWhenRequiredImageIsMissing(String image) {
        final var queue = queue();
        final var error = new DebeziumException("Reading change stream",
                commandException(47, "Change stream was configured to require a " + image
                        + " for all update events, but the " + image + " was not found for event"));
        final var handler = new MongoDbErrorHandler(new MongoDbConnectorConfig(TestHelper.getConfiguration()), queue, null);
        handler.setProducerThrowable(error);

        assertThatThrownBy(queue::poll)
                .isInstanceOf(ConnectException.class)
                .isNotInstanceOf(RetriableException.class)
                .hasCause(error);
        assertThat(handler.getRetries()).isZero();
    }

    @Test
    void shouldPreserveRetryForConnectionFailures() {
        final var queue = queue();
        final var error = new DebeziumException("Reading change stream", new IOException("Connection reset"));
        new MongoDbErrorHandler(new MongoDbConnectorConfig(TestHelper.getConfiguration()), queue, null).setProducerThrowable(error);
        assertThatThrownBy(queue::poll).isInstanceOf(RetriableException.class).hasCause(error);
    }

    @Test
    void shouldNotClassifyUnrelatedNoMatchingDocumentAsMissingImage() {
        final var queue = queue();
        final var error = commandException(47, "No matching document for another operation");
        new MongoDbErrorHandler(new MongoDbConnectorConfig(TestHelper.getConfiguration()), queue, null).setProducerThrowable(error);
        assertThatThrownBy(queue::poll).isInstanceOf(RetriableException.class).hasCause(error);
    }

    @Test
    void shouldFindMissingImageBehindAnotherMongoException() {
        final var queue = queue();
        final var missing = commandException(47, "Change stream was configured to require a pre-image but the pre-image was not found");
        final var error = new MongoException("Wrapped failure", missing);
        new MongoDbErrorHandler(new MongoDbConnectorConfig(TestHelper.getConfiguration()), queue, null).setProducerThrowable(error);
        assertThatThrownBy(queue::poll).isNotInstanceOf(RetriableException.class).hasCause(error);
    }

    private static MongoCommandException commandException(int code, String message) {
        final var response = new BsonDocument()
                .append("ok", new BsonInt32(0))
                .append("code", new BsonInt32(code))
                .append("errmsg", new BsonString(message));
        return new MongoCommandException(response, new ServerAddress());
    }

    private static ChangeEventQueue<DataChangeEvent> queue() {
        return new ChangeEventQueue.Builder<DataChangeEvent>()
                .pollInterval(Duration.ofMillis(1))
                .pollDispatchInterval(Duration.ZERO)
                .maxQueueSize(10)
                .maxBatchSize(10)
                .loggingContextSupplier(() -> LoggingContext.forConnector("mongodb", "test", "test"))
                .build();
    }
}
