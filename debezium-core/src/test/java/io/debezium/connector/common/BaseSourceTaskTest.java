/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.common;

import static io.debezium.relational.RelationalDatabaseConnectorConfig.ERROR_HANDLING_MAX_RETRIES;
import static io.debezium.relational.RelationalDatabaseConnectorConfig.ERROR_HANDLING_RETRY_BACKOFF_MS;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.fest.assertions.Assertions;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

/**
 * @author Tomasz Rojek
 *
 */
@RunWith(MockitoJUnitRunner.class)
public class BaseSourceTaskTest {

    @Mock
    private SourceTaskContext sourceTaskContext;

    @Mock(answer = Answers.CALLS_REAL_METHODS)
    private BaseSourceTask baseSourceTask;

    @Before
    public void initializeContext() {
        baseSourceTask.initialize(sourceTaskContext);
    }

    @Test
    public void givenRetryEnabledOnExceptionRetriesDefinedNumberOfTimes() throws InterruptedException {
        initializeRetry(2, 1);
        Exception exception = new RuntimeException();
        when(baseSourceTask.pollRecords()).thenThrow(exception);

        pollAndVerifyExceptionIsThrownOfTypeAndWithRootCause(RetriableException.class, exception);
        pollAndVerifyExceptionIsThrownOfTypeAndWithRootCause(RetriableException.class, exception);
        pollAndVerifyExceptionIsThrownOfTypeAndWithRootCause(ConnectException.class, exception);

        verify(baseSourceTask, times(3)).pollRecords();
    }

    private void initializeRetry(int maxRetries, long backOff) {
        Map<String, String> props = new HashMap<>();
        props.put(ERROR_HANDLING_MAX_RETRIES.name(), maxRetries + "");
        props.put(ERROR_HANDLING_RETRY_BACKOFF_MS.name(), backOff + "");
        baseSourceTask.start(props);
    }

    private void pollAndVerifyExceptionIsThrownOfTypeAndWithRootCause(Class<? extends Exception> exceptionClass,
                                                                      Exception rootCause)
            throws InterruptedException {
        try {
            baseSourceTask.poll();
            fail("Should throw ConnectException.");
        }
        catch (Exception e) {
            Assertions.assertThat(e).isExactlyInstanceOf(exceptionClass);
            Assertions.assertThat(e.getCause() == rootCause).isTrue();
        }
    }

    @Test
    public void givenRetryDisabledOnExceptionRethrowsOriginalException() throws InterruptedException {
        initializeRetry(0, 0);
        Exception exception = new RuntimeException();
        when(baseSourceTask.pollRecords()).thenThrow(exception);

        try {
            baseSourceTask.poll();
            fail("Should throw RuntimeException.");
        }
        catch (Exception e) {
            Assertions.assertThat(e == exception).isTrue();
        }
    }

    @Test
    public void givenRetryEndlesslyOnExceptionRetryAtLeast10Times() throws InterruptedException {
        initializeRetry(-1, 1);
        Exception exception = new RuntimeException();
        when(baseSourceTask.pollRecords()).thenThrow(exception);

        for (int i = 0; i < 10; i++) {
            pollAndVerifyExceptionIsThrownOfTypeAndWithRootCause(RetriableException.class, exception);
        }

        verify(baseSourceTask, times(10)).pollRecords();
    }

}