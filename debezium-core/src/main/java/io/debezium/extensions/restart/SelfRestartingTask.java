/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.extensions.restart;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.NotThreadSafe;
import io.debezium.config.Configuration;
import io.debezium.util.Clock;
import io.debezium.util.ElapsedTimeStrategy;

/**
 * A Kafka Connect source task that encapsulates any other source task. It creates a delegate task to which all
 * lifecycle methods are delegated. When a {@link #poll() poll method} throws an exception it tries to reconnect to the source
 * 
 * @see MySqlConnectorTask example of usage
 * @author Jiri Pechanec
 */@NotThreadSafe
public abstract class SelfRestartingTask<T extends SourceTask> extends SourceTask {
    private static final int SLEEP_DURING_ELAPSE_WAITING = 1000;
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private T delegate;
    private Class<T> delegateClass;
    private Map<String, String> taskProperties;
    private AtomicBoolean terminate = new AtomicBoolean();

    private Clock clock = Clock.SYSTEM;
    private boolean restartingAllowed = false;
    private long maxRuntime = TimeUnit.MINUTES.toNanos(4);
    private long initialBackOffPause = 0; 
    private long maximumBackOffPause = 0; 

    private SourceTaskContext context;

    protected SelfRestartingTask(final Class<T> delegateClass) {
        this.delegateClass = delegateClass;
        delegate = createDelegate();
    }

    @Override
    public String version() {
        return delegate.version();
    }


    @Override
    public void initialize(final SourceTaskContext context) {
        this.context = context;
        delegate.initialize(context);
    }

    /**
     * Back-off properties are read from the connector configuration
     */
    @Override
    public void start(final Map<String, String> props) {
        taskProperties = props;

        // Validate the configuration ...
        final Configuration config = Configuration.from(props);
        if (!config.validateAndRecord(SelfRestartingConfig.ALL_FIELDS, logger::error)) {
            throw new ConnectException("Error configuring an instance of " + getClass().getSimpleName() + "; check the logs for details");
        }

        restartingAllowed = config.getBoolean(SelfRestartingConfig.ENABLED);
        maxRuntime = TimeUnit.MILLISECONDS.toNanos(config.getLong(SelfRestartingConfig.MAX_TIMEOUT));
        initialBackOffPause = config.getLong(SelfRestartingConfig.BACK_OFF_INITIAL);
        maximumBackOffPause = config.getLong(SelfRestartingConfig.BACK_OFF_MAXIMUM);

        delegate.start(props);
    }

    /**
     * When a delegate's poll() method throws an exception then it is caught and either re-thrown
     * or the task tries to restart the delegate
     */
    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        try {
            return delegate.poll();
        } catch (final InterruptedException e) {
            throw e;
        } catch (final RuntimeException e) {
            if (!restartingAllowed) {
                throw e;
            }
            logger.warn("Exception caught, attempting restart", e);
            stopDelegate();
            return restart(e);
        }
    }

    /**
     * A core of restart logic: it tries to restart the connector by creating a new instance of delegate class
     * and calling initialize() and start() lifecycle method. If the restart fails an exponential back-off
     * is in place
     * @param originalException - the exception that was originally thrown by the delegate 
     * @return records delivered by delegate after successful restart
     * @throws InterruptedException
     */
    private List<SourceRecord> restart(final RuntimeException originalException) throws InterruptedException {
        final long endTime = clock.currentTimeInNanos() + maxRuntime;
        final ElapsedTimeStrategy elapsedTime = ElapsedTimeStrategy.exponential(clock, initialBackOffPause, maximumBackOffPause);
        List<SourceRecord> records = new ArrayList<SourceRecord>();
        while (!terminate.get()) {
            try {
                delegate = createDelegate();
                delegate.initialize(context);
                delegate.start(taskProperties);
                records = delegate.poll();
                logger.info("Restart succeeded");
            }
            catch (final InterruptedException e) {
                throw e;
            }
            catch (final RuntimeException e) {
                logger.debug("Restart failed", e);
                stopDelegate();
                while (!elapsedTime.hasElapsed() && !terminate.get()) {
                    if (clock.currentTimeInNanos() > endTime) {
                        logger.error("Timeout while recovering from error");
                        throw originalException;
                    }
                    Thread.sleep(SLEEP_DURING_ELAPSE_WAITING);
                }
            }
        }
        return records;
    }

    @Override
    public void stop() {
        logger.info("Termination requested");
        terminate.set(true);
        stopDelegate();
    }

    private void stopDelegate() {
        try {
            delegate.stop();
        }
        catch (final Exception e) {
            logger.debug("Error while stopping delegate", e);
        }
    }

    private T createDelegate() {
        try {
            logger.info("Creating an instance of delegate {}", delegateClass);
            return delegateClass.newInstance();
        } catch (final InstantiationException | IllegalAccessException e) {
            throw new ConnectException("Failed to create a delegate instance", e);
        }
    }

    @Override
    public void commit() throws InterruptedException {
        delegate.commit();
    }

    @Override
    public void commitRecord(SourceRecord record) throws InterruptedException {
        delegate.commitRecord(record);
    }
}
