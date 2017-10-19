/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.connect.source.SourceRecord;

/**
 * A component that blocks doing nothing until the connector task is stopped
 * 
 * @author Peter Goransson
 *
 */
public class BlockingReader extends AbstractReader {

    private final CountDownLatch latch = new CountDownLatch(1);
    
    public BlockingReader(String name, MySqlTaskContext context) {
        super(name, context);
    }

    @Override
    protected void doStart() {
        logger.info("Connector has completed all of its work but will continue in the running state. It can be shutdown at any time.");
    }

    @Override
    protected void doStop() {
        try {
            this.completeSuccessfully(); 
        }
        finally {
            latch.countDown();
        }
    }

    @Override
    protected void doCleanup() {
        logger.debug("Blocking Reader has completed.");
    }
    
    /**
     * Waits indefinitely until the connector task is shut down
     */
    @Override
    public List<SourceRecord> poll() throws InterruptedException {                
        latch.await();   
        return super.poll();
    }

}
