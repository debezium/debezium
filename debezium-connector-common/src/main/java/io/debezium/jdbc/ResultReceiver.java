/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.jdbc;

/**
 * This interface allows the code to optionally pass a value between two parts of the application.
 *
 * @author Jiri Pechanec
 */
public interface ResultReceiver {

    /**
     * Send the object to the receiver.
     * @param o - object to be delivered
     */
    void deliver(Object o);

    /**
     * @return true if a value has been sent to the receiver
     */
    boolean hasReceived();

    /**
     * @return the object sent to the receiver
     */
    Object get();

    /**
     * @return default, not thread-safe implementation of the receiver
     */
    static ResultReceiver create() {
        return new ResultReceiver() {
            private boolean received = false;
            private Object object = null;

            @Override
            public void deliver(Object o) {
                received = true;
                object = o;
            }

            @Override
            public boolean hasReceived() {
                return received;
            }

            @Override
            public Object get() {
                return object;
            }

            @Override
            public String toString() {
                return "[received = " + received + ", object = " + object + "]";
            }
        };
    }
}
