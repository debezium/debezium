/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

/**
 * This interface allows the code to optionally pass a value between two parts of the application.
 * 
 * @author Jiri Pechanec
 *
 * @param <T> type of the value to pass
 */
public interface ResultReceiver<T> {

    /**
     * Send the object to the receiver.
     * @param o - object to be delivered
     */
    public void deliver(T o);

    /**
     * @return true if a value has been sent to the receiver
     */
    public boolean hasReceived();

    /**
     * @return the object sent to the receiver
     */
    public T get();

    /**
     * @return default, not thread-safe implementation of the receiver
     */
    public static <T> ResultReceiver<T> create() {
        return new ResultReceiver<T>() {
            private boolean received = false;
            private T object = null;

            public void deliver(T o) {
                received = true;
                object = o;
            }

            public boolean hasReceived() {
                return received;
            }

            public T get() {
                return object;
            }

            @Override
            public String toString() {
                return "[received = " + received + ", object = " + object + "]";
            }
        };
    }
}
