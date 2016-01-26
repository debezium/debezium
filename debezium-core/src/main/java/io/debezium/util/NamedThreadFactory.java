/*
 * Copyright 2010 Debezium Authors.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import io.debezium.annotation.ThreadSafe;

/**
 * A simple named thread factory that creates threads named "{@code $PREFIX$-$NAME$-thread-$NUMBER$}".
 * 
 * @author Randall Hauch
 */
@ThreadSafe
public final class NamedThreadFactory implements ThreadFactory {

    private static final boolean DEFAULT_DAEMON_THREAD = true;
    private static final int DEFAULT_STACK_SIZE = 0;

    private final boolean daemonThreads;
    private final ThreadGroup group;
    private final AtomicInteger threadNumber = new AtomicInteger(1);
    private final String namePrefix;
    private final int stackSize;
    private final Consumer<String> afterThreadCreation;

    public NamedThreadFactory(String prefix) {
        this(prefix, DEFAULT_DAEMON_THREAD, DEFAULT_STACK_SIZE);
    }

    public NamedThreadFactory(String prefix, boolean daemonThreads) {
        this(prefix, daemonThreads, DEFAULT_STACK_SIZE);
    }

    public NamedThreadFactory(String prefix, boolean daemonThreads, int stackSize) {
        this(prefix, daemonThreads, stackSize, null);
    }

    public NamedThreadFactory(String prefix, boolean daemonThreads, int stackSize, Consumer<String> afterThreadCreation) {
        if ( prefix == null ) throw new IllegalArgumentException("The thread prefix may not be null");
        final SecurityManager s = System.getSecurityManager();
        this.group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
        this.namePrefix = prefix;
        this.daemonThreads = daemonThreads;
        this.stackSize = stackSize;
        this.afterThreadCreation = afterThreadCreation;
    }

    @Override
    public Thread newThread(final Runnable runnable) {
        String threadName = namePrefix + threadNumber.getAndIncrement();
        final Thread t = new Thread(group, runnable, threadName, stackSize);
        t.setDaemon(daemonThreads);
        if (t.getPriority() != Thread.NORM_PRIORITY) {
            t.setPriority(Thread.NORM_PRIORITY);
        }
        if (afterThreadCreation != null) {
            try {
                afterThreadCreation.accept(threadName);
            } catch (Throwable e) {
                // do nothing
            }
        }
        return t;
    }
}
