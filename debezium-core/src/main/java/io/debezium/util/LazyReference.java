/*
 * Copyright 2015 Debezium Authors.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

import io.debezium.annotation.ThreadSafe;

/**
 * An atomic reference that atomically uses a supplier to lazily accesses the referenced object the first time it is needed.
 * 
 * @author Randall Hauch
 * @param <T> the type of referenced object
 */
@ThreadSafe
public final class LazyReference<T> {

    public static <T> LazyReference<T> create(Supplier<T> supplier) {
        return new LazyReference<T>(supplier);
    }

    private final AtomicReference<T> ref = new AtomicReference<>();
    private final Supplier<T> supplier;

    private LazyReference(Supplier<T> supplier) {
        this.supplier = supplier;
    }

    /**
     * Determine if the referenced object has been created and accessed.
     * 
     * @return {@code true} if the object has been created, or false otherwise
     */
    public boolean isInitialized() {
        return ref.get() != null;
    }

    /**
     * If the referenced object has been {@link #isInitialized() initialized}, then release it.
     * This method does nothing if the reference has not yet been accessed or {@link #isInitialized() initialized}.
     */
    public void release() {
        release(null);
    }

    /**
     * If the referenced object has been {@link #isInitialized() initialized}, then release it and call the supplied function with
     * the reference. This method does nothing if the reference has not yet been accessed or {@link #isInitialized() initialized}.
     * 
     * @param finalizer the function that should be called when the previously-{@link #isInitialized() initialized} referenced
     *            object is released; may be null
     */
    public void release(Consumer<T> finalizer) {
        ref.updateAndGet(existing->{
            if ( existing != null && finalizer != null ) finalizer.accept(existing);
            return null;
        });
    }

    /**
     * Get the referenced value (creating it if required) and call the supplied function.
     * 
     * @param consumer the function that operates on the value; may not be null
     * @return true if the function was called on the referenced value, or false if there is no referenced value
     */
    public boolean execute(Consumer<T> consumer) {
        T value = get();
        if (value == null) return false;
        consumer.accept(value);
        return true;
    }

    public T get() {
        return ref.updateAndGet(existing->existing != null ? existing : supplier.get());
    }
}
