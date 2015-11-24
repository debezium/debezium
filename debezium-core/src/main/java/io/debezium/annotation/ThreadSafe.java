package io.debezium.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Copyright (c) 2005 Brian Goetz and Tim Peierls.<br />
 * Released under the Creative Commons Attribution License<br />
 * (http://creativecommons.org/licenses/by/2.5)<br />
 * Official home: http://www.jcip.net<br />
 * Adopted from Java Concurrency in Practice.
 * <p>
 * This annotation documents the class as being thread-safe. This means that no sequences of accesses (reads and writes to public
 * fields, calls to public methods) may put the object into an invalid state, regardless of the interleaving of those actions by
 * the runtime, and without requiring any additional synchronization or coordination on the part of the caller.
 * </p>
 * 
 * @see NotThreadSafe
 */
@Documented
@Target( {ElementType.TYPE, ElementType.FIELD} )
@Retention( RetentionPolicy.RUNTIME )
public @interface ThreadSafe {
}