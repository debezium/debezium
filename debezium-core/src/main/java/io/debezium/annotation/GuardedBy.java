package io.debezium.annotation;

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
 * An annotation that describes the monitor protecting the annotated field or method. For example, <code>@GuardedBy("this")</code>
 * specifies that the lock is the object in whose class the field or method is defined, while <code>@GuardedBy("lock")</code>
 * specifies that the method or field is guarded by a lock held in the "lock" field.
 * </p>
 * 
 * @see ThreadSafe
 * @see NotThreadSafe
 * @see Immutable
 */
@Target( {ElementType.FIELD, ElementType.METHOD} )
@Retention( RetentionPolicy.SOURCE )
public @interface GuardedBy {
    String value();
}