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
 * This annotation documents the class as <i>not</i> being thread-safe, meaning the caller is expected to properly handle and
 * guard all concurrent operations on an instance.
 * </p>
 * 
 * @see ThreadSafe
 */
@Documented
@Target( ElementType.TYPE )
@Retention( RetentionPolicy.RUNTIME )
public @interface NotThreadSafe {
}