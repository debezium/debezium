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
 * This annotation documents that instances of the annotated class are immutable. This means that its state is seen to others as
 * never being changed, even though the actual private internal state may indeed change. Therefore, in an immutable class:
 * <ul>
 * <li>all public fields are final; and</li>
 * <li>all public final reference fields refer to other immutable objects; and</li>
 * <li>constructors and methods do not publish references to any internal state which is potentially mutable by the
 * implementation.</li>
 * </ul>
 * </p>
 */
@Documented
@Target( ElementType.TYPE )
@Retention( RetentionPolicy.RUNTIME )
public @interface Immutable {
}
