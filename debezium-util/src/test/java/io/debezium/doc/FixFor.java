/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.doc;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.RetentionPolicy.CLASS;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

/**
 * Annotation that can be used to help track that a test is verifying the fix for one or more specific issues. To use, simply
 * place this annotation on the test method and reference the JIRA issue number:
 *
 * <pre>
 *    &#064;FixFor("DBZ-123")
 *    &#064;Test
 *    public void shouldVerifyBehavior() {
 *     ...
 *    }
 * </pre>
 * <p>
 * It is also possible to reference multiple JIRA issues if the test is verifying multiple ones:
 *
 * <pre>
 *    &#064;FixFor({"DBZ-123","DBZ-456"})
 *    &#064;Test
 *    public void shouldVerifyBehavior() {
 *     ...
 *    }
 * </pre>
 *
 * </p>
 */
@Documented
@Retention(CLASS)
@Target(METHOD)
public @interface FixFor {
    /**
     * The JIRA issue for which this is a fix. For example, "DBZ-123".
     *
     * @return the issue
     */
    String[] value();
}
