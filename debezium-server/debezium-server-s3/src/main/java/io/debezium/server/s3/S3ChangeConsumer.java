/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.s3;

import javax.enterprise.context.Dependent;
import javax.inject.Named;

/**
 * Implementation of the consumer that delivers the messages into Amazon S3 destination.
 *
 * @author Jiri Pechanec
 */
@Named("s3")
@Dependent
public class S3ChangeConsumer extends AbstractS3ChangeConsumer {
}