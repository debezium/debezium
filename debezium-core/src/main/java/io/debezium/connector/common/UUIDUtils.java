/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.common;

import java.security.SecureRandom;
import java.time.Instant;
import java.util.UUID;

/**
 * Taken from <a href=
 * "https://github.com/OpenLineage/OpenLineage/blob/main/client/java/src/main/java/io/openlineage/client/utils/UUIDUtils.java"
 * >OpenLineage code base</a>.
 **/
public class UUIDUtils {

  /**
   * Generate new UUID. Each function call returns a new UUID value.
   *
   * <p>UUID version is an implementation detail, and <b>should not</b> be relied on. For now it is
   * <a href="https://datatracker.ietf.org/doc/rfc9562/">UUIDv7</a>, so for increasing instant
   * values, returned UUID is always greater than previous one.
   *
   * @return {@link UUID} v7
   * @since 1.15.0
   */
  public static UUID generateNewUUID() {
    return generateNewUUID(Instant.now());
  }

  /**
   * Generate new UUID for an instant of time. Each function call returns a new UUID value.
   *
   * <p>UUID version is an implementation detail, and <b>should not</b> be relied on. For now it is
   * <a href="https://datatracker.ietf.org/doc/rfc9562/">UUIDv7</a>, so for increasing instant
   * values, returned UUID is always greater than previous one.
   *
   * <p>Based on <a
   * href="https://github.com/f4b6a3/uuid-creator/blob/98628c29ac9da704d215245f2099d9b439bc3084/src/main/java/com/github/f4b6a3/uuid/UuidCreator.java#L584-L593"
   * target="_top">com.github.f4b6a3.uuid.UuidCreator.getTimeOrderedEpoch</a> implementation (MIT
   * License).
   *
   * @param instant a given instant
   * @return {@link UUID} v7
   * @since 1.15.0
   */
  public static UUID generateNewUUID(Instant instant) {
    long time = instant.toEpochMilli();
    SecureRandom random = new SecureRandom();
    long msb = (time << 16) | (random.nextLong() & 0x0fffL) | 0x7000L;
    long lsb = (random.nextLong() & 0x3fffffffffffffffL) | 0x8000000000000000L;
    return new UUID(msb, lsb);
  }
}