package org.postgresql.core;

/**
 * Represents a provider of results.
 *
 * @param <T> the type of results provided by this provider
 */
public interface Provider<T> {

  /**
   * Gets a result.
   *
   * @return a result
   */
  T get();
}
