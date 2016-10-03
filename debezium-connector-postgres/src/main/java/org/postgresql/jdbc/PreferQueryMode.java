package org.postgresql.jdbc;

/**
 * Specifies which mode is used to execute queries to database: simple means ('Q' execute, no parse, no bind, text mode only),
 * extended means always use bind/execute messages, extendedForPrepared means extended for prepared statements only.
 *
 * Note: this is for debugging purposes only.
 *
 * @see org.postgresql.PGProperty#PREFER_QUERY_MODE
 */
public enum PreferQueryMode {
  SIMPLE("simple"),
  EXTENDED_FOR_PREPARED("extendedForPrepared"),
  EXTENDED("extended"),
  EXTENDED_CACHE_EVERYTING("extendedCacheEveryting");

  private final String value;

  PreferQueryMode(String value) {
    this.value = value;
  }

  public static PreferQueryMode of(String mode) {
    for (PreferQueryMode preferQueryMode : values()) {
      if (preferQueryMode.value.equals(mode)) {
        return preferQueryMode;
      }
    }
    return EXTENDED;
  }

  public String value() {
    return value;
  }
}
