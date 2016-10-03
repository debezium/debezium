package org.postgresql.core;

public interface Version {

  /**
   * Get a machine-readable version number.
   *
   * @return the version in numeric XXYYZZ form, e.g. 90401 for 9.4.1
   */
  int getVersionNum();

}
