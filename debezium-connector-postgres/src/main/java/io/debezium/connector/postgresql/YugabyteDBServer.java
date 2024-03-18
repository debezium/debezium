package io.debezium.connector.postgresql;

/**
 * Helper class to add server related methods to aid in code execution for YugabyteDB specific flow.
 *
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
public class YugabyteDBServer {
  public static boolean isEnabled() {
    return true;
  }
}
