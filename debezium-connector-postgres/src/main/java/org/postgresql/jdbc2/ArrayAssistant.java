package org.postgresql.jdbc2;

/**
 * Implement this interface and register the its instance to ArrayAssistantRegistry, to let Postgres
 * driver to support more array type
 *
 * @author Minglei Tu
 */
public interface ArrayAssistant {
  /**
   * get array base type
   *
   * @return array base type
   */
  Class<?> baseType();

  /**
   * build a array element from its binary bytes
   *
   * @param bytes input bytes
   * @param pos position in input array
   * @param len length of the element
   * @return array element from its binary bytes
   */
  Object buildElement(byte[] bytes, int pos, int len);

  /**
   * build an array element from its literal string
   *
   * @param literal string representation of array element
   * @return array element
   */
  Object buildElement(String literal);
}
