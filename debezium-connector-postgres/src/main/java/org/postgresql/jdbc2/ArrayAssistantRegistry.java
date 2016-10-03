package org.postgresql.jdbc2;

import java.util.HashMap;
import java.util.Map;

/**
 * Array assistants register here
 *
 * @author Minglei Tu
 */
public class ArrayAssistantRegistry {
  private static Map<Integer, ArrayAssistant> arrayAssistantMap =
      new HashMap<Integer, ArrayAssistant>();

  public static ArrayAssistant getAssistant(int oid) {
    return arrayAssistantMap.get(oid);
  }

  ////
  public static void register(int oid, ArrayAssistant assistant) {
    arrayAssistantMap.put(oid, assistant);
  }
}
