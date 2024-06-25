package io.debezium.connector.postgresql.transforms.yugabytedb;

import java.util.Objects;

/**
 * Helper structure to denote a pair of objects.
 * @param <A>
 * @param <B>
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
public class Pair<A, B> {
  private final A first;
  private final B second;

  public Pair(A first, B second) {
    this.first = first;
    this.second = second;
  }

  public A getFirst() {
    return this.first;
  }

  public B getSecond() {
    return this.second;
  }

  public boolean equals(Object o) {
    if (this == o) {
      return true;
    } else if (o != null && this.getClass() == o.getClass()) {
      Pair<?, ?> pair = (Pair) o;
      if (this.first != null) {
        if (!this.first.equals(pair.first)) {
          return false;
        }
      } else if (pair.first != null) {
        return false;
      }

      if (this.second != null) {
        return this.second.equals(pair.second);
      } else {
        return pair.second == null;
      }
    } else {
      return false;
    }
  }

  public int hashCode() {
    return Objects.hashCode(new Object[]{this.first, this.second});
  }
}
