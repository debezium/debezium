/*-------------------------------------------------------------------------
*
* Copyright (c) 2003, PostgreSQL Global Development Group
*
*
*-------------------------------------------------------------------------
*/

package org.postgresql.util;

import java.io.Serializable;
import java.sql.SQLException;

/**
 * PGobject is a class used to describe unknown types An unknown type is any type that is unknown by
 * JDBC Standards
 */
public class PGobject implements Serializable, Cloneable {
  protected String type;
  protected String value;

  /**
   * This is called by org.postgresql.Connection.getObject() to create the object.
   */
  public PGobject() {
  }

  /**
   * This method sets the type of this object.
   *
   * <p>
   * It should not be extended by subclasses, hence its final
   *
   * @param type a string describing the type of the object
   */
  public final void setType(String type) {
    this.type = type;
  }

  /**
   * This method sets the value of this object. It must be overidden.
   *
   * @param value a string representation of the value of the object
   * @throws SQLException thrown if value is invalid for this type
   */
  public void setValue(String value) throws SQLException {
    this.value = value;
  }

  /**
   * As this cannot change during the life of the object, it's final.
   *
   * @return the type name of this object
   */
  public final String getType() {
    return type;
  }

  /**
   * This must be overidden, to return the value of the object, in the form required by
   * org.postgresql.
   *
   * @return the value of this object
   */
  public String getValue() {
    return value;
  }

  /**
   * This must be overidden to allow comparisons of objects
   *
   * @param obj Object to compare with
   * @return true if the two boxes are identical
   */
  public boolean equals(Object obj) {
    if (obj instanceof PGobject) {
      final Object otherValue = ((PGobject) obj).getValue();

      if (otherValue == null) {
        return getValue() == null;
      }
      return otherValue.equals(getValue());
    }
    return false;
  }

  /**
   * This must be overidden to allow the object to be cloned
   */
  public Object clone() throws CloneNotSupportedException {
    return super.clone();
  }

  /**
   * This is defined here, so user code need not overide it.
   *
   * @return the value of this object, in the syntax expected by org.postgresql
   */
  public String toString() {
    return getValue();
  }

  /**
   * Compute hash. As equals() use only value. Return the same hash for the same value.
   *
   * @return Value hashcode, 0 if value is null {@link java.util.Objects#hashCode(Object)}
   */
  @Override
  public int hashCode() {
    return getValue() != null ? getValue().hashCode() : 0;
  }
}
