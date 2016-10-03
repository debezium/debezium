/*-------------------------------------------------------------------------
*
* Copyright (c) 2003-2014, PostgreSQL Global Development Group
*
*
*-------------------------------------------------------------------------
*/

package org.postgresql.geometric;

import org.postgresql.util.PGobject;
import org.postgresql.util.PGtokenizer;

import java.io.Serializable;
import java.sql.SQLException;

/**
 * This implements the polygon datatype within PostgreSQL.
 */
public class PGpolygon extends PGobject implements Serializable, Cloneable {
  /**
   * The points defining the polygon
   */
  public PGpoint points[];

  /**
   * Creates a polygon using an array of PGpoints
   *
   * @param points the points defining the polygon
   */
  public PGpolygon(PGpoint[] points) {
    this();
    this.points = points;
  }

  /**
   * @param s definition of the polygon in PostgreSQL's syntax.
   * @throws SQLException on conversion failure
   */
  public PGpolygon(String s) throws SQLException {
    this();
    setValue(s);
  }

  /**
   * Required by the driver
   */
  public PGpolygon() {
    setType("polygon");
  }

  /**
   * @param s Definition of the polygon in PostgreSQL's syntax
   * @throws SQLException on conversion failure
   */
  public void setValue(String s) throws SQLException {
    PGtokenizer t = new PGtokenizer(PGtokenizer.removePara(s), ',');
    int npoints = t.getSize();
    points = new PGpoint[npoints];
    for (int p = 0; p < npoints; p++) {
      points[p] = new PGpoint(t.getToken(p));
    }
  }

  /**
   * @param obj Object to compare with
   * @return true if the two polygons are identical
   */
  public boolean equals(Object obj) {
    if (obj instanceof PGpolygon) {
      PGpolygon p = (PGpolygon) obj;

      if (p.points.length != points.length) {
        return false;
      }

      for (int i = 0; i < points.length; i++) {
        if (!points[i].equals(p.points[i])) {
          return false;
        }
      }

      return true;
    }
    return false;
  }

  public int hashCode() {
    // XXX not very good..
    int hash = 0;
    for (int i = 0; i < points.length && i < 5; ++i) {
      hash = hash ^ points[i].hashCode();
    }
    return hash;
  }

  public Object clone() throws CloneNotSupportedException {
    PGpolygon newPGpolygon = (PGpolygon) super.clone();
    if (newPGpolygon.points != null) {
      newPGpolygon.points = (PGpoint[]) newPGpolygon.points.clone();
      for (int i = 0; i < newPGpolygon.points.length; ++i) {
        if (newPGpolygon.points[i] != null) {
          newPGpolygon.points[i] = (PGpoint) newPGpolygon.points[i].clone();
        }
      }
    }
    return newPGpolygon;
  }

  /**
   * @return the PGpolygon in the syntax expected by org.postgresql
   */
  public String getValue() {
    StringBuilder b = new StringBuilder();
    b.append("(");
    for (int p = 0; p < points.length; p++) {
      if (p > 0) {
        b.append(",");
      }
      b.append(points[p].toString());
    }
    b.append(")");
    return b.toString();
  }
}
