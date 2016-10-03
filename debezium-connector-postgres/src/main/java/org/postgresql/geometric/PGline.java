/*-------------------------------------------------------------------------
*
* Copyright (c) 2003-2015 PostgreSQL Global Development Group
*
*
*-------------------------------------------------------------------------
*/

package org.postgresql.geometric;

import org.postgresql.util.GT;
import org.postgresql.util.PGobject;
import org.postgresql.util.PGtokenizer;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

import java.io.Serializable;
import java.sql.SQLException;

/**
 * This implements a line represented by the linear equation Ax + By + C = 0
 **/
public class PGline extends PGobject implements Serializable, Cloneable {

  /**
   * Coefficient of x
   */
  public double a;

  /**
   * Coefficient of y
   */
  public double b;

  /**
   * Constant
   */
  public double c;

  /**
   * @param a coefficient of x
   * @param b coefficient of y
   * @param c constant
   */
  public PGline(double a, double b, double c) {
    this();
    this.a = a;
    this.b = b;
    this.c = c;
  }

  /**
   * @param x1 coordinate for first point on the line
   * @param y1 coordinate for first point on the line
   * @param x2 coordinate for second point on the line
   * @param y2 coordinate for second point on the line
   */
  public PGline(double x1, double y1, double x2, double y2) {
    this();
    if (x1 == x2) {
      a = -1;
      b = 0;
    } else {
      a = (y2 - y1) / (x2 - x1);
      b = -1;
    }
    c = y1 - a * x1;
  }

  /**
   * @param p1 first point on the line
   * @param p2 second point on the line
   */
  public PGline(PGpoint p1, PGpoint p2) {
    this(p1.x, p1.y, p2.x, p2.y);
  }

  /**
   * @param lseg Line segment which calls on this line.
   */
  public PGline(PGlseg lseg) {
    this(lseg.point[0], lseg.point[1]);
  }

  /**
   * @param s definition of the line in PostgreSQL's syntax.
   * @throws SQLException on conversion failure
   */
  public PGline(String s) throws SQLException {
    this();
    setValue(s);
  }

  /**
   * required by the driver
   */
  public PGline() {
    setType("line");
  }

  /**
   * @param s Definition of the line in PostgreSQL's syntax
   * @throws SQLException on conversion failure
   */
  @Override
  public void setValue(String s) throws SQLException {
    if (s.trim().startsWith("{")) {
      PGtokenizer t = new PGtokenizer(PGtokenizer.removeCurlyBrace(s), ',');
      if (t.getSize() != 3) {
        throw new PSQLException(GT.tr("Conversion to type {0} failed: {1}.", type, s),
            PSQLState.DATA_TYPE_MISMATCH);
      }
      a = Double.parseDouble(t.getToken(0));
      b = Double.parseDouble(t.getToken(1));
      c = Double.parseDouble(t.getToken(2));
    } else if (s.trim().startsWith("[")) {
      PGtokenizer t = new PGtokenizer(PGtokenizer.removeBox(s), ',');
      if (t.getSize() != 2) {
        throw new PSQLException(GT.tr("Conversion to type {0} failed: {1}.", type, s),
            PSQLState.DATA_TYPE_MISMATCH);
      }
      PGpoint point1 = new PGpoint(t.getToken(0));
      PGpoint point2 = new PGpoint(t.getToken(1));
      a = point2.x - point1.x;
      b = point2.y - point1.y;
      c = point1.y;
    }
  }

  /**
   * @param obj Object to compare with
   * @return true if the two lines are identical
   */
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    if (!super.equals(obj)) {
      return false;
    }

    PGline pGline = (PGline) obj;

    return Double.compare(pGline.a, a) == 0
        && Double.compare(pGline.b, b) == 0
        && Double.compare(pGline.c, c) == 0;
  }

  public int hashCode() {
    int result = super.hashCode();
    long temp;
    temp = Double.doubleToLongBits(a);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    temp = Double.doubleToLongBits(b);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    temp = Double.doubleToLongBits(c);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    return result;
  }

  /**
   * @return the PGline in the syntax expected by org.postgresql
   */
  public String getValue() {
    return "{" + a + "," + b + "," + c + "}";
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    // squid:S2157 "Cloneables" should implement "clone
    return super.clone();
  }
}
