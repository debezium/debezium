/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2004-2014, PostgreSQL Global Development Group
 *
 *
 *-------------------------------------------------------------------------
 */

package org.postgresql.util;

import java.sql.Timestamp;
import java.util.Calendar;

/**
 * This class augments the Java built-in Timestamp to allow for explicit setting of the time zone.
 */
public class PGTimestamp extends Timestamp {
  /**
   * The serial version UID.
   */
  private static final long serialVersionUID = -6245623465210738466L;

  /**
   * The optional calendar for this timestamp.
   */
  private Calendar calendar;

  /**
   * Constructs a <code>PGTimestamp</code> without a time zone. The integral seconds are stored in
   * the underlying date value; the fractional seconds are stored in the <code>nanos</code> field of
   * the <code>Timestamp</code> object.
   *
   * @param time milliseconds since January 1, 1970, 00:00:00 GMT. A negative number is the number
   *        of milliseconds before January 1, 1970, 00:00:00 GMT.
   * @see Timestamp#Timestamp(long)
   */
  public PGTimestamp(long time) {
    this(time, null);
  }

  /**
   * Constructs a <code>PGTimestamp</code> with the given time zone. The integral seconds are stored
   * in the underlying date value; the fractional seconds are stored in the <code>nanos</code> field
   * of the <code>Timestamp</code> object.
   * <p>
   * The calendar object is optional. If absent, the driver will treat the timestamp as
   * <code>timestamp without time zone</code>. When present, the driver will treat the timestamp as
   * a <code>timestamp with time zone</code> using the <code>TimeZone</code> in the calendar object.
   * Furthermore, this calendar will be used instead of the calendar object passed to
   * {@link java.sql.PreparedStatement#setTimestamp(int, Timestamp, Calendar)}.
   *
   * @param time milliseconds since January 1, 1970, 00:00:00 GMT. A negative number is the number
   *        of milliseconds before January 1, 1970, 00:00:00 GMT.
   * @param calendar the calendar object containing the time zone or <code>null</code>.
   * @see Timestamp#Timestamp(long)
   */
  public PGTimestamp(long time, Calendar calendar) {
    super(time);
    this.setCalendar(calendar);
  }

  /**
   * Sets the calendar object for this timestamp.
   *
   * @param calendar the calendar object or <code>null</code>.
   */
  public void setCalendar(Calendar calendar) {
    this.calendar = calendar;
  }

  /**
   * Returns the calendar object for this timestamp.
   *
   * @return the calendar object or <code>null</code>.
   */
  public Calendar getCalendar() {
    return calendar;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((calendar == null) ? 0 : calendar.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!super.equals(obj)) {
      return false;
    }
    if (!(obj instanceof PGTimestamp)) {
      return false;
    }
    PGTimestamp other = (PGTimestamp) obj;
    if (calendar == null) {
      if (other.calendar != null) {
        return false;
      }
    } else if (!calendar.equals(other.calendar)) {
      return false;
    }
    return true;
  }

  @Override
  public Object clone() {
    PGTimestamp clone = (PGTimestamp) super.clone();
    if (getCalendar() != null) {
      clone.setCalendar((Calendar) getCalendar().clone());
    }
    return clone;
  }
}
