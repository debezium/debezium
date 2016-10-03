/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2004-2014, PostgreSQL Global Development Group
 *
 *
 *-------------------------------------------------------------------------
 */

package org.postgresql.util;

import java.sql.PreparedStatement;
import java.sql.Time;
import java.util.Calendar;

/**
 * This class augments the Java built-in Time to allow for explicit setting of the time zone.
 */
public class PGTime extends Time {
  /**
   * The serial version UID.
   */
  private static final long serialVersionUID = 3592492258676494276L;

  /**
   * The optional calendar for this time.
   */
  private Calendar calendar;

  /**
   * Constructs a <code>PGTime</code> without a time zone.
   *
   * @param time milliseconds since January 1, 1970, 00:00:00 GMT; a negative number is milliseconds
   *        before January 1, 1970, 00:00:00 GMT.
   * @see Time#Time(long)
   */
  public PGTime(long time) {
    this(time, null);
  }

  /**
   * Constructs a <code>PGTime</code> with the given calendar object. The calendar object is
   * optional. If absent, the driver will treat the time as <code>time without time zone</code>.
   * When present, the driver will treat the time as a <code>time with time zone</code> using the
   * <code>TimeZone</code> in the calendar object. Furthermore, this calendar will be used instead
   * of the calendar object passed to {@link PreparedStatement#setTime(int, Time, Calendar)}.
   *
   * @param time milliseconds since January 1, 1970, 00:00:00 GMT; a negative number is milliseconds
   *        before January 1, 1970, 00:00:00 GMT.
   * @param calendar the calendar object containing the time zone or <code>null</code>.
   * @see Time#Time(long)
   */
  public PGTime(long time, Calendar calendar) {
    super(time);
    this.setCalendar(calendar);
  }

  /**
   * Sets the calendar object for this time.
   *
   * @param calendar the calendar object or <code>null</code>.
   */
  public void setCalendar(Calendar calendar) {
    this.calendar = calendar;
  }

  /**
   * Returns the calendar object for this time.
   *
   * @return the calendar or <code>null</code>.
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
    if (!(obj instanceof PGTime)) {
      return false;
    }
    PGTime other = (PGTime) obj;
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
    PGTime clone = (PGTime) super.clone();
    if (getCalendar() != null) {
      clone.setCalendar((Calendar) getCalendar().clone());
    }
    return clone;
  }
}
