/*
 * Copyright Debezium Authors.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.jdbc;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.Month;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.util.Calendar;

import org.junit.Before;
import org.junit.Test;

import static org.fest.assertions.Assertions.assertThat;

/**
 * @author Randall Hauch
 *
 */
public class TimeZoneAdapterTest {

    private TimeZoneAdapter adapter;

    @Before
    public void beforeEach() {
        adapter = TimeZoneAdapter.create()
                                 .withLocalZoneForUtilDate()
                                 .withLocalZoneForSqlDate()
                                 .withLocalZoneForSqlTime()
                                 .withLocalZoneForSqlTimestamp()
                                 .withUtcTargetZone();
    }

    @Test
    public void shouldAdaptSqlDate() {
        // '2014-09-08', '17:51:04.777', '2014-09-08 17:51:04.777', '2014-09-08 17:51:04.777'
        java.sql.Date sqlDate = createSqlDate(2014, Month.SEPTEMBER, 8);
        ZonedDateTime expectedDateInTargetTZ = ZonedDateTime.ofInstant(Instant.ofEpochMilli(sqlDate.getTime()), adapter.targetZoneId());
        ZonedDateTime zdt = adapter.toZonedDateTime(sqlDate);
        // The date should match ...
        LocalDate date = zdt.toLocalDate();
        assertThat(date.getYear()).isEqualTo(2014);
        assertThat(date.getMonth()).isEqualTo(Month.SEPTEMBER);
        assertThat(date.getDayOfMonth()).isEqualTo(expectedDateInTargetTZ.get(ChronoField.DAY_OF_MONTH));
        // There should be no time component ...
        LocalTime time = zdt.toLocalTime();
        assertThat(time.getHour()).isEqualTo(0);
        assertThat(time.getMinute()).isEqualTo(0);
        assertThat(time.getSecond()).isEqualTo(0);
        assertThat(time.getNano()).isEqualTo(0);
        // The zone should be our target ...
        assertThat(zdt.getZone()).isEqualTo(adapter.targetZoneId());
    }

    @Test
    public void shouldAdaptSqlTime() {
        // '17:51:04.777'
        java.sql.Time sqlTime = createSqlTime(17, 51, 04, 777);
        ZonedDateTime zdt = adapter.toZonedDateTime(sqlTime);
        // The date should be at epoch ...
        LocalDate date = zdt.toLocalDate();
        assertThat(date.getYear()).isEqualTo(1970);
        assertThat(date.getMonth()).isEqualTo(Month.JANUARY);
        assertThat(date.getDayOfMonth()).isEqualTo(1);
        // The time should match exactly ...
        LocalTime time = zdt.toLocalTime();
        assertThat(time.getHour()).isEqualTo(17);
        assertThat(time.getMinute()).isEqualTo(51);
        assertThat(time.getSecond()).isEqualTo(4);
        assertThat(time.getNano()).isEqualTo(777 * 1000 * 1000);
        // The zone should be our target ...
        assertThat(zdt.getZone()).isEqualTo(adapter.targetZoneId());
    }

    @Test
    public void shouldAdaptSqlTimestamp() {
        adapter = TimeZoneAdapter.create()
                                 .withLocalZoneForSqlTimestamp()
                                 .withUtcTargetZone();

        // '2014-09-08 17:51:04.777'
        // This technique creates the timestamp using the milliseconds from epoch in terms of the local zone ...
        java.sql.Timestamp sqlTimestamp = createSqlTimestamp(2014, Month.SEPTEMBER, 8, 17, 51, 04, 777);
        ZonedDateTime zdt = adapter.toZonedDateTime(sqlTimestamp);
        // The date should match ...
        LocalDate date = zdt.toLocalDate();
        assertThat(date.getYear()).isEqualTo(2014);
        assertThat(date.getMonth()).isEqualTo(Month.SEPTEMBER);
        assertThat(date.getDayOfMonth()).isEqualTo(8);
        // The time should match exactly ...
        LocalTime time = zdt.toLocalTime();
        assertThat(time.getHour()).isEqualTo(17);
        assertThat(time.getMinute()).isEqualTo(51);
        assertThat(time.getSecond()).isEqualTo(4);
        assertThat(time.getNano()).isEqualTo(777 * 1000 * 1000);
        // The zone should be our target ...
        assertThat(zdt.getZone()).isEqualTo(adapter.targetZoneId());
    }

    @Test
    public void shouldAdaptSqlTimestampViaSecondsAndMillis() {
        adapter = TimeZoneAdapter.create()
                                 .withUtcZoneForSqlTimestamp()
                                 .withUtcTargetZone();

        // '2014-09-08 17:51:04.777'
        // This technique creates the timestamp using the milliseconds from epoch in terms of UTC ...
        java.sql.Timestamp sqlTimestamp = createSqlTimestamp(1410198664L, 777);
        ZonedDateTime zdt = adapter.toZonedDateTime(sqlTimestamp);
        // The date should match ...
        LocalDate date = zdt.toLocalDate();
        assertThat(date.getYear()).isEqualTo(2014);
        assertThat(date.getMonth()).isEqualTo(Month.SEPTEMBER);
        assertThat(date.getDayOfMonth()).isEqualTo(8);
        // The time should match exactly ...
        LocalTime time = zdt.toLocalTime();
        assertThat(time.getHour()).isEqualTo(17);
        assertThat(time.getMinute()).isEqualTo(51);
        assertThat(time.getSecond()).isEqualTo(4);
        assertThat(time.getNano()).isEqualTo(777 * 1000 * 1000);
        // The zone should be our target ...
        assertThat(zdt.getZone()).isEqualTo(adapter.targetZoneId());
    }

    @Test
    public void shouldAdaptUtilDate() {
        // '2014-09-08 17:51:04.777'
        java.util.Date utilDate = createUtilDate(2014, Month.SEPTEMBER, 8, 17, 51, 04, 777);
        ZonedDateTime zdt = adapter.toZonedDateTime(utilDate);
        // The date should match ...
        LocalDate date = zdt.toLocalDate();
        assertThat(date.getYear()).isEqualTo(2014);
        assertThat(date.getMonth()).isEqualTo(Month.SEPTEMBER);
        assertThat(date.getDayOfMonth()).isEqualTo(8);
        // The time should match exactly ...
        LocalTime time = zdt.toLocalTime();
        assertThat(time.getHour()).isEqualTo(17);
        assertThat(time.getMinute()).isEqualTo(51);
        assertThat(time.getSecond()).isEqualTo(4);
        assertThat(time.getNano()).isEqualTo(777 * 1000 * 1000);
        // The zone should be our target ...
        assertThat(zdt.getZone()).isEqualTo(adapter.targetZoneId());
    }

    protected java.sql.Date createSqlDate(int year, Month month, int dayOfMonth) {
        Calendar cal = Calendar.getInstance();
        cal.clear();
        cal.set(Calendar.YEAR, year);
        cal.set(Calendar.MONTH, month.getValue() - 1);
        cal.set(Calendar.DATE, dayOfMonth);
        return new java.sql.Date(cal.getTimeInMillis());
    }

    protected java.sql.Time createSqlTime(int hourOfDay, int minute, int second, int milliseconds) {
        Calendar c = Calendar.getInstance();
        c.clear();
        c.set(Calendar.HOUR_OF_DAY, hourOfDay);
        c.set(Calendar.MINUTE, minute);
        c.set(Calendar.SECOND, second);
        c.set(Calendar.MILLISECOND, milliseconds);
        return new java.sql.Time(c.getTimeInMillis());
    }

    /**
     * This sets the calendar via the milliseconds past epoch, and this behaves differently than actually setting the various
     * components of the calendar (see {@link #createSqlTimestamp(int, Month, int, int, int, int, int)}). This is how the
     * MySQL Binary Log client library creates timestamps (v2).
     * 
     * @param secondsFromEpoch the number of seconds since epoch
     * @param millis the number of milliseconds
     * @return the SQL timestamp
     */
    protected java.sql.Timestamp createSqlTimestamp(long secondsFromEpoch, int millis) {
        Calendar c = Calendar.getInstance();
        c.setTimeInMillis(secondsFromEpoch * 1000);
        c.set(Calendar.MILLISECOND, millis);
        return new java.sql.Timestamp(c.getTimeInMillis());
    }

    protected java.sql.Timestamp createSqlTimestamp(int year, Month month, int dayOfMonth, int hourOfDay, int minute, int second,
                                                    int milliseconds) {
        Calendar c = Calendar.getInstance();
        c.set(Calendar.YEAR, year);
        c.set(Calendar.MONTH, month.getValue() - 1);
        c.set(Calendar.DAY_OF_MONTH, dayOfMonth);
        c.set(Calendar.HOUR_OF_DAY, hourOfDay);
        c.set(Calendar.MINUTE, minute);
        c.set(Calendar.SECOND, second);
        c.set(Calendar.MILLISECOND, milliseconds);
        return new java.sql.Timestamp(c.getTimeInMillis());
    }

    protected java.util.Date createUtilDate(int year, Month month, int dayOfMonth, int hourOfDay, int minute, int second,
                                            int milliseconds) {
        Calendar c = Calendar.getInstance();
        c.set(Calendar.YEAR, year);
        c.set(Calendar.MONTH, month.getValue() - 1);
        c.set(Calendar.DAY_OF_MONTH, dayOfMonth);
        c.set(Calendar.HOUR_OF_DAY, hourOfDay);
        c.set(Calendar.MINUTE, minute);
        c.set(Calendar.SECOND, second);
        c.set(Calendar.MILLISECOND, milliseconds);
        return c.getTime();
    }

}
