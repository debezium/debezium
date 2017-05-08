/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.temporal.TemporalAdjuster;

import org.junit.Test;

import static org.fest.assertions.Assertions.assertThat;

/**
 * @author Randall Hauch
 *
 */
public class MySqlValueConvertersTest {

    private static final TemporalAdjuster ADJUSTER = MySqlValueConverters::adjustTemporal;

    @Test
    public void shouldAdjustLocalDateWithTwoDigitYears() {
        assertThat(ADJUSTER.adjustInto(localDateWithYear(00))).isEqualTo(localDateWithYear(2000));
        assertThat(ADJUSTER.adjustInto(localDateWithYear(01))).isEqualTo(localDateWithYear(2001));
        assertThat(ADJUSTER.adjustInto(localDateWithYear(10))).isEqualTo(localDateWithYear(2010));
        assertThat(ADJUSTER.adjustInto(localDateWithYear(69))).isEqualTo(localDateWithYear(2069));
        assertThat(ADJUSTER.adjustInto(localDateWithYear(70))).isEqualTo(localDateWithYear(1970));
        assertThat(ADJUSTER.adjustInto(localDateWithYear(71))).isEqualTo(localDateWithYear(1971));
        assertThat(ADJUSTER.adjustInto(localDateWithYear(99))).isEqualTo(localDateWithYear(1999));
    }

    @Test
    public void shouldAdjustLocalDateTimeWithTwoDigitYears() {
        assertThat(ADJUSTER.adjustInto(localDateTimeWithYear(00))).isEqualTo(localDateTimeWithYear(2000));
        assertThat(ADJUSTER.adjustInto(localDateTimeWithYear(01))).isEqualTo(localDateTimeWithYear(2001));
        assertThat(ADJUSTER.adjustInto(localDateTimeWithYear(10))).isEqualTo(localDateTimeWithYear(2010));
        assertThat(ADJUSTER.adjustInto(localDateTimeWithYear(69))).isEqualTo(localDateTimeWithYear(2069));
        assertThat(ADJUSTER.adjustInto(localDateTimeWithYear(70))).isEqualTo(localDateTimeWithYear(1970));
        assertThat(ADJUSTER.adjustInto(localDateTimeWithYear(71))).isEqualTo(localDateTimeWithYear(1971));
        assertThat(ADJUSTER.adjustInto(localDateTimeWithYear(99))).isEqualTo(localDateTimeWithYear(1999));
    }

    @Test
    public void shouldNotAdjustLocalDateWithThreeDigitYears() {
        assertThat(ADJUSTER.adjustInto(localDateWithYear(-1))).isEqualTo(localDateWithYear(-1));
        assertThat(ADJUSTER.adjustInto(localDateWithYear(100))).isEqualTo(localDateWithYear(100));
    }
    
    @Test
    public void shouldNotAdjustLocalDateTimeWithThreeDigitYears() {
        assertThat(ADJUSTER.adjustInto(localDateTimeWithYear(-1))).isEqualTo(localDateTimeWithYear(-1));
        assertThat(ADJUSTER.adjustInto(localDateTimeWithYear(100))).isEqualTo(localDateTimeWithYear(100));
    }
    
    protected LocalDate localDateWithYear(int year) {
        return LocalDate.of(year, Month.APRIL, 4);
    }
    
    protected LocalDateTime localDateTimeWithYear(int year) {
        return LocalDateTime.of(year, Month.APRIL, 4, 0, 0, 0);
    }

}
