/*
 * Copyright Debezium Authors.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.time;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import org.junit.Ignore;
import org.junit.Test;

/**
 * @author Randall Hauch
 */
@Ignore
@SuppressWarnings("deprecation")
public class TimeTest {

    @Test
    public void test1() {
        java.util.Date dateNow = new java.util.Date();
        System.out.println("Date now:                 " + dateNow);
        System.out.println("Date now (year):          " + dateNow.getYear());
        System.out.println("Date now (month):         " + dateNow.getMonth()+1);
        System.out.println("Date now (day):           " + dateNow.getDay());
        System.out.println("Date now (hour):          " + dateNow.getHours());
        System.out.println("Date now (minute):        " + dateNow.getMinutes());
        System.out.println("Date now (second):        " + dateNow.getSeconds());
        System.out.println("Date now (nanosecond):    " + 0);

    }

    @Test
    public void test2() {
        java.util.Date dateNow = new java.util.Date();
        Instant dateInstant = dateNow.toInstant();
        System.out.println("Date now:                 " + dateNow);
        System.out.println("Instant from date now:    " + dateInstant);

        Instant a = Instant.now();
        LocalDateTime here = LocalDateTime.ofInstant(a, ZoneId.systemDefault());
        LocalDateTime there = LocalDateTime.ofInstant(a, ZoneOffset.UTC);
        Instant instantAtUtc = there.toInstant(ZoneOffset.UTC);
        int compared = here.compareTo(there);
        System.out.println("Instant:                  " + a);
        System.out.println("Here:                     " + here);
        System.out.println("UTC:                      " + there);
        System.out.println("Instant @ UTC:            " + instantAtUtc);
        System.out.println("Compared:                 " + compared);
        System.out.println("Instant millis:           " + a.toEpochMilli());

        LocalDateTime nowHere = LocalDateTime.now();
        Instant nowInstantAtUtc = nowHere.toInstant(ZoneOffset.UTC);

        System.out.println("LocalDateTime.now():               " + nowHere);
        System.out.println("Instant @ UTC                      " + nowInstantAtUtc);
        System.out.println("Instant @ UTC millis:              " + nowInstantAtUtc.toEpochMilli());
        System.out.println("LocalDateTime epoch seconds @ UTC: " + nowHere.toEpochSecond(ZoneOffset.UTC));
        System.out.println("LocalDateTime nanoseconds @ UTC:             " + nowHere.getNano());

        ZonedDateTime zdt = ZonedDateTime.of(nowHere, ZoneOffset.UTC);
        System.out.println("ZoneDateTime @ UTC                 " + zdt);
        System.out.println("ZoneDateTime epoch seconds @ UTC:  " + zdt.toEpochSecond());
        System.out.println("ZoneDateTime nanoseconds @ UTC:              " + zdt.getNano());
        System.out.println("Instant from ZonedDateTime @ UTC:  " + zdt.toInstant());
    }

}
