/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.io.IOException;
import java.io.Serializable;
import java.time.DateTimeException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.Year;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.deserialization.AbstractRowsEventDataDeserializer;
import com.github.shyiko.mysql.binlog.event.deserialization.DeleteRowsEventDataDeserializer;
import com.github.shyiko.mysql.binlog.event.deserialization.UpdateRowsEventDataDeserializer;
import com.github.shyiko.mysql.binlog.event.deserialization.WriteRowsEventDataDeserializer;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;

import io.debezium.DebeziumException;
import io.debezium.config.CommonConnectorConfig.EventProcessingFailureHandlingMode;

/**
 * Custom deserializers for the MySQL Binlog Client library.
 * <p>
 * A few of the {@link AbstractRowsEventDataDeserializer MySQL Binlog Client row deserializers} convert MySQL raw row data into
 * {@link java.sql.Date}, {@link java.sql.Time}, and {@link java.sql.Timestamp} values using {@link Calendar} instances (and thus
 * implicitly use the local timezone to calculate the milliseconds past epoch. Rather than perform this conversion, we prefer to
 * convert the raw MySQL row values directly into {@link LocalDate}, {@link LocalTime}, {@link LocalDateTime}, and
 * {@link OffsetDateTime}.
 * <p>
 * Unfortunately, all of the methods used to deserialize individual values are defined on the
 * {@link AbstractRowsEventDataDeserializer} base class, and inherited by the {@link DeleteRowsEventDataDeserializer},
 * {@link UpdateRowsEventDataDeserializer}, and {@link WriteRowsEventDataDeserializer} subclasses. Since we can't provide
 * a new base class, the simplest way to override these methods is to subclass each of these 3 subclasses and override the
 * methods on all 3 classes. It's ugly, but it works.
 * <p>
 * See the <a href="https://dev.mysql.com/doc/refman/5.0/en/datetime.html">MySQL Date Time</a> documentation.
 *
 * @author Randall Hauch
 */
public class RowDeserializers {

    private static final Logger LOGGER = LoggerFactory.getLogger(RowDeserializers.class);

    /**
     * A specialization of {@link DeleteRowsEventDataDeserializer} that converts MySQL {@code DATE}, {@code TIME},
     * {@code DATETIME}, and {@code TIMESTAMP} values to {@link LocalDate}, {@link LocalTime}, {@link LocalDateTime}, and
     * {@link OffsetDateTime} objects, respectively.
     */
    public static class DeleteRowsDeserializer extends DeleteRowsEventDataDeserializer {
        private EventProcessingFailureHandlingMode eventProcessingFailureHandlingMode;

        public DeleteRowsDeserializer(Map<Long, TableMapEventData> tableMapEventByTableId,
                                      EventProcessingFailureHandlingMode eventProcessingFailureHandlingMode) {
            super(tableMapEventByTableId);
            this.eventProcessingFailureHandlingMode = eventProcessingFailureHandlingMode;
        }

        @Override
        protected Serializable deserializeString(int length, ByteArrayInputStream inputStream) throws IOException {
            return RowDeserializers.deserializeString(length, inputStream);
        }

        @Override
        protected Serializable deserializeVarString(int meta, ByteArrayInputStream inputStream) throws IOException {
            return RowDeserializers.deserializeVarString(meta, inputStream);
        }

        @Override
        protected Serializable deserializeDate(ByteArrayInputStream inputStream) throws IOException {
            return RowDeserializers.deserializeDate(inputStream, eventProcessingFailureHandlingMode);
        }

        @Override
        protected Serializable deserializeDatetime(ByteArrayInputStream inputStream) throws IOException {
            return RowDeserializers.deserializeDatetime(inputStream, eventProcessingFailureHandlingMode);
        }

        @Override
        protected Serializable deserializeDatetimeV2(int meta, ByteArrayInputStream inputStream) throws IOException {
            return RowDeserializers.deserializeDatetimeV2(meta, inputStream, eventProcessingFailureHandlingMode);
        }

        @Override
        protected Serializable deserializeTimeV2(int meta, ByteArrayInputStream inputStream) throws IOException {
            return RowDeserializers.deserializeTimeV2(meta, inputStream);
        }

        @Override
        protected Serializable deserializeTime(ByteArrayInputStream inputStream) throws IOException {
            return RowDeserializers.deserializeTime(inputStream);
        }

        @Override
        protected Serializable deserializeTimestamp(ByteArrayInputStream inputStream) throws IOException {
            return RowDeserializers.deserializeTimestamp(inputStream);
        }

        @Override
        protected Serializable deserializeTimestampV2(int meta, ByteArrayInputStream inputStream) throws IOException {
            return RowDeserializers.deserializeTimestampV2(meta, inputStream);
        }

        @Override
        protected Serializable deserializeYear(ByteArrayInputStream inputStream) throws IOException {
            return RowDeserializers.deserializeYear(inputStream);
        }
    }

    /**
     * A specialization of {@link UpdateRowsEventDataDeserializer} that converts MySQL {@code DATE}, {@code TIME},
     * {@code DATETIME}, and {@code TIMESTAMP} values to {@link LocalDate}, {@link LocalTime}, {@link LocalDateTime}, and
     * {@link OffsetDateTime} objects, respectively.
     */
    public static class UpdateRowsDeserializer extends UpdateRowsEventDataDeserializer {
        private EventProcessingFailureHandlingMode eventProcessingFailureHandlingMode;

        public UpdateRowsDeserializer(Map<Long, TableMapEventData> tableMapEventByTableId,
                                      EventProcessingFailureHandlingMode eventProcessingFailureHandlingMode) {
            super(tableMapEventByTableId);
            this.eventProcessingFailureHandlingMode = eventProcessingFailureHandlingMode;
        }

        @Override
        protected Serializable deserializeString(int length, ByteArrayInputStream inputStream) throws IOException {
            return RowDeserializers.deserializeString(length, inputStream);
        }

        @Override
        protected Serializable deserializeVarString(int meta, ByteArrayInputStream inputStream) throws IOException {
            return RowDeserializers.deserializeVarString(meta, inputStream);
        }

        @Override
        protected Serializable deserializeDate(ByteArrayInputStream inputStream) throws IOException {
            return RowDeserializers.deserializeDate(inputStream, eventProcessingFailureHandlingMode);
        }

        @Override
        protected Serializable deserializeDatetime(ByteArrayInputStream inputStream) throws IOException {
            return RowDeserializers.deserializeDatetime(inputStream, eventProcessingFailureHandlingMode);
        }

        @Override
        protected Serializable deserializeDatetimeV2(int meta, ByteArrayInputStream inputStream) throws IOException {
            return RowDeserializers.deserializeDatetimeV2(meta, inputStream, eventProcessingFailureHandlingMode);
        }

        @Override
        protected Serializable deserializeTimeV2(int meta, ByteArrayInputStream inputStream) throws IOException {
            return RowDeserializers.deserializeTimeV2(meta, inputStream);
        }

        @Override
        protected Serializable deserializeTime(ByteArrayInputStream inputStream) throws IOException {
            return RowDeserializers.deserializeTime(inputStream);
        }

        @Override
        protected Serializable deserializeTimestamp(ByteArrayInputStream inputStream) throws IOException {
            return RowDeserializers.deserializeTimestamp(inputStream);
        }

        @Override
        protected Serializable deserializeTimestampV2(int meta, ByteArrayInputStream inputStream) throws IOException {
            return RowDeserializers.deserializeTimestampV2(meta, inputStream);
        }

        @Override
        protected Serializable deserializeYear(ByteArrayInputStream inputStream) throws IOException {
            return RowDeserializers.deserializeYear(inputStream);
        }
    }

    /**
     * A specialization of {@link WriteRowsEventDataDeserializer} that converts MySQL {@code DATE}, {@code TIME},
     * {@code DATETIME}, and {@code TIMESTAMP} values to {@link LocalDate}, {@link LocalTime}, {@link LocalDateTime}, and
     * {@link OffsetDateTime} objects, respectively.
     */
    public static class WriteRowsDeserializer extends WriteRowsEventDataDeserializer {
        private EventProcessingFailureHandlingMode eventProcessingFailureHandlingMode;

        public WriteRowsDeserializer(Map<Long, TableMapEventData> tableMapEventByTableId,
                                     EventProcessingFailureHandlingMode eventProcessingFailureHandlingMode) {
            super(tableMapEventByTableId);
            this.eventProcessingFailureHandlingMode = eventProcessingFailureHandlingMode;
        }

        @Override
        protected Serializable deserializeString(int length, ByteArrayInputStream inputStream) throws IOException {
            return RowDeserializers.deserializeString(length, inputStream);
        }

        @Override
        protected Serializable deserializeVarString(int meta, ByteArrayInputStream inputStream) throws IOException {
            return RowDeserializers.deserializeVarString(meta, inputStream);
        }

        @Override
        protected Serializable deserializeDate(ByteArrayInputStream inputStream) throws IOException {
            return RowDeserializers.deserializeDate(inputStream, eventProcessingFailureHandlingMode);
        }

        @Override
        protected Serializable deserializeDatetime(ByteArrayInputStream inputStream) throws IOException {
            return RowDeserializers.deserializeDatetime(inputStream, eventProcessingFailureHandlingMode);
        }

        @Override
        protected Serializable deserializeDatetimeV2(int meta, ByteArrayInputStream inputStream) throws IOException {
            return RowDeserializers.deserializeDatetimeV2(meta, inputStream, eventProcessingFailureHandlingMode);
        }

        @Override
        protected Serializable deserializeTimeV2(int meta, ByteArrayInputStream inputStream) throws IOException {
            return RowDeserializers.deserializeTimeV2(meta, inputStream);
        }

        @Override
        protected Serializable deserializeTime(ByteArrayInputStream inputStream) throws IOException {
            return RowDeserializers.deserializeTime(inputStream);
        }

        @Override
        protected Serializable deserializeTimestamp(ByteArrayInputStream inputStream) throws IOException {
            return RowDeserializers.deserializeTimestamp(inputStream);
        }

        @Override
        protected Serializable deserializeTimestampV2(int meta, ByteArrayInputStream inputStream) throws IOException {
            return RowDeserializers.deserializeTimestampV2(meta, inputStream);
        }

        @Override
        protected Serializable deserializeYear(ByteArrayInputStream inputStream) throws IOException {
            return RowDeserializers.deserializeYear(inputStream);
        }
    }

    private static final int MASK_10_BITS = (1 << 10) - 1;
    private static final int MASK_6_BITS = (1 << 6) - 1;

    /**
     * Converts a MySQL string to a {@code byte[]}.
     *
     * @param length the number of bytes used to store the length of the string
     * @param inputStream the binary stream containing the raw binlog event data for the value
     * @return the {@code byte[]} object
     * @throws IOException if there is an error reading from the binlog event data
     */
    protected static Serializable deserializeString(int length, ByteArrayInputStream inputStream) throws IOException {
        // charset is not present in the binary log (meaning there is no way to distinguish between CHAR / BINARY)
        // as a result - return byte[] instead of an actual String
        int stringLength = length < 256 ? inputStream.readInteger(1) : inputStream.readInteger(2);
        return inputStream.read(stringLength);
    }

    /**
     * Converts a MySQL string to a {@code byte[]}.
     *
     * @param meta the {@code meta} value containing the number of bytes in the length field
     * @param inputStream the binary stream containing the raw binlog event data for the value
     * @return the {@code byte[]} object
     * @throws IOException if there is an error reading from the binlog event data
     */
    protected static Serializable deserializeVarString(int meta, ByteArrayInputStream inputStream) throws IOException {
        int varcharLength = meta < 256 ? inputStream.readInteger(1) : inputStream.readInteger(2);
        return inputStream.read(varcharLength);
    }

    /**
     * Converts a MySQL {@code DATE} value to a {@link LocalDate}.
     * <p>
     * This method treats all <a href="http://dev.mysql.com/doc/refman/8.2/en/date-and-time-types.html">zero values</a>
     * for {@code DATE} columns as NULL, since they cannot be accurately represented as valid {@link LocalDate} objects.
     *
     * @param inputStream the binary stream containing the raw binlog event data for the value
     * @return the {@link LocalDate} object
     * @throws IOException if there is an error reading from the binlog event data
     */
    protected static Serializable deserializeDate(ByteArrayInputStream inputStream,
                                                  EventProcessingFailureHandlingMode eventProcessingFailureHandlingMode)
            throws IOException {
        int value = inputStream.readInteger(3);
        int day = value % 32; // 1-based day of the month
        value >>>= 5;
        int month = value % 16; // 1-based month number
        int year = value >> 4;
        if (year == 0 || month == 0 || day == 0) {
            return null;
        }
        try {
            return LocalDate.of(year, month, day);
        }
        catch (DateTimeException e) {
            return handleException(eventProcessingFailureHandlingMode, "date", e, LocalDate.EPOCH);
        }
    }

    /**
     * Converts a MySQL {@code TIME} value <em>without fractional seconds</em> to a {@link java.time.Duration}.
     *
     * @param inputStream the binary stream containing the raw binlog event data for the value
     * @return the {@link LocalTime} object
     * @throws IOException if there is an error reading from the binlog event data
     */
    protected static Serializable deserializeTime(ByteArrayInputStream inputStream) throws IOException {
        // Times are stored as an integer as `HHMMSS`, so we need to split out the digits ...
        int value = inputStream.readInteger(3);
        int[] split = split(value, 100, 3);
        int hours = split[2];
        int minutes = split[1];
        int seconds = split[0];
        return Duration.ofHours(hours).plusMinutes(minutes).plusSeconds(seconds);
    }

    /**
     * Converts a MySQL {@code TIME} value <em>with fractional seconds</em> to a {@link java.time.Duration}.
     *
     * @param meta the {@code meta} value containing the fractional second precision, or {@code fsp}
     * @param inputStream the binary stream containing the raw binlog event data for the value
     * @return the {@link java.time.Duration} object
     * @throws IOException if there is an error reading from the binlog event data
     */
    protected static Serializable deserializeTimeV2(int meta, ByteArrayInputStream inputStream) throws IOException {
        /*
         * (in big endian)
         *
         * 1 bit sign (1= non-negative, 0= negative)
         * 1 bit unused (reserved for future extensions)
         * 10 bits hour (0-838)
         * 6 bits minute (0-59)
         * 6 bits second (0-59)
         *
         * (3 bytes in total)
         *
         * + fractional-seconds storage (size depends on meta)
         */
        long time = bigEndianLong(inputStream.read(3), 0, 3);
        boolean is_negative = bitSlice(time, 0, 1, 24) == 0;
        int hours = bitSlice(time, 2, 10, 24);
        int minutes = bitSlice(time, 12, 6, 24);
        int seconds = bitSlice(time, 18, 6, 24);
        int nanoSeconds;
        if (is_negative) { // mysql binary arithmetic for negative encoded values
            hours = ~hours & MASK_10_BITS;
            hours = hours & ~(1 << 10); // unset sign bit
            minutes = ~minutes & MASK_6_BITS;
            minutes = minutes & ~(1 << 6); // unset sign bit
            seconds = ~seconds & MASK_6_BITS;
            seconds = seconds & ~(1 << 6); // unset sign bit
            nanoSeconds = deserializeFractionalSecondsInNanosNegative(meta, inputStream);
            if (nanoSeconds == 0 && seconds < 59) { // weird java Duration behavior
                ++seconds;
            }
            hours = -hours;
            minutes = -minutes;
            seconds = -seconds;
            nanoSeconds = -nanoSeconds;
        }
        else {
            nanoSeconds = deserializeFractionalSecondsInNanos(meta, inputStream);
        }
        return Duration.ofHours(hours).plusMinutes(minutes).plusSeconds(seconds).plusNanos(nanoSeconds);
    }

    /**
     * Converts a MySQL {@code DATETIME} value <em>without fractional seconds</em> to a {@link LocalDateTime}.
     * <p>
     * This method treats all <a href="http://dev.mysql.com/doc/refman/8.2/en/date-and-time-types.html">zero values</a>
     * for {@code DATETIME} columns as NULL, since they cannot be accurately represented as valid {@link LocalDateTime} objects.
     *
     * @param inputStream the binary stream containing the raw binlog event data for the value
     * @return the {@link LocalDateTime} object
     * @throws IOException if there is an error reading from the binlog event data
     */
    protected static Serializable deserializeDatetime(ByteArrayInputStream inputStream,
                                                      EventProcessingFailureHandlingMode eventProcessingFailureHandlingMode)
            throws IOException {
        int[] split = split(inputStream.readLong(8), 100, 6);
        int year = split[5];
        int month = split[4]; // 1-based month number
        int day = split[3]; // 1-based day of the month
        int hours = split[2];
        int minutes = split[1];
        int seconds = split[0];
        int nanoOfSecond = 0; // This version does not support fractional seconds
        if (year == 0 || month == 0 || day == 0) {
            return null;
        }
        try {
            return LocalDateTime.of(year, month, day, hours, minutes, seconds, nanoOfSecond);
        }
        catch (DateTimeException e) {
            return handleException(eventProcessingFailureHandlingMode, "datetime", e,
                    LocalDateTime.of(LocalDate.EPOCH, LocalTime.MIDNIGHT));
        }
    }

    /**
     * Converts a MySQL {@code DATETIME} value <em>with fractional seconds</em> to a {@link LocalDateTime}.
     * <p>
     * This method treats all <a href="http://dev.mysql.com/doc/refman/8.2/en/date-and-time-types.html">zero values</a>
     * for {@code DATETIME} columns as NULL, since they cannot be accurately represented as valid {@link LocalDateTime} objects.
     *
     * @param meta the {@code meta} value containing the fractional second precision, or {@code fsp}
     * @param inputStream the binary stream containing the raw binlog event data for the value
     * @return the {@link LocalDateTime} object
     * @throws IOException if there is an error reading from the binlog event data
     */
    protected static Serializable deserializeDatetimeV2(int meta, ByteArrayInputStream inputStream,
                                                        EventProcessingFailureHandlingMode eventProcessingFailureHandlingMode)
            throws IOException {
        /*
         * (in big endian)
         *
         * 1 bit sign (1= non-negative, 0= negative)
         * 17 bits year*13+month (year 0-9999, month 0-12)
         * 5 bits day (0-31)
         * 5 bits hour (0-23)
         * 6 bits minute (0-59)
         * 6 bits second (0-59)
         *
         * (5 bytes in total)
         *
         * + fractional-seconds storage (size depends on meta)
         */
        long datetime = bigEndianLong(inputStream.read(5), 0, 5);
        int yearMonth = bitSlice(datetime, 1, 17, 40);
        int year = yearMonth / 13;
        int month = yearMonth % 13; // 1-based month number
        int day = bitSlice(datetime, 18, 5, 40); // 1-based day of the month
        int hours = bitSlice(datetime, 23, 5, 40);
        int minutes = bitSlice(datetime, 28, 6, 40);
        int seconds = bitSlice(datetime, 34, 6, 40);
        int nanoOfSecond = deserializeFractionalSecondsInNanos(meta, inputStream);
        if (year == 0 || month == 0 || day == 0) {
            return null;
        }
        try {
            return LocalDateTime.of(year, month, day, hours, minutes, seconds, nanoOfSecond);
        }
        catch (DateTimeException e) {
            return handleException(eventProcessingFailureHandlingMode, "datetimeV2", e,
                    LocalDateTime.of(LocalDate.EPOCH, LocalTime.MIDNIGHT));
        }
    }

    /**
     * Converts a MySQL {@code TIMESTAMP} value <em>without fractional seconds</em> to a {@link OffsetDateTime}.
     * MySQL stores the {@code TIMESTAMP} values as seconds past epoch in UTC, but the resulting {@link OffsetDateTime} will
     * be in the local timezone.
     *
     * @param inputStream the binary stream containing the raw binlog event data for the value
     * @return the {@link OffsetDateTime} object
     * @throws IOException if there is an error reading from the binlog event data
     */
    protected static Serializable deserializeTimestamp(ByteArrayInputStream inputStream) throws IOException {
        long epochSecond = inputStream.readLong(4);
        int nanoSeconds = 0; // no fractional seconds
        return ZonedDateTime.ofInstant(Instant.ofEpochSecond(epochSecond, nanoSeconds), ZoneOffset.UTC);
    }

    /**
     * Converts a MySQL {@code TIMESTAMP} value <em>with fractional seconds</em> to a {@link OffsetDateTime}.
     * MySQL stores the {@code TIMESTAMP} values as seconds + fractional seconds past epoch in UTC, but the resulting
     * {@link OffsetDateTime} will be in the local timezone.
     *
     * @param meta the {@code meta} value containing the fractional second precision, or {@code fsp}
     * @param inputStream the binary stream containing the raw binlog event data for the value
     * @return the {@link OffsetDateTime} object
     * @throws IOException if there is an error reading from the binlog event data
     */
    protected static Serializable deserializeTimestampV2(int meta, ByteArrayInputStream inputStream) throws IOException {
        long epochSecond = bigEndianLong(inputStream.read(4), 0, 4);
        int nanoSeconds = deserializeFractionalSecondsInNanos(meta, inputStream);
        return ZonedDateTime.ofInstant(Instant.ofEpochSecond(epochSecond, nanoSeconds), ZoneOffset.UTC);
    }

    /**
     * Converts a MySQL {@code YEAR} value to a {@link Year} object.
     *
     * @param inputStream the binary stream containing the raw binlog event data for the value
     * @return the {@link Year} object
     * @throws IOException if there is an error reading from the binlog event data
     */
    protected static Serializable deserializeYear(ByteArrayInputStream inputStream) throws IOException {
        return Year.of(1900 + inputStream.readInteger(1));
    }

    /**
     * Split the integer into multiple integers.
     * <p>
     * We can't use/access the private {@code split} method in the {@link AbstractRowsEventDataDeserializer} class, so we
     * replicate it here. Note the original is licensed under the same Apache Software License 2.0 as Debezium.
     *
     * @param value the long value
     * @param divider the value used to separate the individual values (e.g., 10 to separate each digit into a separate value,
     *            100 to separate each pair of digits into a separate value, 1000 to separate each 3 digits into a separate value,
     *            etc.)
     * @param length the expected length of the integer array
     * @return the integer values
     * @author <a href="mailto:stanley.shyiko@gmail.com">Stanley Shyiko</a>
     */
    protected static int[] split(long value, int divider, int length) {
        int[] result = new int[length];
        for (int i = 0; i < length - 1; i++) {
            result[i] = (int) (value % divider);
            value /= divider;
        }
        result[length - 1] = (int) value;
        return result;
    }

    /**
     * Read a big-endian long value.
     * <p>
     * We can't use/access the private {@code bigEndianLong} method in the {@link AbstractRowsEventDataDeserializer} class, so
     * we replicate it here. Note the original is licensed under the same Apache Software License 2.0 as Debezium.
     *
     * @param bytes the bytes containing the big-endian representation of the value
     * @param offset the offset within the {@code bytes} byte array where the value starts
     * @param length the length of the byte representation within the {@code bytes} byte array
     * @return the long value
     * @author <a href="mailto:stanley.shyiko@gmail.com">Stanley Shyiko</a>
     */
    protected static long bigEndianLong(byte[] bytes, int offset, int length) {
        long result = 0;
        for (int i = offset; i < (offset + length); i++) {
            byte b = bytes[i];
            result = (result << 8) | (b >= 0 ? (int) b : (b + 256));
        }
        return result;
    }

    /**
     * Slice an integer out of a portion of long value.
     * <p>
     * We can't use/access the private {@code bitSlice} method in the {@link AbstractRowsEventDataDeserializer} class, so
     * we replicate it here. Note the original is licensed under the same Apache Software License 2.0 as Debezium.
     *
     * @param value the long containing the integer encoded within it
     * @param bitOffset the number of bits where the integer value starts
     * @param numberOfBits the number of bits in the integer value
     * @param payloadSize the total number of bits used in the {@code value}
     * @return the integer value
     */
    protected static int bitSlice(long value, int bitOffset, int numberOfBits, int payloadSize) {
        long result = value >> payloadSize - (bitOffset + numberOfBits);
        return (int) (result & ((1 << numberOfBits) - 1));
    }

    /**
     * Read the binary input stream to obtain the number of nanoseconds given the <em>fractional seconds precision</em>, or
     * <em>fsp</em>.
     * <p>
     * We can't use/access the {@code deserializeFractionalSeconds} method in the {@link AbstractRowsEventDataDeserializer} class,
     * so we replicate it here with modifications to support nanoseconds rather than microseconds.
     * Note the original is licensed under the same Apache Software License 2.0 as Debezium.
     *
     * @param fsp the fractional seconds precision describing the number of digits precision used to store the fractional seconds
     *            (e.g., 1 for storing tenths of a second, 2 for storing hundredths, 3 for storing milliseconds, etc.)
     * @param inputStream the binary data stream
     * @return the number of nanoseconds
     * @throws IOException if there is an error reading from the binlog event data
     */
    protected static int deserializeFractionalSecondsInNanos(int fsp, ByteArrayInputStream inputStream) throws IOException {
        // Calculate the number of bytes to read, which is
        // '1' when fsp=(1,2) -- 7
        // '2' when fsp=(3,4) and -- 12
        // '3' when fsp=(5,6) -- 21
        int length = (fsp + 1) / 2;
        if (length > 0) {
            long fraction = bigEndianLong(inputStream.read(length), 0, length);
            // Convert the fractional value (which has extra trailing digit for fsp=1,3, and 5) to nanoseconds ...
            return (int) (fraction / (0.0000001 * Math.pow(100, length - 1)));
        }
        return 0;
    }

    /**
     * Read the binary input stream to obtain the number of nanoseconds given the <em>fractional seconds precision</em>, or
     * <em>fsp</em>.
     * <p>
     * We can't use/access the {@code deserializeFractionalSeconds} method in the {@link AbstractRowsEventDataDeserializer} class,
     * so we replicate it here with modifications to support nanoseconds rather than microseconds and negative values.
     * Note the original is licensed under the same Apache Software License 2.0 as Debezium.
     *
     * @param fsp the fractional seconds precision describing the number of digits precision used to store the fractional seconds
     *            (e.g., 1 for storing tenths of a second, 2 for storing hundredths, 3 for storing milliseconds, etc.)
     * @param inputStream the binary data stream
     * @return the number of nanoseconds
     * @throws IOException if there is an error reading from the binlog event data
     */
    protected static int deserializeFractionalSecondsInNanosNegative(int fsp, ByteArrayInputStream inputStream) throws IOException {
        // Calculate the number of bytes to read, which is
        // '1' when fsp=(1,2)
        // '2' when fsp=(3,4) and
        // '3' when fsp=(5,6)
        int length = (fsp + 1) / 2;
        if (length > 0) {
            long fraction = bigEndianLong(inputStream.read(length), 0, length);
            int maskBits = 0;
            switch (length) { // mask bits according to field precision
                case 1:
                    maskBits = 8;
                    break;
                case 2:
                    maskBits = 15;
                    break;
                case 3:
                    maskBits = 20;
                    break;
                default:
                    break;
            }
            fraction = ~fraction & ((1 << maskBits) - 1);
            fraction = (fraction & ~(1 << maskBits)) + 1; // unset sign bit
            // Convert the fractional value (which has extra trailing digit for fsp=1,3, and 5) to nanoseconds ...
            return (int) (fraction / (0.0000001 * Math.pow(100, length - 1)));
        }
        return 0;
    }

    private RowDeserializers() {
    }

    private static Serializable handleException(EventProcessingFailureHandlingMode eventProcessingFailureHandlingMode,
                                                String columnType, Exception e, Serializable defaultValue) {
        if (eventProcessingFailureHandlingMode == EventProcessingFailureHandlingMode.FAIL) {
            LOGGER.error("Error while deserializing binlog data of {}: {}", columnType, e.getMessage());
            throw new DebeziumException("Error while deserializing binlog data of " + columnType + ": " + e.getMessage());
        }
        else if (eventProcessingFailureHandlingMode == EventProcessingFailureHandlingMode.WARN) {
            LOGGER.warn("Error while deserializing binlog data of {}: {}", columnType, e.getMessage());
            return defaultValue;
        }
        else {
            LOGGER.debug("Error while deserializing binlog data of {}: {}", columnType, e.getMessage());
            return defaultValue;
        }
    }
}
