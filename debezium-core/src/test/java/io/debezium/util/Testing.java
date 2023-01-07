/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

import org.junit.Before;

import io.debezium.util.Stopwatch.Statistics;
import io.debezium.util.Stopwatch.StopwatchSet;

/**
 * A set of utility methods for test cases.
 *
 * @author Randall Hauch
 */
public interface Testing {

    @Before
    default void resetBeforeEachTest() {
        Print.enabled = false;
        Debug.enabled = false;
        Timer.reset();
    }

    final class Print {
        private static boolean enabled = false;

        public static void enable() {
            enabled = true;
        }

        public static void disable() {
            enabled = false;
        }

        public static boolean isEnabled() {
            return enabled;
        }
    }

    static void print(Object message) {
        if (message != null && Print.enabled) {
            System.out.println(message);
        }
    }

    static void print(int length, String leader, Object message) {
        if (message != null && Print.enabled) {
            int len = leader.length();
            System.out.print(leader);
            if (len < length) {
                for (int i = len; i != length; ++i) {
                    System.out.print(" ");
                }
            }
            System.out.println(message);
        }
    }

    final class Debug {
        private static boolean enabled = false;

        public static void enable() {
            enabled = true;
        }

        public static void disable() {
            enabled = false;
        }

        public static boolean isEnabled() {
            return enabled;
        }
    }

    static void debug(Object message) {
        if (message != null && Debug.enabled) {
            System.out.println(message);
        }
    }

    static void printError(Object message) {
        if (message != null) {
            System.err.println(message);
        }
    }

    static void printError(Throwable throwable) {
        if (throwable != null) {
            throwable.printStackTrace();
        }
    }

    static void printError(String message, Throwable throwable) {
        printError(message);
        printError(throwable);
    }

    /**
     * Network-related utility methods.
     */
    interface Network {
        /**
         * Find a port that is available. This method starts a {@link ServerSocket} and obtains the port on which the socket is
         * listening, and then shuts down the socket so the port becomes available.
         *
         * @return the number of the now-available port
         */
        static int getAvailablePort() {
            return IoUtil.getAvailablePort();
        }

    }

    /**
     * File system utility methods.
     */
    interface Files {

        String DBZ_TEST_DATA_DIR_ENV_VAR_NAME = "DBZ_TEST_DATA_DIR";
        String DBZ_TEST_DATA_DIR_SYSTEM_PROPERTY_KEY = "dbz.test.data.dir";

        String DATA_DIR = determineTestDataDir();

        static String determineTestDataDir() {
            String value = System.getProperty(DBZ_TEST_DATA_DIR_SYSTEM_PROPERTY_KEY);
            if (value != null && (value = value.trim()).length() > 0) {
                return value;
            }

            value = System.getenv(DBZ_TEST_DATA_DIR_ENV_VAR_NAME);
            if (value != null && (value = value.trim()).length() > 0) {
                return value;
            }

            return "target/data"; // default value
        }

        /**
         * Obtain an InputStream to a named resource on the given classpath.
         *
         * @param pathOnClasspath the path of the resource on the classpath
         * @param testClass the test class, used for accessing the class loader
         * @return the string representation
         */
        static InputStream readResourceAsStream(String pathOnClasspath, Class<?> testClass) {
            InputStream stream = testClass.getClassLoader().getResourceAsStream(pathOnClasspath);
            assertThat(stream).isNotNull();
            return stream;
        }

        /**
         * Obtain an InputStream to a named resource on the classpath used to load this {@link Testing} class.
         *
         * @param pathOnClasspath the path of the resource on the classpath
         * @return the string representation
         */
        static InputStream readResourceAsStream(String pathOnClasspath) {
            return readResourceAsStream(pathOnClasspath, Testing.class);
        }

        /**
         * Read a classpath resource into a string.
         *
         * @param pathOnClasspath the path of the resource on the classpath
         * @return the string representation
         */
        static String readResourceAsString(String pathOnClasspath) {
            try (InputStream stream = readResourceAsStream(pathOnClasspath)) {
                return IoUtil.read(stream);
            }
            catch (IOException e) {
                fail("Unable to read '" + pathOnClasspath + "'", e);
                return null;
            }
        }

        /**
         * Create a directory within the test data directory at the given relative path.
         *
         * @param relativePath the path of the directory within the test data directory; may not be null
         * @return the reference to the existing readable and writable directory
         */
        static File createTestingDirectory(String relativePath) {
            Path dirPath = createTestingPath(relativePath);
            return IoUtil.createDirectory(dirPath);
        }

        /**
         * Returns the name of the directory where tests are storing their data. Default value is {@code 'target/data'}.
         * This value can be overridden by value of the {@code DBZ_TEST_DATA_DIR} system or environment variable.
         */
        static String dataDir() {
            return DATA_DIR;
        }

        /**
         * Create a randomly-named file within the test data directory.
         *
         * @return the reference to the existing readable and writable file
         */
        static File createTestingFile() {
            return createTestingFile(UUID.randomUUID().toString());
        }

        /**
         * Create a file within the test data directory at the given relative path.
         *
         * @param relativePath the path of the file within the test data directory; may not be null
         * @return the reference to the existing readable and writable file
         */
        static File createTestingFile(String relativePath) {
            Path path = createTestingPath(relativePath);
            return IoUtil.createFile(path);
        }

        /**
         * Create a file within the test data directory at the given relative path.
         *
         * @param relativePath the path of the file within the test data directory; may not be null
         * @return the reference to the existing readable and writable file
         */
        static File createTestingFile(Path relativePath) {
            Path path = relativePath.toAbsolutePath();
            if (!inTestDataDir(path)) {
                throw new IllegalStateException("Expecting '" + relativePath + "' to be within the testing directory");
            }
            return IoUtil.createFile(path);
        }

        /**
         * Create the path to a file within the test data directory at the given relative path.
         *
         * @param relativePath the path of the file within the test data directory; may not be null
         * @return the reference to the existing readable and writable file
         */
        static Path createTestingPath(String relativePath) {
            return Paths.get(dataDir(), relativePath).toAbsolutePath();
        }

        /**
         * Create a directory within the test data directory at the given relative path.
         *
         * @param relativePath the path of the directory within the test data directory; may not be null
         * @param removeExistingContent true if any existing content should be removed
         * @return the reference to the existing readable and writable directory
         * @throws IOException if there is a problem deleting the files at this path
         */
        static File createTestingDirectory(String relativePath, boolean removeExistingContent) throws IOException {
            Path dirPath = createTestingPath(relativePath);
            return IoUtil.createDirectory(dirPath, removeExistingContent);
        }

        /**
         * A method that will delete a file or folder only if it is within the 'target' directory (for safety).
         * Folders are removed recursively.
         *
         * @param path the path to the file or folder in the target directory
         */
        static void delete(String path) {
            if (path != null) {
                delete(Paths.get(path));
            }
        }

        /**
         * A method that will delete a file or folder only if it is within the 'target' directory (for safety).
         * Folders are removed recursively.
         *
         * @param fileOrFolder the file or folder in the target directory
         */
        static void delete(File fileOrFolder) {
            if (fileOrFolder != null) {
                delete(fileOrFolder.toPath());
            }
        }

        /**
         * A method that will delete a file or folder only if it is within the 'target' directory (for safety).
         * Folders are removed recursively.
         *
         * @param path the path to the file or folder in the target directory
         */
        static void delete(Path path) {
            if (path != null) {
                path = path.toAbsolutePath();
                if (inTestDataDir(path)) {
                    try {
                        IoUtil.delete(path);
                    }
                    catch (IOException e) {
                        printError("Unable to remove '" + path.toAbsolutePath() + "'", e);
                    }
                }
                else {
                    printError("Will not remove directory that is outside test data area: " + path);
                }
            }
        }

        /**
         * Verify that the supplied file or directory is within the test data directory.
         *
         * @param file the file or directory; may not be null
         * @return true if inside the test data directory, or false otherwise
         */
        static boolean inTestDataDir(File file) {
            return inTestDataDir(file.toPath());
        }

        /**
         * Verify that the supplied file or directory is within the test data directory.
         *
         * @param path the path to the file or directory; may not be null
         * @return true if inside the test data directory, or false otherwise
         */
        static boolean inTestDataDir(Path path) {
            Path target = FileSystems.getDefault().getPath(dataDir()).toAbsolutePath();
            return path.toAbsolutePath().startsWith(target);
        }
    }

    default Statistics once(InterruptableFunction runnable) throws InterruptedException {
        return Timer.time(null, 1, runnable, null);
    }

    default <T> Statistics once(Callable<T> runnable, Consumer<T> cleanup) throws InterruptedException {
        return Timer.time(null, 1, runnable, cleanup);
    }

    default Statistics time(String desc, int repeat, InterruptableFunction runnable) throws InterruptedException {
        return Timer.time(desc, repeat, runnable, null);
    }

    default <T> Statistics time(String desc, int repeat, Callable<T> runnable, Consumer<T> cleanup) throws InterruptedException {
        return Timer.time(desc, repeat, runnable, cleanup);
    }

    final class Timer {
        private static Stopwatch sw = Stopwatch.accumulating();
        private static StopwatchSet sws = Stopwatch.multiple();

        public static void reset() {
            sw = Stopwatch.accumulating();
            sws = Stopwatch.multiple();
        }

        public static Statistics completionTime() {
            return sw.durations().statistics();
        }

        public static Statistics operationTimes() {
            return sws.statistics();
        }

        protected static <T> Statistics time(String desc, int repeat, Callable<T> runnable, Consumer<T> cleanup)
                throws InterruptedException {
            sw.start();
            try {
                sws.time(repeat, runnable, result -> {
                    if (cleanup != null) {
                        cleanup.accept(result);
                    }
                });
            }
            catch (Throwable t) {
                t.printStackTrace();
                fail(t.getMessage());
            }
            sw.stop();
            // if (desc != null) Testing.print(60, "Time to " + desc + ":", sw.durations().statistics().getTotalAsString());
            // Testing.print(60,"Total clock time:",sw.durations().statistics().getTotalAsString());
            // Testing.print(54,"Time to invoke the functions:",sws);
            return sw.durations().statistics();
        }

    }

    @FunctionalInterface
    interface InterruptableFunction extends Callable<Void> {
        @Override
        Void call() throws InterruptedException;
    }
}
