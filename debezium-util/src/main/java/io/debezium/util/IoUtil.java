/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.MalformedURLException;
import java.net.ServerSocket;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.annotation.Immutable;

/**
 * A set of utilities for more easily performing I/O.
 */
@Immutable
public class IoUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(IoUtil.class);

    /**
     * Read and return the entire contents of the supplied {@link InputStream stream}. This method always closes the stream when
     * finished reading.
     *
     * @param stream the stream to the contents; may be null
     * @return the contents, or an empty byte array if the supplied reader is null
     * @throws IOException if there is an error reading the content
     */
    public static byte[] readBytes(InputStream stream) throws IOException {
        if (stream == null) {
            return new byte[]{};
        }
        byte[] buffer = new byte[1024];
        try (ByteArrayOutputStream output = new ByteArrayOutputStream()) {
            int numRead = 0;
            while ((numRead = stream.read(buffer)) > -1) {
                output.write(buffer, 0, numRead);
            }
            output.flush();
            return output.toByteArray();
        }
    }

    /**
     * Read and return the entire contents of the supplied {@link File file}.
     *
     * @param file the file containing the contents; may be null
     * @return the contents, or an empty byte array if the supplied file is null
     * @throws IOException if there is an error reading the content
     */
    public static byte[] readBytes(File file) throws IOException {
        if (file == null) {
            return new byte[]{};
        }
        try (InputStream stream = new BufferedInputStream(new FileInputStream(file))) {
            return readBytes(stream);
        }
    }

    /**
     * Read the lines from the content of the resource file at the given path on the classpath.
     *
     * @param resourcePath the logical path to the classpath, file, or URL resource
     * @param classLoader the classloader that should be used to load the resource as a stream; may be null
     * @param clazz the class that should be used to load the resource as a stream; may be null
     * @param lineProcessor the function that this method calls for each line read from the supplied stream; may not be null
     * @throws IOException if an I/O error occurs
     */
    public static void readLines(String resourcePath, ClassLoader classLoader, Class<?> clazz, Consumer<String> lineProcessor)
            throws IOException {
        try (InputStream stream = IoUtil.getResourceAsStream(resourcePath, classLoader, clazz, null, null)) {
            IoUtil.readLines(stream, lineProcessor);
        }
    }

    /**
     * Read the lines from the supplied stream. This function completely reads the stream and therefore closes the stream.
     *
     * @param stream the stream with the contents to be read; may not be null
     * @param lineProcessor the function that this method calls for each line read from the supplied stream; may not be null
     * @throws IOException if an I/O error occurs
     */
    public static void readLines(InputStream stream, Consumer<String> lineProcessor) throws IOException {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream))) {
            String line = null;
            while ((line = reader.readLine()) != null) {
                lineProcessor.accept(line);
            }
        }
    }

    /**
     * Read the lines from the supplied stream. This function completely reads the stream and therefore closes the stream.
     *
     * @param stream the stream with the contents to be read; may not be null
     * @param lineProcessor the function that this method calls for each line read from the supplied stream; may not be null
     * @param charset the character set used to interpret the stream content
     * @throws IOException if an I/O error occurs
     */
    public static void readLines(InputStream stream, Consumer<String> lineProcessor, Charset charset) throws IOException {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream, charset))) {
            String line = null;
            while ((line = reader.readLine()) != null) {
                lineProcessor.accept(line);
            }
        }
    }

    /**
     * Read the lines from the supplied stream. This function completely reads the stream and therefore closes the stream.
     *
     * @param path path to the file with the contents to be read; may not be null
     * @param lineProcessor the function that this method calls for each line read from the supplied stream; may not be null
     * @throws IOException if an I/O error occurs
     */
    public static void readLines(Path path, Consumer<String> lineProcessor) throws IOException {
        try (Stream<String> stream = Files.lines(path)) {
            stream.forEach(lineProcessor);
        }
    }

    /**
     * Read and return the entire contents of the supplied {@link Reader}. This method always closes the reader when finished
     * reading.
     *
     * @param reader the reader of the contents; may be null
     * @return the contents, or an empty string if the supplied reader is null
     * @throws IOException if there is an error reading the content
     */
    public static String read(Reader reader) throws IOException {
        if (reader == null) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        try (Reader r = reader) {
            int numRead = 0;
            char[] buffer = new char[1024];
            while ((numRead = reader.read(buffer)) > -1) {
                sb.append(buffer, 0, numRead);
            }
        }
        return sb.toString();
    }

    /**
     * Read and return the entire contents of the supplied {@link InputStream}. This method always closes the stream when finished
     * reading.
     *
     * @param stream the streamed contents; may be null
     * @return the contents, or an empty string if the supplied stream is null
     * @throws IOException if there is an error reading the content
     */
    public static String read(InputStream stream) throws IOException {
        return stream == null ? "" : read(new InputStreamReader(stream));
    }

    /**
     * Read and return the entire contents of the supplied {@link InputStream}. This method always closes the stream when finished
     * reading.
     *
     * @param stream the streamed contents; may be null
     * @param charset character set of the stream data; may not be null
     * @return the contents, or an empty string if the supplied stream is null
     * @throws IOException if there is an error reading the content
     */
    public static String read(InputStream stream,
                              String charset)
            throws IOException {
        return stream == null ? "" : read(new InputStreamReader(stream, charset));
    }

    /**
     * Read and return the entire contents of the supplied {@link File}.
     *
     * @param file the file containing the information to be read; may be null
     * @return the contents, or an empty string if the supplied reader is null
     * @throws IOException if there is an error reading the content
     */
    public static String read(File file) throws IOException {
        if (file == null) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        try (Reader reader = new FileReader(file)) {
            int numRead = 0;
            char[] buffer = new char[1024];
            while ((numRead = reader.read(buffer)) > -1) {
                sb.append(buffer, 0, numRead);
            }
        }
        return sb.toString();
    }

    /**
     * Get the {@link InputStream input stream} to the resource given by the supplied path. This method performs these operations
     * in order, returning as soon as a file is found:
     * <ol>
     * <li>look for a file on the file system at the given absolute path; otherwise</li>
     * <li>look for a file on the file system at the given path relative to the JVM process; otherwise</li>
     * <li>if a {@code classloader} is supplied, use it to load the file on the classpath at the given path; otherwise</li>
     * <li>if a {@code clazz} is supplied, use it to load the file on its classpath at the given path; otherwise</li>
     * <li>try to convert the path to a URL and obtain the referenced resource</li>
     * </ol>
     * If all of these fail, this method returns null.
     *
     * @param resourcePath the logical path to the classpath, file, or URL resource
     * @param classLoader the classloader that should be used to load the resource as a stream; may be null
     * @param clazz the class that should be used to load the resource as a stream; may be null
     * @param resourceDesc the description of the resource to be used in messages sent to {@code logger}; may be null
     * @param logger a function that is to be called with log messages describing what is being tried; may be null
     * @return an input stream to the resource; or null if the resource could not be found
     * @throws IllegalArgumentException if the resource path is null or empty
     */
    public static InputStream getResourceAsStream(String resourcePath,
                                                  ClassLoader classLoader,
                                                  Class<?> clazz, String resourceDesc, Consumer<String> logger) {
        if (resourcePath == null) {
            throw new IllegalArgumentException("resourcePath may not be null");
        }
        if (resourceDesc == null && logger != null) {
            resourceDesc = resourcePath;
        }
        InputStream result = null;
        try {
            // Try absolute path ...
            Path filePath = FileSystems.getDefault().getPath(resourcePath).toAbsolutePath();
            File f = filePath.toFile();
            if (f.exists() && f.isFile() && f.canRead()) {
                result = new BufferedInputStream(new FileInputStream(f));
            }
            logMessage(result, logger, resourceDesc, "on filesystem at " + filePath);
        }
        catch (InvalidPathException e) {
            // just continue ...
        }
        catch (FileNotFoundException e) {
            // just continue ...
        }
        if (result == null) {
            try {
                // Try relative to current working directory ...
                Path current = FileSystems.getDefault().getPath(".").toAbsolutePath();
                Path absolute = current.resolve(Paths.get(resourcePath)).toAbsolutePath();
                File f = absolute.toFile();
                if (f.exists() && f.isFile() && f.canRead()) {
                    result = new BufferedInputStream(new FileInputStream(f));
                }
                logMessage(result, logger, resourceDesc, "on filesystem relative to '" + current + "' at '" + absolute + "'");
            }
            catch (InvalidPathException e) {
                // just continue ...
            }
            catch (FileNotFoundException e) {
                // just continue ...
            }
        }
        if (result == null && classLoader != null) {
            // Try using the class loader ...
            result = classLoader.getResourceAsStream(resourcePath);
            logMessage(result, logger, resourceDesc, "on classpath");
        }
        if (result == null && clazz != null) {
            // Not yet found, so try the class ...
            result = clazz.getResourceAsStream(resourcePath);
            if (result == null) {
                // Not yet found, so try the class's class loader ...
                result = clazz.getClassLoader().getResourceAsStream(resourcePath);
            }
            logMessage(result, logger, resourceDesc, "on classpath");
        }
        if (result == null) {
            // Still not found, so try to construct a URL out of it ...
            try {
                URL url = new URL(resourcePath);
                result = url.openStream();
                logMessage(result, logger, resourceDesc, "at URL " + url.toExternalForm());
            }
            catch (MalformedURLException e) {
                // just continue ...
            }
            catch (IOException err) {
                // just continue ...
            }
        }
        // May be null ...
        return result;
    }

    /**
     * Create a directory at the given absolute or relative path.
     *
     * @param path the relative or absolute path of the directory; may not be null
     * @return the reference to the existing readable and writable directory
     */
    public static File createDirectory(Path path) {
        File dir = path.toAbsolutePath().toFile();
        if (dir.exists() && dir.canRead() && dir.canWrite()) {
            if (dir.isDirectory()) {
                return dir;
            }
            throw new IllegalStateException("Expecting '" + path + "' to be a directory but found a file");
        }
        dir.mkdirs();
        return dir;
    }

    /**
     * Create a file at the given absolute or relative path.
     *
     * @param path the relative or absolute path of the file to create; may not be null
     * @return the reference to the existing readable and writable file
     */
    public static File createFile(Path path) {
        File file = path.toAbsolutePath().toFile();
        if (file.exists() && file.canRead() && file.canWrite()) {
            if (file.isFile()) {
                return file;
            }
            throw new IllegalStateException("Expecting '" + path + "' to be a file but found a directory");
        }
        file.getParentFile().mkdirs();
        try {
            Files.createFile(path);
        }
        catch (IOException e) {
            throw new IllegalStateException("Unable to create the file '" + path + "': " + e.getMessage(), e);
        }
        return file;
    }

    /**
     * Create a directory at the given absolute or relative path, removing any existing content beforehand.
     *
     * @param path the relative or absolute path of the directory to recreate; may not be null
     * @param removeExistingContent true if any existing content should be removed
     * @return the reference to the existing readable and writable directory
     * @throws IOException if there is a problem deleting the files at this path
     */
    public static File createDirectory(Path path, boolean removeExistingContent) throws IOException {
        File dir = path.toAbsolutePath().toFile();
        if (dir.exists() && dir.canRead() && dir.canWrite()) {
            if (!dir.isDirectory()) {
                throw new IllegalStateException("Expecting '" + path + "' to be a directory but found a file");
            }
            if (!removeExistingContent) {
                return dir;
            }
            delete(path);
        }
        dir.mkdirs();
        return dir;
    }

    /**
     * A method that will delete a file or folder only if it is within the 'target' directory (for safety).
     * Folders are removed recursively.
     *
     * @param path the path to the file or folder in the target directory
     * @throws IOException if there is a problem deleting the file at the given path
     */
    public static void delete(String path) throws IOException {
        if (path != null) {
            delete(Paths.get(path));
        }
    }

    /**
     * A method that will delete a file or folder. Folders are removed recursively.
     *
     * @param fileOrFolder the file or folder to be deleted
     * @throws IOException if there is a problem deleting the file at the given path
     */
    public static void delete(File fileOrFolder) throws IOException {
        if (fileOrFolder != null) {
            delete(fileOrFolder.toPath());
        }
    }

    /**
     * A method that will delete multiple file and/or folders. Folders are removed recursively.
     *
     * @param filesOrFolder the files and folders to be deleted
     * @throws IOException if there is a problem deleting the file at the given path
     */
    public static void delete(File... filesOrFolder) throws IOException {
        for (File fileOrFolder : filesOrFolder) {
            delete(fileOrFolder);
        }
    }

    /**
     * A method that will recursively delete a file or folder.
     *
     * @param path the path to the file or folder in the target directory
     * @throws IOException if there is a problem deleting the file at the given path
     */
    public static void delete(Path path) throws IOException {
        if (path != null) {
            if (path.toAbsolutePath().toFile().exists()) {
                LOGGER.debug("Deleting '{}'...", path);
                Set<FileVisitOption> options = EnumSet.noneOf(FileVisitOption.class);
                int maxDepth = 10;
                FileVisitor<Path> removingVisitor = new SimpleFileVisitor<Path>() {
                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                        java.nio.file.Files.delete(file);
                        return FileVisitResult.SKIP_SUBTREE;
                    }

                    @Override
                    public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                        java.nio.file.Files.delete(dir);
                        return FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
                        LOGGER.error("Unable to remove '{}'", file.getFileName(), exc);
                        return FileVisitResult.CONTINUE;
                    }
                };

                java.nio.file.Files.walkFileTree(path, options, maxDepth, removingVisitor);
            }
        }
    }

    /**
     * Find a port that is available. This method starts a {@link ServerSocket} and obtains the port on which the socket is
     * listening, and then shuts down the socket so the port becomes available.
     *
     * @return the number of the now-available port
     * @throws IllegalStateException if it cannot find an available port
     */
    public static int getAvailablePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
        catch (IOException e) {
            throw new IllegalStateException("Cannot find available port: " + e.getMessage(), e);
        }
    }

    private static void logMessage(InputStream stream, Consumer<String> logger, String resourceDesc, String msg) {
        if (stream != null && logger != null) {
            logger.accept("Found " + resourceDesc + " " + msg);
        }
    }

    /**
     * Atomically load the properties file at the given location within the designated class loader.
     *
     * @param classLoader the supplier for the class loader; may not be null or return null
     * @param classpathResource the path to the resource file; may not be null
     * @return the properties object; never null, but possibly empty
     * @throws IllegalStateException if the file could not be found or read
     */
    public static Properties loadProperties(Supplier<ClassLoader> classLoader, String classpathResource) {
        // This is idempotent, so we don't need to lock ...
        try (InputStream stream = classLoader.get().getResourceAsStream(classpathResource)) {
            Properties props = new Properties();
            props.load(stream);
            return props;
        }
        catch (IOException e) {
            throw new IllegalStateException("Unable to find or read the '" + classpathResource + "' file using the " +
                    classLoader + " class loader", e);
        }
    }

    /**
     * Atomically load the properties file at the given location within the designated class loader.
     *
     * @param classLoader the class loader; may not be null
     * @param classpathResource the path to the resource file; may not be null
     * @return the properties object; never null, but possibly empty
     * @throws IllegalStateException if the file could not be found or read
     */
    public static Properties loadProperties(ClassLoader classLoader, String classpathResource) {
        return loadProperties(() -> classLoader, classpathResource);
    }

    /**
     * Atomically load the properties file at the given location within the designated class' class loader.
     *
     * @param clazz the class whose class loader is to be used; may not be null
     * @param classpathResource the path to the resource file; may not be null
     * @return the properties object; never null, but possibly empty
     * @throws IllegalStateException if the file could not be found or read
     */
    public static Properties loadProperties(Class<?> clazz, String classpathResource) {
        return loadProperties(clazz::getClassLoader, classpathResource);
    }

    /**
     * Read a resource on classpath as a String
     * @param classpathResource
     * @return the content of resource as String
     */
    public static String readClassPathResource(String classpathResource) {
        try (InputStream stream = IoUtil.class.getClassLoader().getResourceAsStream(classpathResource)) {
            Objects.requireNonNull(stream);
            return IoUtil.read(stream);
        }
        catch (IOException e) {
            throw new DebeziumException("Unable to read '" + classpathResource + "'");
        }
    }

    private IoUtil() {
        // Prevent construction
    }
}
