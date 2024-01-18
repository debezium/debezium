/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.testcontainers.util;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Fake DNS resolver which allows tests to work well with testcontainer under Docker Desktop
 *
 * Adaptation of  https://github.com/CorfuDB/CorfuDB/blob/master/it/src/main/java/org/corfudb/universe/universe/docker/FakeDns.java
 */
public class FakeDns {
    private static final Logger LOGGER = LoggerFactory.getLogger(FakeDns.class);

    private static final FakeDns instance = new FakeDns();

    private final Map<String, InetAddress> forwardResolutions = new HashMap<>();

    private Object originalNameService = null;

    /**
     * whether the fake resolver has been installed
     */
    private boolean installed = false;

    private FakeDns() {
    }

    public static FakeDns getInstance() {
        return instance;
    }

    public void addResolution(String hostname, InetAddress ip) {
        synchronized (this) {
            forwardResolutions.put(hostname, ip);
        }
        LOGGER.info("Added dns resolution: {} -> {}", hostname, ip);
    }

    public void removeResolution(String hostname, InetAddress ip) {
        synchronized (this) {
            forwardResolutions.remove(hostname, ip);
        }
        LOGGER.info("Removed dns resolution: {} -> {}", hostname, ip);
    }

    public synchronized boolean isInstalled() {
        return installed;
    }

    /**
     * Install the fake DNS resolver into the Java runtime.
     */
    public FakeDns install() {
        synchronized (this) {
            if (installed) {
                return this;
            }
            try {
                originalNameService = installDns();
            }
            catch (Exception e) {
                throw new IllegalStateException(e);
            }
            installed = true;
        }
        LOGGER.info("FakeDns is now ENABLED in JVM Runtime");
        return this;
    }

    /**
     * Restore default DNS resolver in Java runtime.
     **/
    public FakeDns restore() {
        synchronized (this) {
            if (!installed) {
                return this;
            }
            try {
                restoreDns(originalNameService);
            }
            catch (Exception e) {
                throw new IllegalStateException(e);
            }
            installed = false;
        }
        LOGGER.info("FakeDns is now DISABLED in JVM Runtime");
        return this;
    }

    private Object installDns()
            throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, ClassNotFoundException, NoSuchFieldException {
        // Override the NameService in Java 9 or later.
        final Class<?> nameServiceInterface = Class.forName("java.net.InetAddress$NameService");
        Field field = InetAddress.class.getDeclaredField("nameService");

        // Get the default NameService to fall back to.
        Method method = InetAddress.class.getDeclaredMethod("createNameService");
        method.setAccessible(true);
        Object fallbackNameService = method.invoke(null);

        // Install a wrapping NameService proxy instance
        Object installedNameService = Proxy.newProxyInstance(
                nameServiceInterface.getClassLoader(),
                new Class<?>[]{ nameServiceInterface },
                new NameServiceListener(fallbackNameService));

        // Set the NameService on the InetAddress field
        field.setAccessible(true);
        field.set(InetAddress.class, installedNameService);

        return fallbackNameService;
    }

    private void restoreDns(Object nameService)
            throws IllegalAccessException, NoSuchFieldException {
        Field field = InetAddress.class.getDeclaredField("nameService");
        field.setAccessible(true);
        field.set(InetAddress.class, nameService);
    }

    public static <T extends Throwable> void propagateIfPossible(Throwable throwable, Class<T> declaredType) throws T {
        if (declaredType.isInstance(throwable)) {
            throw declaredType.cast(throwable);
        }
    }

    /**
     * NameServiceListener with a NameService implementation to fallback to.
     */
    private class NameServiceListener implements InvocationHandler {

        private final Object fallbackNameService;

        /**
         * Creates {@link NameServiceListener} with fallback NameService
         * The parameter is untyped as {@code java.net.InetAddress$NameService} is private
         *
         * @param fallbackNameService fallback NameService
         */
        NameServiceListener(Object fallbackNameService) {
            this.fallbackNameService = fallbackNameService;
        }

        private InetAddress[] lookupAllHostAddr(String host) throws UnknownHostException {
            InetAddress inetAddress;

            synchronized (FakeDns.this) {
                inetAddress = forwardResolutions.get(host);
            }
            if (inetAddress != null) {
                return new InetAddress[]{ inetAddress };
            }

            return invokeFallBackMethod("lookupAllHostAddr", InetAddress[].class, host);
        }

        private String getHostByAddr(byte[] addr) throws UnknownHostException {
            return invokeFallBackMethod("getHostsByAddr", String.class, addr);
        }

        private <T, P> T invokeFallBackMethod(String methodName, Class<T> returnType, P param) throws UnknownHostException {
            try {
                Method method = fallbackNameService
                        .getClass()
                        .getDeclaredMethod(methodName, param.getClass());

                method.setAccessible(true);
                var result = method.invoke(fallbackNameService, param);
                return returnType.cast(result);
            }
            catch (ReflectiveOperationException | SecurityException e) {
                propagateIfPossible(e.getCause(), UnknownHostException.class);
                throw new AssertionError("unexpected reflection issue", e);
            }
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            switch (method.getName()) {
                case "lookupAllHostAddr":
                    return lookupAllHostAddr((String) args[0]);
                case "getHostByAddr":
                    return getHostByAddr((byte[]) args[0]);
                default:
                    throw new UnsupportedOperationException();
            }
        }
    }
}
