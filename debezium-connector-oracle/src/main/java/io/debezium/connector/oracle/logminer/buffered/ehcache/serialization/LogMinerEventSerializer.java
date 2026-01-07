/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered.ehcache.serialization;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.ehcache.spi.serialization.Serializer;
import org.ehcache.spi.serialization.SerializerException;

import io.debezium.connector.oracle.logminer.events.LogMinerEvent;

/**
 * An Ehcache {@link Serializer} implementation for handling {@link LogMinerEvent} and subtypes.
 *
 * @author Chris Cranford
 */
public class LogMinerEventSerializer extends AbstractEhcacheSerializer<LogMinerEvent> {

    private static final Map<Class<?>, Class<?>> primitiveToWrapperMap = new HashMap<>();
    static {
        primitiveToWrapperMap.put(boolean.class, Boolean.class);
        primitiveToWrapperMap.put(byte.class, Byte.class);
        primitiveToWrapperMap.put(char.class, Character.class);
        primitiveToWrapperMap.put(short.class, Short.class);
        primitiveToWrapperMap.put(int.class, Integer.class);
        primitiveToWrapperMap.put(long.class, Long.class);
        primitiveToWrapperMap.put(float.class, Float.class);
        primitiveToWrapperMap.put(double.class, Double.class);
    }

    private final Map<String, SerdesProvider<?>> serdesProviders = new HashMap<>();
    private final Map<Class<?>, Constructor<?>> constructorCache = new ConcurrentHashMap<>();

    public LogMinerEventSerializer(ClassLoader classLoader) {
        registerSerdesProviders();
    }

    @Override
    protected void serialize(LogMinerEvent object, SerializerOutputStream stream) throws IOException {
        final SerdesProvider serdes = getSerdesByClassName(object.getClass().getName());
        stream.writeString(object.getClass().getName());
        serdes.serialize(object, stream);
    }

    @Override
    protected LogMinerEvent deserialize(SerializerInputStream stream) throws IOException {
        final String clazzName = stream.readString();
        final SerdesProvider<?> serdes = getSerdesByClassName(clazzName);

        final DeserializationContext context = new DeserializationContext();
        serdes.deserialize(context, stream);

        return constructObject(serdes.getJavaType(), context);
    }

    private void registerSerdesProviders() {
        registerSerdes(new DmlEventSerdesProvider<>());
        registerSerdes(new LobEraseEventSerdesProvider<>());
        registerSerdes(new LobWriteEventSerdesProvider<>());
        registerSerdes(new LogMinerEventSerdesProvider<>());
        registerSerdes(new RedoSqlDmlEventSerdesProvider<>());
        registerSerdes(new SelectLobLocatorSerdesProvider<>());
        registerSerdes(new TruncateEventSerdesProvider<>());
        registerSerdes(new XmlBeginEventSerdesProvider<>());
        registerSerdes(new XmlEndEventSerdesProvider<>());
        registerSerdes(new XmlWriteEventSerdesProvider<>());
        registerSerdes(new ExtendedStringBeginEventSerdesProvider<>());
        registerSerdes(new ExtendedStringWriteEventSerdesProvider<>());
    }

    private <T> void registerSerdes(SerdesProvider<T> serdesProvider) {
        serdesProviders.put(serdesProvider.getJavaType().getName(), serdesProvider);
    }

    private SerdesProvider<?> getSerdesByClassName(String clazzName) {
        final SerdesProvider<?> provider = serdesProviders.get(clazzName);
        if (provider == null) {
            throw new SerializerException("Failed to find SerdesProvider for class: " + clazzName);
        }
        return provider;
    }

    private LogMinerEvent constructObject(Class<?> clazz, DeserializationContext context) {
        try {
            final List<Object> values = context.getValues();

            // The constructor cache allows for maintaining a reference to the matching class constructor
            // used after the first match lookup to improve serialization performance.
            final Constructor<?> constructor = constructorCache.computeIfAbsent(clazz, (classType) -> {
                final Class<?>[] parameterTypes = getParameterTypes(values);
                final Constructor<?> result = getMatchingConstructor(clazz, parameterTypes);
                if (result == null) {
                    throw new SerializerException("Failed to find matching constructor for argument types: " + Arrays.toString(parameterTypes));
                }
                return result;
            });

            return (LogMinerEvent) constructor.newInstance(values.toArray());
        }
        catch (Exception e) {
            throw new SerializerException("Failed to construct object of type " + clazz.getName(), e);
        }
    }

    private Class<?>[] getParameterTypes(List<Object> values) {
        return values.stream().map(Object::getClass).toArray(Class<?>[]::new);
    }

    private Constructor<?> getMatchingConstructor(Class<?> clazz, Class<?>[] parameterTypes) {
        for (Constructor<?> constructor : clazz.getConstructors()) {
            final Class<?>[] paramTypes = constructor.getParameterTypes();
            if (paramTypes.length == parameterTypes.length) {
                boolean matches = true;
                for (int i = 0; i < paramTypes.length; i++) {
                    if (!paramTypes[i].isAssignableFrom(parameterTypes[i])
                            && !parameterTypes[i].isAssignableFrom(paramTypes[i])
                            && !isWrapperPrimitiveMatch(paramTypes[i], parameterTypes[i])) {
                        matches = false;
                        break;
                    }
                }
                if (matches) {
                    return constructor;
                }
            }
        }
        return null;
    }

    private static boolean isWrapperPrimitiveMatch(Class<?> paramType, Class<?> argType) {
        if (paramType.isPrimitive()) {
            return argType.equals(primitiveToWrapperMap.get(paramType));
        }
        return false;
    }
}
