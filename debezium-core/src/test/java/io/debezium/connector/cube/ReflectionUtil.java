/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cube;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.List;

class ReflectionUtil {

    public static List<Field> getFieldsOfType(final Class<?> source, final Class<?> requestedClass) {
        List<Field> declaredAccessibleFields = AccessController.doPrivileged(new PrivilegedAction<List<Field>>() {
            public List<Field> run() {
                List<Field> foundFields = new ArrayList<Field>();
                Class<?> nextSource = source;
                while (nextSource != Object.class) {
                    for (Field field : nextSource.getDeclaredFields()) {
                        if (field.getType().isAssignableFrom(requestedClass)) {
                            if (!field.isAccessible()) {
                                field.setAccessible(true);
                            }
                            foundFields.add(field);
                        }
                    }
                    nextSource = nextSource.getSuperclass();
                }
                return foundFields;
            }
        });
        return declaredAccessibleFields;
    }

    public static boolean isClassWithAnnotation(final Class<?> source,
            final Class<? extends Annotation> annotationClass) {
        return AccessController.doPrivileged(new PrivilegedAction<Boolean>() {
            @Override
            public Boolean run() {
                boolean annotationPresent = false;
                Class<?> nextSource = source;
                while (nextSource != Object.class && nextSource != null) {
                    if (nextSource.isAnnotationPresent(annotationClass)) {
                        return true;
                    }
                    nextSource = nextSource.getSuperclass();
                }
                return annotationPresent;
            }
        });
    }

    public static boolean isAnnotationWithAnnotation(final Class<? extends Annotation> source,
            final Class<? extends Annotation> annotationClass) {
        return isClassWithAnnotation(source, annotationClass);
    }
}
