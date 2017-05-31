/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cube;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;

import org.arquillian.cube.docker.impl.docker.DockerClientExecutor;
import org.arquillian.cube.spi.CubeRegistry;
import org.jboss.arquillian.core.api.Instance;
import org.jboss.arquillian.core.api.annotation.Inject;
import org.jboss.arquillian.test.spi.TestEnricher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Injects implementations of {link DatabaseCube} according to {@link CubeReference} annotation based
 * configuration.
 *
 * @author Jiri Pechanec
 *
 */
public class DatabaseCubeTestEnricher implements TestEnricher {
    private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseCubeTestEnricher.class);

    @Inject
    Instance<CubeRegistry> cubeRegistryInstance;

    @Inject
    private Instance<DockerClientExecutor> dockerClientExecutor;

    @Inject
    private Instance<DatabaseCubeFactory> cubeFactory;

    @Inject
    private Instance<CubeRegistry> registry;

    @Override
    public void enrich(final Object testCase) {
        try {
            List<Field> fields = ReflectionUtil.getFieldsOfType(testCase.getClass(), DatabaseCube.class);
            for (final Field f : fields) {
                // Annotated directly
                if (ReflectionUtil.isClassWithAnnotation(f.getType(),
                        CubeReference.class)) {
                    final String cubeName = f.getType().getAnnotation(CubeReference.class).value();
                    f.set(testCase, cubeFactory.get().createDatabaseCube(registry.get().getCube(cubeName),
                            dockerClientExecutor.get()));
                }

                // Annotated via meta-annotation
                final Annotation[] annotations = f.getAnnotations();
                for (final Annotation a : annotations) {
                    if (ReflectionUtil.isAnnotationWithAnnotation(a.annotationType(), CubeReference.class)) {
                        final String cubeName = a.annotationType().getAnnotation(CubeReference.class).value();
                        f.set(testCase, cubeFactory.get().createDatabaseCube(registry.get().getCube(cubeName),
                                dockerClientExecutor.get()));
                    }
                }
            }
        } catch (final Exception e) {
            LOGGER.error("Error while processing annotations", e);
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public Object[] resolve(final Method method) {
        return new Object[method.getParameterTypes().length];
    }

}
