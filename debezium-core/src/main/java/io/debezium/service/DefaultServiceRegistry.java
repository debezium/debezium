/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.service;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.annotation.ThreadSafe;
import io.debezium.bean.spi.BeanRegistry;
import io.debezium.common.annotation.Incubating;
import io.debezium.config.Configuration;
import io.debezium.service.spi.Configurable;
import io.debezium.service.spi.InjectService;
import io.debezium.service.spi.ServiceProvider;
import io.debezium.service.spi.ServiceRegistry;
import io.debezium.service.spi.ServiceRegistryAware;
import io.debezium.service.spi.Startable;
import io.debezium.service.spi.Stoppable;

/**
 * Default implementation of the {@link ServiceRegistry}.
 *
 * @author Chris Cranford
 */
@Incubating
@ThreadSafe
public class DefaultServiceRegistry implements ServiceRegistry {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultServiceRegistry.class);

    private final ConcurrentMap<Class<?>, ServiceRegistration<?>> serviceRegistrations = new ConcurrentHashMap<>();
    private final ConcurrentMap<Class<?>, Service> initializedServices = new ConcurrentHashMap<>();
    private final List<ServiceRegistration<?>> registrations = new ArrayList<>();

    private final Configuration configuration;

    /**
     * Creates the default service registry, which registers the {@link BeanRegistry} as a service.
     *
     * @param configuration the user configuration, should not be {@code null}
     * @param beanRegistry the bean registry instance, should not be {@code null}
     */
    public DefaultServiceRegistry(Configuration configuration, BeanRegistry beanRegistry) {
        this.configuration = configuration;
        registerService(new ServiceRegistration<>(BeanRegistry.class, beanRegistry), beanRegistry);
    }

    @Override
    public <T extends Service> T getService(Class<T> serviceClass) {
        T service = serviceClass.cast(initializedServices.get(serviceClass));
        if (service != null) {
            return service;
        }

        // Service initialization requires synchronization
        synchronized (this) {
            service = serviceClass.cast(initializedServices.get(serviceClass));
            if (service != null) {
                return service;
            }
            final ServiceRegistration<T> registration = findRegistration(serviceClass);
            if (registration == null) {
                throw new UnknownServiceException(serviceClass);
            }
            service = registration.getService();
            if (service == null) {
                service = initializeService(registration);
            }
            if (service != null) {
                initializedServices.put(serviceClass, service);
            }
            return service;
        }
    }

    @Override
    public void close() {
        initializedServices.clear();
        synchronized (registrations) {
            ListIterator<ServiceRegistration<?>> iterator = registrations.listIterator(registrations.size());
            while (iterator.hasPrevious()) {
                ServiceRegistration<?> registration = iterator.previous();
                stopService(registration);
            }
            registrations.clear();
        }
        serviceRegistrations.clear();
        LOGGER.info("Debezium ServiceRegistry stopped.");
    }

    @Override
    public <T extends Service> void registerServiceProvider(ServiceProvider<T> serviceProvider) {
        final ServiceRegistration<T> registration = new ServiceRegistration<>(serviceProvider);
        serviceRegistrations.put(serviceProvider.getServiceClass(), registration);
    }

    private <T extends Service> T createService(ServiceProvider<T> serviceProvider) {
        return serviceProvider.createService(configuration, this);
    }

    private <T extends Service> void configureService(ServiceRegistration<T> registration) {
        if (registration.getService() instanceof Configurable) {
            ((Configurable) registration.getService()).configure(configuration);
        }
    }

    private <T extends Service> void injectDependencies(ServiceRegistration<T> registration) {
        final T service = registration.getService();
        doInjections(service);

        if (service instanceof ServiceRegistryAware) {
            ((ServiceRegistryAware) service).injectServiceRegistry(this);
        }
    }

    private <T extends Service> void startService(ServiceRegistration<T> registration) {
        if (registration.getService() instanceof Startable) {
            ((Startable) registration.getService()).start();
        }
    }

    private <T extends Service> void stopService(ServiceRegistration<T> registration) {
        if (registration.getService() instanceof Stoppable) {
            ((Stoppable) registration.getService()).stop();
        }
    }

    private <T extends Service> void registerService(ServiceRegistration<T> registration, T service) {
        registration.setService(service);
        synchronized (registrations) {
            serviceRegistrations.put(registration.getServiceClass(), registration);
            registrations.add(registration);
        }
    }

    private <T extends Service> void doInjections(T service) {
        try {
            for (Method method : service.getClass().getMethods()) {
                InjectService injectService = method.getAnnotation(InjectService.class);
                if (injectService == null) {
                    continue;
                }
                doInjection(service, method, injectService);
            }
        }
        catch (Exception e) {
            LOGGER.error("Failed to inject services into service: " + service.getClass().getName(), e);
        }
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private <T extends Service> void doInjection(T service, Method method, InjectService injectService) {
        final Class<?>[] parameterTypes = method.getParameterTypes();
        if (method.getParameterCount() != 1) {
            throw new ServiceDependencyException("InjectService on a method with an unexpected number of parameters");
        }

        Class requestedServiceType = injectService.service();
        if (requestedServiceType == null || requestedServiceType.equals(Void.class)) {
            requestedServiceType = parameterTypes[0];
        }

        final Service requestedService = getService(requestedServiceType);
        if (requestedService == null) {
            if (injectService.required()) {
                throw new ServiceDependencyException(String.format("Service '%s' not found, required by '%s'.",
                        requestedServiceType.getName(), service.getClass().getName()));
            }
        }
        else {
            try {
                method.invoke(service, requestedService);
            }
            catch (Exception e) {
                throw new ServiceDependencyException(String.format("Failed to inject service '%s' into '%s'.",
                        requestedServiceType.getName(), service.getClass().getName()));
            }
        }
    }

    @SuppressWarnings("unchecked")
    private <T extends Service> ServiceRegistration<T> findRegistration(Class<T> serviceClass) {
        return (ServiceRegistration<T>) serviceRegistrations.get(serviceClass);
    }

    private <T extends Service> T initializeService(ServiceRegistration<T> registration) {
        T service = createService(registration);
        if (service == null) {
            return null;
        }
        doMultiPhaseInitialization(registration);
        return service;
    }

    private <T extends Service> void doMultiPhaseInitialization(ServiceRegistration<T> registration) {
        injectDependencies(registration);
        configureService(registration);
        startService(registration);
    }

    private <T extends Service> T createService(ServiceRegistration<T> registration) {
        final ServiceProvider<T> initiator = registration.getServiceProvider();
        if (initiator == null) {
            throw new UnknownServiceException(registration.getServiceClass());
        }
        try {
            T service = createService(initiator);
            if (service != null) {
                registerService(registration, service);
            }
            return service;
        }
        catch (Exception e) {
            throw new DebeziumException(String.format("Unable to create service %s",
                    registration.getServiceClass().getName()), e);
        }
    }

}
