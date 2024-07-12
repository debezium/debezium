/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.testcontainers.util.dns;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.net.spi.InetAddressResolver;
import java.net.spi.InetAddressResolverProvider;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Custom {@link InetAddressResolverProvider} that uses {@link FakeDnsAddressResolver} to resolve addresses.

 */
public class FakeDnsAddressResolverProvider extends InetAddressResolverProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(FakeDnsAddressResolverProvider.class);

    @Override
    public InetAddressResolver get(Configuration configuration) {
        LOGGER.info("Using Custom Address Resolver : {} ", this.name());
        LOGGER.info("Registry initialised");
        return new FakeDnsAddressResolver(FakeDns.getInstance(), configuration.builtinResolver());
    }

    @Override
    public String name() {
        return FakeDnsAddressResolver.class.getName();
    }

    /**
     * When {@link FakeDns} is enabled the resolver will first perform lookups in the fake DNS registry.
     * If the address is not found, the resolver will delegate to the fallback resolver.
     * If {@link FakeDns} is disabled, the resolver will only delegate to the fallback resolver.
     */
    public static final class FakeDnsAddressResolver implements InetAddressResolver {
        private final InetAddressResolver fallbackResolver;
        private final FakeDns fakeDns;

        public FakeDnsAddressResolver(FakeDns fakeDns, InetAddressResolver fallbackResolver) {
            this.fakeDns = fakeDns;
            this.fallbackResolver = fallbackResolver;
        }

        @Override
        public Stream<InetAddress> lookupByName(String host, LookupPolicy lookupPolicy) throws UnknownHostException {
            var maybeAddress = fakeDns.resolve(host);

            return maybeAddress.isPresent()
                    ? maybeAddress.stream()
                    : fallbackResolver.lookupByName(host, lookupPolicy);
        }

        @Override
        public String lookupByAddress(byte[] address) throws UnknownHostException {
            var maybeHost = fakeDns.resolve(address);

            return maybeHost.isPresent()
                    ? maybeHost.get()
                    : fallbackResolver.lookupByAddress(address);
        }
    }
}
