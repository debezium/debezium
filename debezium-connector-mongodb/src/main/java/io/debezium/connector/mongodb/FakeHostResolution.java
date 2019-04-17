/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FakeHostResolution {
    private final static Logger logger = LoggerFactory.getLogger(FakeHostResolution.class);
    public static void load() {
        logger.info("######################################## Loading fake host resolver");
    }

    private static final Map<String, String> hostMappings = new HashMap<String, String>() {
        {
            put("mongodb1", "10.61.1.46");
            put("mongodb2", "10.61.2.172");
            put("mongodb3", "10.61.1.78");
        }
    };

    /** Fake mongodb DNS resolution */
    public static class MyHostNameService implements sun.net.spi.nameservice.NameService {
        @Override
        public InetAddress[] lookupAllHostAddr(String paramString) throws UnknownHostException {
            
            for (Entry<String, String> entry : hostMappings.entrySet()) {
                if (entry.getKey().equals(paramString)) {
                    logger.info("######################################## getting {} ip address for hostname {} ",entry.getKey(),entry.getValue());
                    final byte[] arrayOfByte = sun.net.util.IPAddressUtil.textToNumericFormatV4(entry.getValue());
                    final InetAddress address = InetAddress.getByAddress(paramString, arrayOfByte);
                    return new InetAddress[] { address };
                }
            }
            throw new UnknownHostException();
        }

        @Override
        public String getHostByAddr(byte[] paramArrayOfByte) throws UnknownHostException {
            throw new UnknownHostException();
        }
    }

    static {
        
        try {
            @SuppressWarnings("unchecked")
            List<sun.net.spi.nameservice.NameService> nameServices = (List<sun.net.spi.nameservice.NameService>) org.apache.commons.lang3.reflect.FieldUtils
                    .readStaticField(InetAddress.class, "nameServices", true);
            nameServices.add(new MyHostNameService());
            logger.info("######################################## added fake host resolver");
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
    }

}
