package io.debezium.pipeline.txmetadata;

import java.util.Map;

public interface HeaderMapWrapper {

    Map<String, Object> headerMap();
}
