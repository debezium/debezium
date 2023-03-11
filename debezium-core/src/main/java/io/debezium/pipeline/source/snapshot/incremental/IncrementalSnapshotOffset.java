package io.debezium.pipeline.source.snapshot.incremental;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.List;
import java.util.Map;

public class IncrementalSnapshotOffset {

    private ObjectMapper mapper = new ObjectMapper();
    private TypeReference<List<Map<String, String>>> offsetMapperTypeRef = new TypeReference<>() {
    };

    public IncrementalSnapshotOffset() {

    }
    public static IncrementalSnapshotOffset parse(String offsetsString) {
        return new IncrementalSnapshotOffset();
    }

    public String toJsonString() {
        return this.toString();
    }
}
