package io.debezium.embedded.reactive;

import java.util.List;

public interface RecordSupplier<R> {
    List<R> poll() throws InterruptedException;
}
