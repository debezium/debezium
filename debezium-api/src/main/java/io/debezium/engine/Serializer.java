package io.debezium.engine;

public interface Serializer<T, R> {
    public T serialize(R r);
}
