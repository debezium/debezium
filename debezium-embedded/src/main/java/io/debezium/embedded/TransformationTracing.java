/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.Transformation;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapGetter;

final class TransformationTracing {

    static final String INSTRUMENTATION_SCOPE = "io.debezium.embedded";
    static final String SPAN_NAME_PREFIX = "debezium-transform ";
    static final AttributeKey<String> ALIAS = AttributeKey.stringKey("debezium.transform.alias");
    static final AttributeKey<String> TYPE = AttributeKey.stringKey("debezium.transform.type");
    static final AttributeKey<String> OUTCOME = AttributeKey.stringKey("debezium.transform.outcome");
    static final AttributeKey<String> ERROR_TYPE = AttributeKey.stringKey("error.type");

    static final String OUTCOME_PASSED = "passed";
    static final String OUTCOME_FILTERED = "filtered";
    static final String OUTCOME_FAILED = "failed";

    private static final TextMapGetter<Headers> HEADERS_GETTER = new TextMapGetter<>() {
        @Override
        public Iterable<String> keys(Headers headers) {
            final List<String> keys = new ArrayList<>();
            if (headers != null) {
                headers.forEach(header -> keys.add(header.key()));
            }
            return keys;
        }

        @Override
        public String get(Headers headers, String key) {
            if (headers == null) {
                return null;
            }
            final Header header = headers.lastWithName(key);
            return header == null || header.value() == null ? null : header.value().toString();
        }
    };

    private TransformationTracing() {
    }

    static Transformation<SourceRecord> wrap(String alias, Transformation<SourceRecord> delegate) {
        final String type = delegate.getClass().getName();
        final String spanName = SPAN_NAME_PREFIX + alias;
        return new Transformation<>() {
            @Override
            public SourceRecord apply(SourceRecord record) {
                final Context parent = GlobalOpenTelemetry.getPropagators()
                        .getTextMapPropagator()
                        .extract(Context.current(), record.headers(), HEADERS_GETTER);
                if (!Span.fromContext(parent).getSpanContext().isValid()) {
                    return delegate.apply(record);
                }

                final Span span = GlobalOpenTelemetry.getTracer(INSTRUMENTATION_SCOPE)
                        .spanBuilder(spanName)
                        .setParent(parent)
                        .setSpanKind(SpanKind.INTERNAL)
                        .setAttribute(ALIAS, alias)
                        .setAttribute(TYPE, type)
                        .startSpan();
                try (Scope ignored = parent.with(span).makeCurrent()) {
                    final SourceRecord result = delegate.apply(record);
                    span.setAttribute(OUTCOME, result == null ? OUTCOME_FILTERED : OUTCOME_PASSED);
                    return result;
                }
                catch (RuntimeException | Error e) {
                    span.setAttribute(OUTCOME, OUTCOME_FAILED);
                    span.setAttribute(ERROR_TYPE, e.getClass().getName());
                    span.setStatus(StatusCode.ERROR);
                    throw e;
                }
                finally {
                    span.end();
                }
            }

            @Override
            public ConfigDef config() {
                return delegate.config();
            }

            @Override
            public void configure(Map<String, ?> configs) {
                delegate.configure(configs);
            }

            @Override
            public void close() {
                delegate.close();
            }
        };
    }
}
