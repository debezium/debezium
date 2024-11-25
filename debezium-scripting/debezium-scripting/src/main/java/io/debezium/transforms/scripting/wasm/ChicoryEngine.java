/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms.scripting.wasm;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import com.dylibso.chicory.experimental.aot.AotMachine;
import com.dylibso.chicory.experimental.hostmodule.annotations.HostModule;
import com.dylibso.chicory.experimental.hostmodule.annotations.WasmExport;
import com.dylibso.chicory.runtime.ByteBufferMemory;
import com.dylibso.chicory.runtime.ImportMemory;
import com.dylibso.chicory.runtime.ImportValues;
import com.dylibso.chicory.runtime.Instance;
import com.dylibso.chicory.wasm.ChicoryException;
import com.dylibso.chicory.wasm.WasmModule;
import com.dylibso.chicory.wasm.types.MemoryLimits;

import io.debezium.DebeziumException;
import io.debezium.transforms.scripting.RecordHeader;

@HostModule("env")
public class ChicoryEngine {

    private final Instance instance;
    private final List<Object> objects = new ArrayList<>();

    private ChicoryEngine(boolean aot, WasmModule module, int memoryMax) {
        var imports = ImportValues.builder()
                .addMemory(new ImportMemory("env", "memory",
                        new ByteBufferMemory(new MemoryLimits(2, memoryMax))))
                .addFunction(ChicoryEngine_ModuleFactory.toHostFunctions(this))
                .build();

        var instanceBuilder = Instance.builder(module)
                .withImportValues(imports);

        if (aot) {
            try {
                instance = instanceBuilder
                        .withMachineFactory(AotMachine::new)
                        .build();
            }
            catch (ChicoryException ex) {
                throw new DebeziumException("Failed to compile the WASM module to Java Bytecode, please use the fallback 'wasm.chicory-interpreter' ", ex);
            }
        }
        else {
            instance = instanceBuilder.build();
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private boolean aot = true;
        private WasmModule module;
        private int memoryMaxLimit = -1;

        private Builder() {
        };

        public Builder withWasmModule(WasmModule module) {
            this.module = module;
            return this;
        }

        public Builder withMaxMemory(int memoryMax) {
            this.memoryMaxLimit = memoryMax;
            return this;
        }

        public Builder withAot(boolean aot) {
            this.aot = aot;
            return this;
        }

        public ChicoryEngine build() {
            Objects.requireNonNull(module);
            if (memoryMaxLimit > MemoryLimits.MAX_PAGES) {
                throw new DebeziumException("Memory max limit cannot exceed: " + MemoryLimits.MAX_PAGES + " but found: " + memoryMaxLimit);
            }
            else if (memoryMaxLimit == -1) {
                memoryMaxLimit = MemoryLimits.MAX_PAGES;
            }
            return new ChicoryEngine(aot, module, memoryMaxLimit);
        }
    }

    private int malloc(int size) {
        return (int) instance.export("malloc").apply(size)[0];
    }

    private void free(int ptr) {
        instance.export("free").apply(ptr);
    }

    public Object eval(Object proxyObject) {
        try {
            var rootObjPtr = registerProxyObject(proxyObject);
            var resultPtr = (int) instance.export("process").apply(rootObjPtr)[0];
            return objects.get(resultPtr);
        }
        finally {
            objects.clear();
        }
    }

    private int registerProxyObject(Object proxyObjext) {
        var index = objects.size();
        objects.add(proxyObjext);
        return index;
    }

    // TODO: verify all the types that can go forth and back
    // ProxyObjects can return arrays, should we always treat everything as an array?
    // or we should have a specific API for it?
    // alternatively we can always prefix the return value with the type, like:
    // 0: following a pointer to a String
    // 1: following pointer to an Array (TODO: decide how to encode arrays)
    // 2: following a reference to a PointerObject
    // TODO: test for array access
    // Struct
    // Map
    // Array
    // TODO: kafka "Schema" handling? - verify
    private Object resolveField(Object proxyObject, String fieldName) {
        if (proxyObject instanceof Map) {
            return ((Map<String, Object>) proxyObject).get(fieldName);
        }
        else if (proxyObject instanceof Struct) {
            return ((Struct) proxyObject).get(fieldName);
        }
        else if (proxyObject instanceof Schema) {
            // TODO: verify this has the correct semantics
            return ((Schema) proxyObject).field(fieldName).schema();
        }
        else if (proxyObject instanceof RecordHeader) {
            // TODO: is this the correct semantics? it doesn't account for the "fieldName"
            return ((RecordHeader) proxyObject).value;
        }
        else {
            throw new DebeziumException("Attempting to access a field: " + fieldName + " but we found unhandled type: " + proxyObject.getClass().getSimpleName());
        }
    }

    @WasmExport
    public int get(int proxyObjectRef, int fieldNamePtr) {
        var fieldName = instance.memory().readCString(fieldNamePtr);
        free(fieldNamePtr);

        var split = fieldName.split("\\.");
        if (split.length == 0) {
            throw new DebeziumException("Guest module performed a Get on an empty path");
        }

        var proxyObject = objects.get(proxyObjectRef);
        for (int i = 0; i < split.length; i++) {
            proxyObject = resolveField(proxyObject, split[i]);
            if (i == split.length - 1) {
                return registerProxyObject(proxyObject);
            }
        }
        throw new DebeziumException("Failed to resolve guest module Get on path: " + fieldName);
    }

    @WasmExport
    public int getString(int proxyObjectRef) {
        var proxyObject = objects.get(proxyObjectRef);
        if (proxyObject.getClass().isAssignableFrom(String.class)) {
            var result = (String) proxyObject;
            var resultPtr = malloc(result.length() + 1);
            instance.memory().writeCString(resultPtr, result);
            return resultPtr;
        }
        else {
            throw new DebeziumException("Attempting to materialize a String but " + proxyObject.getClass().getSimpleName() + " found");
        }
    }

    @WasmExport
    public int getUInt32(int proxyObjectRef) {
        var proxyObject = objects.get(proxyObjectRef);
        if (proxyObject.getClass().isAssignableFrom(Integer.class)) {
            return (Integer) proxyObject;
        }
        // TODO: verify if this is enough to cover all cases
        else if (proxyObject.getClass().isAssignableFrom(Byte.class)) {
            return ((Byte) proxyObject).intValue();
        }
        else {
            throw new DebeziumException("Attempting to materialize a Int but " + proxyObject.getClass().getSimpleName() + " found");
        }
    }

    @WasmExport
    public int isNull(int proxyObjectRef) {
        if (objects.get(proxyObjectRef) == null) {
            return 1;
        }
        else {
            return 0;
        }
    }

    @WasmExport
    public int setBool(int boolRef) {
        var result = (instance.memory().read(boolRef) == 1) ? Boolean.TRUE : Boolean.FALSE;
        free(boolRef);
        return registerProxyObject(result);
    }

    @WasmExport
    public int setInt(int intRef) {
        var result = Integer.valueOf(instance.memory().readInt(intRef));
        free(intRef);
        return registerProxyObject(result);
    }

    @WasmExport
    public int setString(int stringRef) {
        var result = instance.memory().readCString(stringRef);
        free(stringRef);
        return registerProxyObject(result);
    }

    @WasmExport
    public int setNull() {
        return registerProxyObject(null);
    }
}
