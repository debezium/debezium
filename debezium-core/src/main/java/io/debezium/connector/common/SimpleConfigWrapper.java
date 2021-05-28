package io.debezium.connector.common;

import java.util.ArrayList;
import java.util.List;

public class SimpleConfigWrapper<ConfigValue> implements ConfigWrapper<ConfigValue> {

    private List<ConfigValue> values;

    public SimpleConfigWrapper(ArrayList<ConfigValue> configValues) {
        this.values = configValues;
    }
}
