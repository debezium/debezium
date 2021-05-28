package io.debezium.transforms;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.transforms.util.RegexValidator;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

public class EventRouterWrapper<R extends ConnectRecordWrapper<R>> implements TransformationWrapper<R> {

    public static final String OVERVIEW_DOC = "Update the record topic using the configured regular expression and replacement string."
            + "<p/>Under the hood, the regex is compiled to a <code>java.util.regex.Pattern</code>. "
            + "If the pattern matches the input topic, <code>java.util.regex.Matcher#replaceFirst()</code> is used with the replacement string to obtain the new topic.";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ConfigName.REGEX, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, new RegexValidator(), ConfigDef.Importance.HIGH,
                    "Regular expression to use for matching.")
            .define(ConfigName.REPLACEMENT, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH,
                    "Replacement string.");

    private interface ConfigName {
        String REGEX = "regex";
        String REPLACEMENT = "replacement";
    }

    private Pattern regex;
    private String replacement;

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        regex = Pattern.compile(config.getString(EventRouterWrapper.ConfigName.REGEX));
        replacement = config.getString(EventRouterWrapper.ConfigName.REPLACEMENT);
    }

    @Override
    public R apply(R record) {
        final Matcher matcher = regex.matcher(record.topic());
        if (matcher.matches()) {
            final String topic = matcher.replaceFirst(replacement);
            return record.newRecord(topic, record.kafkaPartition(), record.keySchema(), record.key(), record.valueSchema(), record.value(), record.timestamp());
        }
        return record;
    }

    @Override
    public void close() {
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

}
