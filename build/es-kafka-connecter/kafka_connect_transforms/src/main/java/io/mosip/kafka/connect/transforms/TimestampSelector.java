package io.mosip.kafka.connect.transforms;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.util.Map;
import java.util.HashMap;

import io.mosip.kafka.connect.transforms.SchemaUtil;
import static io.mosip.kafka.connect.transforms.Requirements.requireMap;
import static io.mosip.kafka.connect.transforms.Requirements.requireStruct;

public abstract class TimestampSelector<R extends ConnectRecord<R>> implements Transformation<R> {

    private class Config {
        String[] tsOrder;
        String outputField;
        String defaultTimestampField;
        boolean useCurrentTime;

        Config(String[] tso, String outField, String defaultField, boolean useCurrent) {
            this.tsOrder = tso;
            this.outputField = outField;
            this.defaultTimestampField = defaultField;
            this.useCurrentTime = useCurrent;
        }
    }

    public static final String TS_ORDER_CONFIG = "ts.order";
    public static final String OUTPUT_FIELD_CONFIG = "output.field";
    public static final String DEFAULT_FIELD_CONFIG = "default.field";
    public static final String USE_CURRENT_TIME_CONFIG = "use.current.time";

    private Config config;
    private Cache<Schema, Schema> schemaUpdateCache;

    public static ConfigDef CONFIG_DEF = new ConfigDef()
        .define(TS_ORDER_CONFIG, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, 
                "The order of the timestamp fields to select from.")
        .define(OUTPUT_FIELD_CONFIG, ConfigDef.Type.STRING, "@timestamp", ConfigDef.Importance.HIGH, 
                "Name of the resultant/output timestamp field.")
        .define(DEFAULT_FIELD_CONFIG, ConfigDef.Type.STRING, "", ConfigDef.Importance.MEDIUM, 
                "Default field to use if no timestamp fields are found.")
        .define(USE_CURRENT_TIME_CONFIG, ConfigDef.Type.BOOLEAN, true, ConfigDef.Importance.MEDIUM, 
                "Use current time if no timestamp field is found.");

    @Override
    public void configure(Map<String, ?> configs) {
        AbstractConfig absconf = new AbstractConfig(CONFIG_DEF, configs, false);

        schemaUpdateCache = new SynchronizedCache<>(new LRUCache<Schema, Schema>(16));

        String tsOrderBulk = absconf.getString(TS_ORDER_CONFIG);
        String outputField = absconf.getString(OUTPUT_FIELD_CONFIG);
        String defaultField = absconf.getString(DEFAULT_FIELD_CONFIG);
        boolean useCurrentTime = absconf.getBoolean(USE_CURRENT_TIME_CONFIG);

        if (tsOrderBulk.isEmpty()) {
            throw new ConfigException("Required transform config field not set: " + TS_ORDER_CONFIG);
        }

        String[] tsOrder = tsOrderBulk.replaceAll("\\s+", "").split(",");

        if (tsOrder.length == 0) {
            throw new ConfigException("Number of fields in timestamp order are zero.");
        }

        config = new Config(tsOrder, outputField, defaultField, useCurrentTime);
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
        schemaUpdateCache = null;
    }

    @Override
    public R apply(R record) {
        if (operatingValue(record) == null) {
            return record;
        } else if (operatingSchema(record) == null) {
            return applySchemaless(record);
        } else {
            return applyWithSchema(record);
        }
    }

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

    public static class Key<R extends ConnectRecord<R>> extends TimestampSelector<R> {
        @Override
        protected Schema operatingSchema(R record) {
            return record.keySchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.key();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, updatedValue, 
                    record.valueSchema(), record.value(), record.timestamp());
        }
    }

    public static class Value<R extends ConnectRecord<R>> extends TimestampSelector<R> {
        @Override
        protected Schema operatingSchema(R record) {
            return record.valueSchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), 
                    updatedSchema, updatedValue, record.timestamp());
        }
    }

    private R applySchemaless(R record) {
        try {
            final Map<String, Object> value = requireMap(operatingValue(record), "TimestampSelector");
            final Map<String, Object> updatedValue = new HashMap<>(value);

            // Try each field in order
            Object timestamp = null;
            for (String field : config.tsOrder) {
                try {
                    Object fieldValue = getNestedField(value, field);
                    if (fieldValue != null && !fieldValue.toString().isEmpty()) {
                        timestamp = fieldValue;
                        break;
                    }
                } catch (Exception e) {
                    // Continue to next field if there's an error
                    continue;
                }
            }

            // If no timestamp found in specified fields
            if (timestamp == null) {
                // Try default field if specified
                if (!config.defaultTimestampField.isEmpty()) {
                    try {
                        timestamp = getNestedField(value, config.defaultTimestampField);
                    } catch (Exception e) {
                        // Ignore if default field is also problematic
                    }
                }
                
                // Use current time as last resort if configured
                if (timestamp == null && config.useCurrentTime) {
                    timestamp = System.currentTimeMillis();
                }
            }

            // Only add output field if we found a timestamp
            if (timestamp != null) {
                updatedValue.put(config.outputField, timestamp);
            }

            return newRecord(record, null, updatedValue);
        } catch (Exception e) {
            // If anything goes wrong, just return the original record
            return record;
        }
    }

    private R applyWithSchema(R record) {
        try {
            final Struct value = requireStruct(operatingValue(record), "TimestampSelector");
            
            // Find timestamp and schema
            Object timestamp = null;
            Schema timestampSchema = null;
            
            // Try each field in order
            for (String field : config.tsOrder) {
                try {
                    Field schemaField = value.schema().field(field);
                    if (schemaField != null) {
                        Object fieldValue = value.get(field);
                        if (fieldValue != null && !fieldValue.toString().isEmpty()) {
                            timestamp = fieldValue;
                            timestampSchema = schemaField.schema();
                            break;
                        }
                    }
                } catch (Exception e) {
                    // Continue to next field if there's an error
                    continue;
                }
            }

            // If no timestamp found in specified fields
            if (timestamp == null) {
                // Try default field if specified
                if (!config.defaultTimestampField.isEmpty()) {
                    try {
                        Field defaultField = value.schema().field(config.defaultTimestampField);
                        if (defaultField != null) {
                            timestamp = value.get(config.defaultTimestampField);
                            timestampSchema = defaultField.schema();
                        }
                    } catch (Exception e) {
                        // Ignore if default field is also problematic
                    }
                }
                
                // Use current time as last resort if configured
                if (timestamp == null && config.useCurrentTime) {
                    timestamp = System.currentTimeMillis();
                    timestampSchema = Schema.INT64_SCHEMA;  // Current time is a long
                }
            }

            // If we still don't have a timestamp, return the original record
            if (timestamp == null) {
                return record;
            }

            // Update schema and create new struct
            Schema updatedSchema = schemaUpdateCache.get(value.schema());
            if (updatedSchema == null) {
                updatedSchema = makeUpdatedSchema(value.schema(), config.outputField, timestampSchema);
                schemaUpdateCache.put(value.schema(), updatedSchema);
            }

            final Struct updatedValue = new Struct(updatedSchema);
            for (Field field : value.schema().fields()) {
                updatedValue.put(field.name(), value.get(field));
            }
            updatedValue.put(config.outputField, timestamp);

            return newRecord(record, updatedSchema, updatedValue);
        } catch (Exception e) {
            // If anything goes wrong, just return the original record
            return record;
        }
    }

    private Object getNestedField(Map<String, Object> map, String field) {
        String[] parts = field.split("\\.");
        Object current = map;
        
        for (String part : parts) {
            if (current instanceof Map) {
                current = ((Map<?, ?>) current).get(part);
                if (current == null) {
                    return null;
                }
            } else {
                return null;
            }
        }
        
        return current;
    }

    static Schema makeUpdatedSchema(Schema schema, String outField, Schema outSchema) {
        final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());

        for (Field field : schema.fields()) {
            builder.field(field.name(), field.schema());
        }

        builder.field(outField, outSchema);

        return builder.build();
    }
}