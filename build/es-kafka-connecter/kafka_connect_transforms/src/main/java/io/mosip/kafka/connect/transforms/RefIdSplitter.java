package io.mosip.kafka.connect.transforms;

import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class RefIdSplitter<R extends ConnectRecord<R>> implements Transformation<R> {

    private String inputField;
    private String regcntrField;
    private String machineField;

    @Override
    public void configure(Map<String, ?> configs) {
        inputField = (String) configs.get("input.field");
        String outputFields = (String) configs.get("output.fields");

        if (outputFields == null || !outputFields.contains(",")) {
            throw new IllegalArgumentException("`output.fields` must be two fields separated by a comma, e.g. `regcntr_id,machine_id`");
        }

        String[] fields = outputFields.split(",");
        regcntrField = fields[0].trim();
        machineField = fields[1].trim();
    }

    @Override
    public R apply(R record) {
        if (!(record.value() instanceof Struct)) return record;

        Struct value = (Struct) record.value();
        Schema schema = record.valueSchema();

        String refId = value.getString(inputField);
        if (refId == null || !refId.contains("_")) return record;

        String[] parts = refId.split("_");
        if (parts.length != 2) return record;

        String regcntrId = parts[0];
        String machineId = parts[1];

        Struct updated = new Struct(schema);
        for (org.apache.kafka.connect.data.Field field : schema.fields()) {
            updated.put(field, value.get(field));
        }

        updated.put(regcntrField, regcntrId);
        updated.put(machineField, machineId);

        return record.newRecord(
            record.topic(),
            record.kafkaPartition(),
            record.keySchema(),
            record.key(),
            schema,
            updated,
            record.timestamp()
        );
    }

    @Override
    public void close() {}

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
        .define("input.field", ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "The field containing ref_id (e.g., 'ref_id')")
        .define("output.fields", ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Comma-separated output fields like 'regcntr_id,machine_id'");
}
