package io.mosip.kafka.connect.transforms;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

// import org.apache.http.HttpHost;
// import org.elasticsearch.client.RestHighLevelClient;
// import org.elasticsearch.client.RestClient;
// import org.elasticsearch.client.RequestOptions;
// import org.elasticsearch.action.search.SearchRequest;
// import org.elasticsearch.action.search.SearchResponse;
// import org.elasticsearch.search.SearchHit;
// import org.elasticsearch.search.builder.SearchSourceBuilder;
// import org.elasticsearch.index.query.QueryBuilders;
// import org.elasticsearch.index.query.BoolQueryBuilder;

import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;


import org.json.JSONObject;
import org.json.JSONException;
import org.json.JSONArray;

import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.io.IOException;

public abstract class DynamicNewField<R extends ConnectRecord<R>> implements Transformation<R> {

    private abstract class Config{
        String type;
        String[] inputFields;
        String[] inputDefaultValues;
        String outputField;
        Schema outputSchema;
        Config(String type, String[] inputFields, String[] inputDefaultValues, String outputField, Schema outputSchema){
            this.type = type;
            this.inputFields = inputFields;
            this.inputDefaultValues = inputDefaultValues;
            this.outputField = outputField;
            this.outputSchema = outputSchema;
        }
        abstract Object make(Object input);

        abstract List<Object> makeList(Object input);

        void close() {}
    }
    private class ESQueryConfig extends Config{
        String esUrl;
        String esIndex;
        String[] esInputFields;
        String esOutputField;

        // RestHighLevelClient esClient;
        CloseableHttpClient hClient;
        //HttpGet hGet;

        ESQueryConfig(String type, String esUrl, String esIndex, String[] esInputFields, String esOutputField, String[] inputFields, String[] inputDefaultValues,String outputField) {
            super(type,inputFields,inputDefaultValues,outputField,Schema.STRING_SCHEMA);

            this.esUrl=esUrl;
            this.esIndex=esIndex;
            this.esInputFields=esInputFields;
            this.esOutputField=esOutputField;

            // esClient = new RestHighLevelClient(RestClient.builder(HttpHost.create(this.esUrl)));
            this.hClient = HttpClients.createDefault();
            //hGet = new HttpGet(this.esUrl+"/"+this.esIndex+"/_search");
            //hGet.setHeader("Content-type", "application/json");
        }

        Object makeQuery(List<Object> inputValues) {
            if (inputValues.size() != inputFields.length) {
                System.err.println("Mismatch in input fields vs values: " + Arrays.toString(inputFields) + " -> " + inputValues);
                return "empty";
            } else if (inputValues.isEmpty()) {
                return "empty";
            }

            // Construct ES POST query
            StringBuilder requestJson = new StringBuilder();
            requestJson.append("{\"query\": { \"bool\": { \"must\": [");
            for (int i = 0; i < inputFields.length; i++) {
                if (i > 0) requestJson.append(",");
                requestJson.append("{\"term\": {\"")
                        .append(esInputFields[i])
                        .append(".keyword\": \"")
                        .append(String.valueOf(inputValues.get(i)))
                        .append("\"}}");
            }
            requestJson.append("]}}}");

            final String fullUrl = this.esUrl + "/" + this.esIndex + "/_search";
            final int MAX_RETRIES = 5;

            for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
                try {
                    HttpPost hPost = new HttpPost(fullUrl);
                    hPost.setHeader("Content-type", "application/json");
                    hPost.setEntity(new StringEntity(requestJson.toString()));

                    try (CloseableHttpResponse response = hClient.execute(hPost)) {
                        int statusCode = response.getCode();
                        if (statusCode != 200) {
                            System.err.println("Unexpected ES response code: " + statusCode);
                            return "empty";
                        }

                        HttpEntity entity = response.getEntity();
                        String responseBody = EntityUtils.toString(entity);
                        JSONObject responseJson = new JSONObject(responseBody);
                        JSONArray hits = responseJson.getJSONObject("hits").getJSONArray("hits");

                        Set<String> outputValues = new LinkedHashSet<>();
                        for (int j = 0; j < hits.length(); j++) {
                            JSONObject src = hits.getJSONObject(j).optJSONObject("_source");
                            if (src != null) {
                                String val = src.optString(esOutputField, "").trim();
                                if (!val.isEmpty()) outputValues.add(val);
                            }
                        }
                        return outputValues.isEmpty() ? "empty" : String.join(" | ", outputValues);
                    }
                } catch (Exception e) {
                    System.err.println("Error during ES join (attempt " + attempt + "): " + e.getMessage());
                    if (attempt == MAX_RETRIES) {
                        return "empty";
                    }
                }
            }

            return "empty";
        }

        List<Object> makeQueryForList(List<Object> inputValues){

            int arraySize = -1;
            for(Object v : inputValues){
                if(v instanceof List){
                    if(arraySize == -1) arraySize = ((List<?>)v).size();
                    else if(arraySize != ((List<?>)v).size()) throw new DataException("Irregular Array List Sizes");
                }
            }
            List<Object> output = new ArrayList<Object>();

            for (int j = 0; j < arraySize; j++) {
                List<Object> singleInput = new ArrayList<>();
                for (Object v : inputValues) {
                    if (v instanceof List<?>) singleInput.add(((List<?>) v).get(j));
                    else singleInput.add(v);
                }
                output.add(make(singleInput));
            }
            return output;
        }
        
         @Override
        Object make(Object input) {
            return makeQuery((List<Object>) input);
        }

        @Override
        List<Object> makeList(Object input) {
            return makeQueryForList((List<Object>) input);
        }

        @Override
        void close() {
            try { hClient.close(); } catch (IOException ignored) {}
        }

    }

    public static final String PURPOSE = "dynamic field insertion";
    public static final String TYPE_CONFIG = "query.type";
    public static final String ES_URL_CONFIG = "es.url";
    public static final String ES_INDEX_CONFIG = "es.index";
    public static final String ES_INPUT_FIELDS_CONFIG = "es.input.fields";
    public static final String ES_OUTPUT_FIELD_CONFIG = "es.output.field";
    public static final String INPUT_FIELDS_CONFIG = "input.fields";
    public static final String OUTPUT_FIELD_CONFIG = "output.field";
    public static final String DEFAULT_VALUE_CONFIG = "input.default.values";

    private Config config;
    private Cache<Schema, Schema> schemaUpdateCache;

    public static ConfigDef CONFIG_DEF = new ConfigDef()
        .define(TYPE_CONFIG, ConfigDef.Type.STRING, "es", ConfigDef.Importance.HIGH, "This is the type of query made. For now this field is ignored and defaulted to es")
        .define(ES_URL_CONFIG, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, "Installed Elasticsearch URL")
        .define(ES_INDEX_CONFIG, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, "Name of the index in ES to search")
        .define(ES_INPUT_FIELDS_CONFIG, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, "ES documents with given input field will be searched for. This field tells the key name")
        .define(ES_OUTPUT_FIELD_CONFIG, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, "If a successful match is made with the above input field+value, the value of this output field from the same document will be returned")
        .define(INPUT_FIELDS_CONFIG, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, "Name of the field in the current index")
        .define(OUTPUT_FIELD_CONFIG, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, "Name to give to the new field")
        .define(DEFAULT_VALUE_CONFIG, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, "Default vlaues for input fields");


    @Override
    public void configure(Map<String, ?> configs) {
        AbstractConfig absconf = new AbstractConfig(CONFIG_DEF, configs);

        schemaUpdateCache = new SynchronizedCache<>(new LRUCache<Schema,Schema>(16));

        String type = absconf.getString(TYPE_CONFIG);

        if(type.equals("es")){
            String esUrl = absconf.getString(ES_URL_CONFIG);
            String esIndex = absconf.getString(ES_INDEX_CONFIG);
            String esInputFieldBulk = absconf.getString(ES_INPUT_FIELDS_CONFIG);
            String esOutputField = absconf.getString(ES_OUTPUT_FIELD_CONFIG);
            String inputFieldBulk = absconf.getString(INPUT_FIELDS_CONFIG);
            String outputField = absconf.getString(OUTPUT_FIELD_CONFIG);
            String inputDefaultValuesBulk = absconf.getString(DEFAULT_VALUE_CONFIG);

            if(type.isEmpty() || esUrl.isEmpty() || esIndex.isEmpty() || esInputFieldBulk.isEmpty() || esOutputField.isEmpty() || inputFieldBulk.isEmpty() || outputField.isEmpty() || inputDefaultValuesBulk.isEmpty()){
                throw new ConfigException("One of required transform config fields not set. Required field in tranforms: " + ES_URL_CONFIG + " ," + ES_INDEX_CONFIG + " ," + ES_INPUT_FIELDS_CONFIG + " ," + ES_OUTPUT_FIELD_CONFIG + " ," + INPUT_FIELDS_CONFIG + " ," + OUTPUT_FIELD_CONFIG + " ," + DEFAULT_VALUE_CONFIG);
            }

            String[] inputFields = inputFieldBulk.replaceAll("\\s+","").split(",");
            String[] esInputFields = esInputFieldBulk.replaceAll("\\s+","").split(",");
            String[] inputDefaultValues = inputDefaultValuesBulk.replaceAll("\\s+","").split(",");

            if(inputFields.length != esInputFields.length || inputFields.length != inputDefaultValues.length){
                throw new ConfigException("No of " + INPUT_FIELDS_CONFIG + " and no of " + ES_INPUT_FIELDS_CONFIG + "and number of " + DEFAULT_VALUE_CONFIG + " doesnt match. Given " + INPUT_FIELDS_CONFIG + ": " + inputFieldBulk + ". Given " + ES_INPUT_FIELDS_CONFIG + ": " + esInputFieldBulk + ". Given " + DEFAULT_VALUE_CONFIG + ": " + inputDefaultValuesBulk);
            }

            try{
                config = new ESQueryConfig(type,esUrl,esIndex,esInputFields,esOutputField,inputFields,inputDefaultValues,outputField);
            }
            catch(Exception e){
                throw new ConfigException("Can't connect to ElasticSearch. Given url : " + esUrl + " Error: " + e.getMessage());
            }
        }
        else{
            throw new ConfigException("Unknown Type : " + type + ". Available types now: \"es\"" );
        }
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

    public static class Key<R extends ConnectRecord<R>> extends DynamicNewField<R> {
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
            return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, updatedValue, record.valueSchema(), record.value(), record.timestamp());
        }
    }

    public static class Value<R extends ConnectRecord<R>> extends DynamicNewField<R> {
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
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
        }
    }


    private R applySchemaless(R record) {
        final Map<String, Object> value = Requirements.requireMap(operatingValue(record), PURPOSE);

        final Map<String, Object> updatedValue = new HashMap<>(value);

        List<Object> valueList = new ArrayList<Object>();
        boolean nullFound = false, hasList = false;
        for (int i = 0; i < config.inputFields.length; i++) {
            Object v = Requirements.getNestedField(value, config.inputFields[i]);
            if (v == null || (v instanceof String && ((String)v).isEmpty())) {
                if (!"null".equals(config.inputDefaultValues[i])) v = config.inputDefaultValues[i];
                else { nullFound = true; break; }
            }
            if (v instanceof List<?>) hasList = true;
            valueList.add(v);
        }
        Object result = (!nullFound && hasList) ? config.makeList(valueList) : (!nullFound ? config.make(valueList) : "empty");
        updatedValue.put(config.outputField, result);
        return newRecord(record, null, updatedValue);
    }


    private R applyWithSchema(R record) {
        Struct value = Requirements.requireStruct(operatingValue(record), PURPOSE);
        Schema schema = value.schema();
        Schema updatedSchema = schemaUpdateCache.get(schema);
        if (updatedSchema == null) {
            SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());
            for (Field field : schema.fields()) builder.field(field.name(), field.schema());
            builder.field(config.outputField, config.outputSchema);
            updatedSchema = builder.build();
            schemaUpdateCache.put(schema, updatedSchema);
        }

        Struct updatedValue = new Struct(updatedSchema);
        for (Field field : schema.fields()) updatedValue.put(field.name(), value.get(field));


        List<Object> valueList = new ArrayList<>();
        for(String field : config.inputFields){
            Object v = Requirements.getNestedField(value, field);
            // v is expected to be a string, case of List dealt in applySchemaless()
            if (v != null) valueList.add(v);
            else {
                if (!"null".equals(config.inputDefaultValues[Arrays.asList(config.inputFields).indexOf(field)]))
                    valueList.add(config.inputDefaultValues[Arrays.asList(config.inputFields).indexOf(field)]);
                else valueList.add("empty");
            }

        }
        updatedValue.put(config.outputField, config.make(valueList));

        return newRecord(record, updatedSchema, updatedValue);
    }
    
    private Schema makeUpdatedSchema(Schema schema) {
        final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());

        for (Field field : schema.fields()) {
            builder.field(field.name(), field.schema());
        }

        builder.field(config.outputField, config.outputSchema);

        return builder.build();
    }
    

}
