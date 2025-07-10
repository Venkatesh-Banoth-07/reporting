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

import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;

import org.json.JSONObject;
import org.json.JSONException;
import org.json.JSONArray;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
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
        CloseableHttpClient hClient;

        ESQueryConfig(String type, String esUrl, String esIndex, String[] esInputFields, String esOutputField, String[] inputFields, String[] inputDefaultValues,String outputField) {
            super(type,inputFields,inputDefaultValues,outputField,Schema.STRING_SCHEMA);

            this.esUrl=esUrl;
            this.esIndex=esIndex;
            this.esInputFields=esInputFields;
            this.esOutputField=esOutputField;
            this.hClient = HttpClients.createDefault();
        }

        Object makeQuery(List<Object> inputValues){
            if(inputValues.size() != inputFields.length){
                return "Cant get all values for the mentioned " + INPUT_FIELDS_CONFIG + ". Given " + INPUT_FIELDS_CONFIG + " : " + Arrays.toString(inputFields) + " " + inputValues;
            }
            else if(inputValues.size() == 0){
                return null;
            }

            String requestJson = "{\"query\": { \"bool\": { \"must\": [";

            for(int i = 0; i < inputFields.length; i++){
                if(i != 0) requestJson += ",";
                requestJson += "{\"term\": {\"" + esInputFields[i] + ".keyword\": \"" + inputValues.get(i) + "\"}}";
            }
            requestJson += "]}}}";

            JSONObject responseJson;
            final int MAX_RETRIES = 3; // Reduced retries for actual connection issues

            for(int i = 1; i <= MAX_RETRIES; i++){
                try {
                    HttpPost hPost = new HttpPost(this.esUrl + "/" + this.esIndex + "/_search");
                    hPost.setHeader("Content-type", "application/json");
                    hPost.setEntity(new StringEntity(requestJson));

                    try (CloseableHttpResponse hResponse = hClient.execute(hPost)) {
                        int statusCode = hResponse.getCode();
                        if (statusCode != 200) {
                            if (i == MAX_RETRIES) {
                                return "Unexpected response from Elasticsearch: " + statusCode;
                            }
                            continue; // Retry for non-200 status codes
                        }

                        HttpEntity entity = hResponse.getEntity();
                        String jsonString = EntityUtils.toString(entity);
                        responseJson = new JSONObject(jsonString);

                        // Check if we got a valid response structure
                        if (!responseJson.has("hits")) {
                            if (i == MAX_RETRIES) {
                                return "Invalid response structure from Elasticsearch";
                            }
                            continue; // Retry for malformed responses
                        }

                        JSONObject hitsObj = responseJson.getJSONObject("hits");
                        JSONArray hitsArray = hitsObj.getJSONArray("hits");
                        
                        // Check if we have any hits
                        if (hitsArray.length() == 0) {
                            return "No matching record found"; // This is NOT an error - it's a valid "no match" response
                        }

                        // Extract the result from the first hit
                        JSONObject firstHit = hitsArray.getJSONObject(0);
                        JSONObject source = firstHit.getJSONObject("_source");
                        
                        if (!source.has(esOutputField)) {
                            return "Output field '" + esOutputField + "' not found in source document";
                        }

                        return source.getString(esOutputField);
                    }

                } catch (IOException e) {
                    // Network/connection issues - retry
                    if (i == MAX_RETRIES) {
                        return "Connection error: " + e.getMessage();
                    }
                } catch (JSONException e) {
                    // JSON parsing issues - retry
                    if (i == MAX_RETRIES) {
                        return "JSON parsing error: " + e.getMessage();
                    }
                } catch (Exception e) {
                    // Other unexpected errors - retry
                    if (i == MAX_RETRIES) {
                        return "Unexpected error: " + e.getMessage();
                    }
                }
            }

            return "Max retries exceeded"; // This should never be reached
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
            try { 
                if (hClient != null) {
                    hClient.close(); 
                }
            } catch (IOException ignored) {}
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
        .define(DEFAULT_VALUE_CONFIG, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, "Default values for input fields");

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
                throw new ConfigException("One of required transform config fields not set. Required fields: " + ES_URL_CONFIG + " ," + ES_INDEX_CONFIG + " ," + ES_INPUT_FIELDS_CONFIG + " ," + ES_OUTPUT_FIELD_CONFIG + " ," + INPUT_FIELDS_CONFIG + " ," + OUTPUT_FIELD_CONFIG + " ," + DEFAULT_VALUE_CONFIG);
            }

            String[] inputFields = inputFieldBulk.replaceAll("\\s+","").split(",");
            String[] esInputFields = esInputFieldBulk.replaceAll("\\s+","").split(",");
            String[] inputDefaultValues = inputDefaultValuesBulk.replaceAll("\\s+","").split(",");

            if(inputFields.length != esInputFields.length || inputFields.length != inputDefaultValues.length){
                throw new ConfigException("Number of " + INPUT_FIELDS_CONFIG + " and number of " + ES_INPUT_FIELDS_CONFIG + " and number of " + DEFAULT_VALUE_CONFIG + " don't match. Given " + INPUT_FIELDS_CONFIG + ": " + inputFieldBulk + ". Given " + ES_INPUT_FIELDS_CONFIG + ": " + esInputFieldBulk + ". Given " + DEFAULT_VALUE_CONFIG + ": " + inputDefaultValuesBulk);
            }

            try{
                config = new ESQueryConfig(type,esUrl,esIndex,esInputFields,esOutputField,inputFields,inputDefaultValues,outputField);
                System.out.println("DynamicNewField configured successfully for ES URL: " + esUrl + ", Index: " + esIndex);
            }
            catch(Exception e){
                throw new ConfigException("Can't initialize ElasticSearch configuration. Given url : " + esUrl + " Error: " + e.getMessage());
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
        if (config != null) {
            config.close();
        }
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
            if (v == null || (v instanceof String && ((String)v).trim().isEmpty())) {
                if (!"null".equals(config.inputDefaultValues[i])) {
                    v = config.inputDefaultValues[i];
                } else { 
                    nullFound = true; 
                    break; 
                }
            }
            if (v instanceof List<?>) hasList = true;
            valueList.add(v);
        }
        
        Object result;
        if (nullFound) {
            result = "No input data available";
        } else if (hasList) {
            result = config.makeList(valueList);
        } else {
            result = config.make(valueList);
            // Handle the specific case where join returns "No matching record found"
            if (result instanceof String && ((String)result).equals("No matching record found")) {
                result = "No match found"; // Shorter, cleaner message for display
            }
        }
        
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
        boolean nullFound = false;
        
        for(int i = 0; i < config.inputFields.length; i++){
            String fieldName = config.inputFields[i];
            Object v = Requirements.getNestedField(value, fieldName);
            
            if (v == null || (v instanceof String && ((String)v).trim().isEmpty())) {
                if (!"null".equals(config.inputDefaultValues[i])) {
                    valueList.add(config.inputDefaultValues[i]);
                } else {
                    nullFound = true;
                    break;
                }
            } else {
                valueList.add(v);
            }
        }
        
        Object result;
        if (nullFound) {
            result = "No input data available";
        } else {
            result = config.make(valueList);
            // Handle the specific case where join returns "No matching record found"
            if (result instanceof String && ((String)result).equals("No matching record found")) {
                result = "No match found"; // Shorter, cleaner message for display
            }
        }
        
        updatedValue.put(config.outputField, result);
        return newRecord(record, updatedSchema, updatedValue);
    }
}