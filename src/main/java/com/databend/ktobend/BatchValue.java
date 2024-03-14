package com.databend.ktobend;


import com.databend.jdbc.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.databend.jdbc.com.fasterxml.jackson.core.JsonProcessingException;
import com.databend.jdbc.com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class BatchValue {
    private String batch;
    private String tableName;
    private Map<String, Object> value;
    public BatchValue() {
    }
    public BatchValue(String jsonStr) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            BatchValue batchValue = mapper.readValue(jsonStr, BatchValue.class);
            this.batch = batchValue.getBatch();
            this.tableName = batchValue.getTableName();
            this.value = batchValue.getValue();
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }

    public String getBatch() {
        return batch;
    }

    public void setBatch(String batch) {
        this.batch = batch;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public Map<String, Object> getValue() {
        return value;
    }

    public void setValue(Map<String, Object> value) {
        this.value = value;
    }

    public String getValueJson() throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.writeValueAsString(value);
    }
}