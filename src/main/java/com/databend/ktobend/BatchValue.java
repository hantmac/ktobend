package com.databend.ktobend;


import com.databend.jdbc.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.databend.jdbc.com.fasterxml.jackson.core.JsonProcessingException;
import com.databend.jdbc.com.fasterxml.jackson.databind.JsonNode;
import com.databend.jdbc.com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class BatchValue {
    private String batch;
    private String tableName;
    private JsonNode value;

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

    public Boolean isValueArray() {
        return value.isArray();
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

    public JsonNode getValue() {
        return value;
    }

    public void setValue(JsonNode value) {
        this.value = value;
    }

    public String getValueJson() throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.writeValueAsString(value);
    }

    public List<String> getValueList() {
        List<String> list = new ArrayList<>();
        Iterator<JsonNode> iterator = value.elements();
        while (iterator.hasNext()) {
            JsonNode node = iterator.next();
            list.add(node.toString());
        }

        return list;
    }
}