package com.databend.ktobend;

import com.databend.jdbc.com.fasterxml.jackson.core.JsonProcessingException;
import com.databend.jdbc.com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

public class TestBatchValue {

    @Test
public void testBatchValue() throws JsonProcessingException {
        String json = "{\"tableName\":\"tbcc\",\"batch\":\"2024-03-14-1\", \"value\":[{\"id\":10, \"batch\":\"2024-03-14-1\",\"u64\": 30,\"f64\": 20,\"s\": \"hao\",\"s2\": \"hello\",\"a16\":[1],\"a8\":[2],\"d\": \"2011-03-06\",\"t\": \"2016-04-04 11:30:00\"},{\"id\":10, \"batch\":\"2024-03-14-1\",\"u64\": 30,\"f64\": 20,\"s\": \"hao\",\"s2\": \"hello\",\"a16\":[1],\"a8\":[2],\"d\": \"2011-03-06\",\"t\": \"2016-04-04 11:30:00\"}]}";

        BatchValue batchValue = new BatchValue(json);

        Assert.assertEquals("tbcc", batchValue.getTableName());
        System.out.println("Batch: " + batchValue.getBatch());
        System.out.println("Value: " + batchValue.getValueJson());
    }
}
