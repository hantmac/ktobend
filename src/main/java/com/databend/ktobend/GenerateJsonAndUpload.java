package com.databend.ktobend;

import com.databend.jdbc.DatabendConnection;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.*;

import com.databend.jdbc.com.fasterxml.jackson.databind.ObjectMapper;
import net.andreinc.mockneat.MockNeat;

public class GenerateJsonAndUpload {

    private static final MockNeat mockNeat = MockNeat.threadLocal();
    KafkaStringProducer stringProducer;
    DatabendConnection databendConnection;

    public GenerateJsonAndUpload() throws SQLException {
        KafkaJsonConsumer consumer = new KafkaJsonConsumer(Config.getKafkaJsonTopic(), Config.getKafkaConsumerGroupIdJson());
        int batchSize = Config.getDatabendBatchSize();
        this.stringProducer = new KafkaStringProducer();
        this.databendConnection = (DatabendConnection) Databendconn.createConnection();
    }

    public static List<Map<String, Object>> genBatch(String batch, int n) {
        List<Map<String, Object>> data = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            Map<String, Object> d = new HashMap<>();
            d.put("id", mockNeat.ints().range(10000000, 2000000000).get());
            d.put("batch", batch);
            d.put("name", mockNeat.names().full().get());
            d.put("birthday", LocalDateTime.now().toLocalDate().toString());
            d.put("address", mockNeat.strings().size(40).get());
            d.put("company", mockNeat.strings().size(40).get());
            d.put("job", mockNeat.strings().size(40).get());
            d.put("bank", "中国人民银行");
            d.put("password", mockNeat.passwords().get());
            d.put("phone_number", mockNeat.strings().size(40).get());
            d.put("user_agent", mockNeat.strings().size(40).get());
            d.put("c1", mockNeat.strings().size(40).get());
            d.put("c2", mockNeat.strings().size(40).get());
            d.put("c3", mockNeat.strings().size(40).get());
            d.put("c4", mockNeat.strings().size(40).get());
            d.put("c5", mockNeat.strings().size(40).get());
            d.put("c6", mockNeat.strings().size(40).get());
            d.put("c7", mockNeat.strings().size(40).get());
            d.put("c8", mockNeat.strings().size(40).get());
            d.put("c9", mockNeat.strings().size(40).get());
            d.put("c10", mockNeat.strings().size(40).get());
            d.put("d", LocalDateTime.now().toLocalDate().toString());
            d.put("t", LocalDateTime.now().toString());
            data.add(d);
        }
        return data;
    }

    public String generateNDJsonData(List<Map<String, Object>> batchJsonData) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        StringBuilder sb = new StringBuilder();

        for (Map<String, Object> data : batchJsonData) {
            sb.append(mapper.writeValueAsString(data));
            sb.append("\n");
        }

        return sb.toString();
    }

    public void handleBatch(List<Map<String, Object>> data, String batchName) throws IOException, SQLException {
        String jsonStr = generateNDJsonData(data);
        byte[] jsonData = jsonStr.getBytes();

        final int maxRetries = 3;  // 设置最大重试次数
        int attempt = 0;
        while (attempt < maxRetries) {
            try (InputStream is = new ByteArrayInputStream(jsonData)) {
                Instant currentTime = Instant.now();
                String fileName = "databend-ingest-" + currentTime.toString() + ".ndjson";
                fileName = fileName.replace(":", "-");
                // upload to stage @~
                Instant uploadStart = Instant.now();
                this.databendConnection.uploadStream(null, "", is, fileName, jsonData.length, false);
                Instant uploadEnd = Instant.now();
                System.out.println("Uploaded file to stage: " + fileName + " in " + uploadEnd.minusMillis(uploadStart.toEpochMilli()).toEpochMilli() + "ms");
                // kafka record is tableName + fileName, format is: "tableName:fileName:'batch1','batch2',..."
//                String batchesStr = "'" + String.join("','", batchNames) + "'";
                String batchesStr = batchName;
                String tableFileInfoRecord = Config.getDatabendTmpTable() + ":" + fileName + ":" + batchesStr;
                this.stringProducer.sendStringToKafka(Config.getKafkaFileTopic(), tableFileInfoRecord);
                System.out.println("sended file info to kafka: " + tableFileInfoRecord);
                break;  // if success, break the loop
            } catch (Exception e) {
                attempt++;
                if (attempt == maxRetries) {
                    e.printStackTrace();
                    throw new RuntimeException("Failed to handle batch after " + maxRetries + " attempts", e);
                }
                // if failed, sleep for a while and retry
                try {
                    Thread.sleep(1000 * attempt);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(ie);
                }
            }
        }
    }

    public void close() {
        this.stringProducer.closeProducer();
    }


    public void run() {
        for (int i = 0; i < Config.getDatabendTestBatchSize(); i++) {
            String batch = Config.getDatabendBatchName() + i;
            List<Map<String, Object>> batchData = genBatch(batch, Config.getDatabendTestFileBatchSize());
            try {
                System.out.println("handle batch: " + batch);
                handleBatch(batchData, batch);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}