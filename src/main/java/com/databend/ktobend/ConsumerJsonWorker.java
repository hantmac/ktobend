package com.databend.ktobend;

import com.databend.jdbc.DatabendConnection;

import java.io.*;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;

/*
 * This class is responsible for consuming messages from Kafka, and then presign upload to s3 stage,
 * send the tableName:fileName to kafka.
 * The messages are in JSON format, and are uploaded to Databend in NDJSON format.
 */
public class ConsumerJsonWorker {
    KafkaBatchReader batchReader;
    KafkaStringProducer stringProducer;
    DatabendConnection databendConnection;

    public ConsumerJsonWorker() throws SQLException {
        KafkaJsonConsumer consumer = new KafkaJsonConsumer(Config.getKafkaJsonTopic(), Config.getKafkaConsumerGroupIdJson());
        int batchSize = Config.getDatabendBatchSize();
        this.batchReader = new KafkaBatchReader(consumer, batchSize, java.time.Duration.ofSeconds(100));
        this.stringProducer = new KafkaStringProducer();
        this.databendConnection = (DatabendConnection) Databendconn.createConnection();
    }

    public void close() {
        this.batchReader.close();
        this.stringProducer.closeProducer();
    }

    public void handleBatch(int i) throws IOException, SQLException {
        MessagesBatch batch = this.batchReader.readBatch();
        System.out.println(i);
        if (batch.Empty()) {
            System.out.println("batch is empty");
            return;
        }
        File jsonFile = generateNDJsonFile(batch.getMessages());

        final int maxRetries = 3;  // 设置最大重试次数
        int attempt = 0;
        while (attempt < maxRetries) {
            try (FileInputStream fis = new FileInputStream(jsonFile)) {
                String fileName = jsonFile.getName();
                // upload to stage @~
                this.databendConnection.uploadStream(null, "", fis, fileName, jsonFile.length(), false);
                System.out.println("Uploaded file to stage: " + fileName);
                // kafka record is tableName + fileName, format is: "tableName:fileName:'batch1','batch2',..."
                String batchesStr = "'" + String.join("','", batch.getBatches()) + "'";
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

        jsonFile.delete();
    }

    public File generateNDJsonFile(List<String> batchJsonData) throws IOException {
        File tempDir = new File(System.getProperty("java.io.tmpdir"));
        File tempFile = new File(tempDir, "databend-ingest-" + UUID.randomUUID().toString() + ".ndjson");

        int bytesSum = 0;
        try (FileWriter fw = new FileWriter(tempFile)) {
            for (String data : batchJsonData) {

                fw.write(data + "\n");
                bytesSum += data.getBytes().length;
            }
            fw.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return tempFile;
    }

    public void run() {
        int i = 0;
        while (true) {
            try {
                i++;
                handleBatch(i);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

}
