package com.databend.ktobend;

import com.databend.jdbc.DatabendConnection;

import java.io.*;
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

    public ConsumerJsonWorker() {
        KafkaJsonConsumer consumer = new KafkaJsonConsumer(Config.getKafkaJsonTopic());
        int batchSize = Config.getDatabendBatchSize();
        this.batchReader = new KafkaBatchReader(consumer, batchSize, java.time.Duration.ofSeconds(100));
        this.stringProducer = new KafkaStringProducer();
    }

    public void close() {
        this.batchReader.close();
        this.stringProducer.closeProducer();
    }

    public void handleBatch() throws IOException {
        MessagesBatch batch = this.batchReader.readBatch();
        if (batch.Empty()) {
            return;
        }
        File jsonFile = generateNDJsonFile(batch.getMessages());


        try (FileInputStream fis = new FileInputStream(jsonFile);) {
            DatabendConnection c = (DatabendConnection) Databendconn.createConnection();
            String fileName = jsonFile.getName();
            // upload to stage @~
            c.uploadStream(null, "", fis, fileName, jsonFile.length(), false);
            System.out.println("Uploaded file to stage: " + fileName);
            // kafka record is tableName + fileName, format is: "tableName:fileName:'batch1','batch2',..."
            String batchesStr = "'" + String.join("','", batch.getBatches()) + "'";
            String tableFileInfoRecord = Config.getDatabendTable() + ":" + fileName + ":" + batchesStr;
            this.stringProducer.sendStringToKafka(Config.getKafkaFileTopic(), tableFileInfoRecord);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            jsonFile.delete();
        }
    }

    public File generateNDJsonFile(List<String> batchJsonData) throws IOException {
        File tempDir = new File(System.getProperty("java.io.tmpdir"));
        File tempFile = new File(tempDir, "databend-ingest-" + UUID.randomUUID().toString() + ".json");

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
        while (true) {
            try {
                handleBatch();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

}
