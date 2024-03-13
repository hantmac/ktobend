package com.databend.ktobend;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class ConsumerStageFileWorker {
    public KafkaJsonConsumer consumer;
    public Databendconn databendconn;
    public Integer fileSize;

    public ConsumerStageFileWorker() throws SQLException {
        this.consumer = new KafkaJsonConsumer(Config.getKafkaFileTopic());
        this.fileSize = Config.getDatabendBatchSize();
        this.databendconn = new Databendconn();
    }

    public void consumeFileAndCopyInto() throws Exception {
        List<String> files = new ArrayList<>();
        while (true) {
            try {
                ConsumerRecord<String, String> record = fetchMessageWithTimeout(java.time.Duration.ofSeconds(100));
                System.out.println("Received message: " + record.value() + " from offset: " + record.offset());
                // split the record into tableName and fileName
                String[] tableFileInfo = record.value().split(":");
                String tableName = tableFileInfo[0];
                String fileName = tableFileInfo[1];
                files.add(fileName);
                if (files.size() > fileSize) {
                    this.databendconn.copyInto(tableName, files);
                    break;
                }
            } catch (Exception e) {
                throw new Exception(e);
            }
        }
    }

    public ConsumerRecord<String, String> fetchMessageWithTimeout(Duration timeOut) throws Exception {
        try {
            return this.consumer.getConsumer().poll(timeOut).iterator().next();
        } catch (Exception e) {
            throw new Exception(e);
        }
    }

    public void close() {
        this.consumer.closeConsumer();
    }

    public void run() {
        while (true) {
            try {
                consumeFileAndCopyInto();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

}
