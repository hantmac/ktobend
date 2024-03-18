package com.databend.ktobend;

import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

public class Config {
    private static final String CONFIG_FILE = "config.properties";

    private static final Properties properties = new Properties();

    static {
        try {
            InputStream input;
            if (Files.exists(Paths.get(CONFIG_FILE))) {
                // if the config file exists in the current directory, load it
                input = new FileInputStream(CONFIG_FILE);
            } else {
                // 否则，从 JAR 文件中加载
                input = Config.class.getClassLoader().getResourceAsStream(CONFIG_FILE);
                if (input == null) {
                    System.err.println("Sorry, unable to find " + CONFIG_FILE);
                    System.exit(1);
                }
            }
            properties.load(input);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    public static String getKafkaBootstrapServers() {
        return properties.getProperty("kafka.bootstrap.servers");
    }

    public static String getKafkaConsumerGroupIdJson() {
        return properties.getProperty("kafka.consumer.group.id.json");
    }

    public static String getKafkaConsumerGroupIdFile() {
        return properties.getProperty("kafka.consumer.group.id.file");
    }

    public static String getKafkaJsonTopic() {
        return properties.getProperty("kafka.json.topic");
    }

    public static String getKafkaFileTopic() {
        return properties.getProperty("kafka.file.topic");
    }


    public static String getDatabendDsn() {
        return properties.getProperty("databend.dsn");
    }

    public static String getDatabendTmpTable() {
        return properties.getProperty("databend.tmpTable");
    }

    public static String getDatabendUser() {
        return properties.getProperty("databend.user");
    }

    public static String getDatabendPassword() {
        return properties.getProperty("databend.password");
    }

    public static String getDatabendTargetTable() {
        return properties.getProperty("databend.targetTable");
    }

    public static int getDatabendInterval() {
        // seconds
        String intervalStr = properties.getProperty("databend.interval");
        try {
            return Integer.parseInt(intervalStr);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid integer value for databend.interval: " + intervalStr);
        }
    }

    public static int getDatabendBatchSize() {
        String batchSizeStr = properties.getProperty("databend.batch.size");
        try {
            return Integer.parseInt(batchSizeStr);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid integer value for databend.batch.size: " + batchSizeStr);
        }
    }

    public static int getDatabendFileBatchSize() {
        String batchSizeStr = properties.getProperty("databend.file.batch.size");
        try {
            return Integer.parseInt(batchSizeStr);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid integer value for databend.file.batch.size: " + batchSizeStr);
        }
    }

    public static int getDatabendTestBatchSize() {
        String batchSizeStr = properties.getProperty("databend.test.batch");
        try {
            return Integer.parseInt(batchSizeStr);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid integer value for databend.test.batch.size: " + batchSizeStr);
        }
    }

    public static int getDatabendTestFileBatchSize() {
        String batchSizeStr = properties.getProperty("databend.test.file.batch");
        try {
            return Integer.parseInt(batchSizeStr);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid integer value for databend.test.file.batch.size: " + batchSizeStr);
        }
    }

    public static String getDatabendBatchName() {
        return properties.getProperty("databend.batch.name");
    }
}


