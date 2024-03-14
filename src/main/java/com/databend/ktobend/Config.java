package com.databend.ktobend;

import java.io.InputStream;
import java.util.Properties;

public class Config {
    private static final String CONFIG_FILE = "config.properties";

    private static final Properties properties = new Properties();

    static {
        try (InputStream input = Config.class.getClassLoader().getResourceAsStream(CONFIG_FILE)) {
            if (input == null) {
                System.err.println("Sorry, unable to find " + CONFIG_FILE);
                System.exit(1);
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

    public static String getKafkaConsumerGroupId() {
        return properties.getProperty("kafka.consumer.group.id");
    }

    public static String getKafkaJsonTopic() {
        return properties.getProperty("kafka.json.topic");
    }

    public static String getKafkaFileTopic() {
        return properties.getProperty("kafka.file.topic");
    }

    public static String getOutputDirectory() {
        return properties.getProperty("output.directory");
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
}

