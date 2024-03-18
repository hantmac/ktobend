package com.databend.ktobend;

import java.sql.*;
import java.time.Instant;
import java.util.List;

public class Databendconn {
    static String databendDsn = Config.getDatabendDsn();


    public static Connection createConnection()
            throws SQLException {
        try {
            Class.forName("com.databend.jdbc.DatabendDriver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return DriverManager.getConnection(databendDsn, Config.getDatabendUser(), Config.getDatabendPassword());
    }

    public String exec(String sql) throws SQLException {
        Connection connection = createConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql);
        resultSet.next();
        return resultSet.getString(1);
    }

    public void copyInto(String tableName, List<String> files) throws Exception {
        String filesStr = "'" + String.join("','", files) + "'";
        String copyIntoSql = String.format("copy into %s from @~ files=(%s) file_format=(type=NDJSON) purge=true;", tableName, filesStr);
        Connection connection = createConnection();
        try (Statement statement = connection.createStatement()) {
            Instant copyIntoStart = Instant.now();
            statement.execute(copyIntoSql);
            Instant copyIntoEnd = Instant.now();
            System.out.println("Copied files into: " + files.size() + " , time elapsed: " + (copyIntoEnd.toEpochMilli() - copyIntoStart.toEpochMilli()) + "ms");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void mergeInto(String batches) throws Exception {
        String originalMergeSql = "merge into %s using (select * from %s where batch in (%s)) b on %s.id = b.id  when matched and %s.t < b.t then \n" +
                "update * \n" +
                "when not matched then\n" +
                "insert *";
        String sourceTable = Config.getDatabendTmpTable();
        String targetTable = Config.getDatabendTargetTable();
        String mergeIntoSql = String.format(originalMergeSql, targetTable, sourceTable, batches, targetTable, targetTable);
        Connection connection = createConnection();
        try (Statement statement = connection.createStatement()) {
            statement.execute("set enable_experimental_merge_into = 1");
            Instant mergeIntoStart = Instant.now();
            statement.execute(mergeIntoSql);
            Instant mergeIntoEnd = Instant.now();
            System.out.println("Merged stage into: " + mergeIntoSql + " , time elapsed: " + (mergeIntoEnd.toEpochMilli() - mergeIntoStart.toEpochMilli()) + "ms");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}


