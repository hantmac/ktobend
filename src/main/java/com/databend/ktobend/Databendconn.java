package com.databend.ktobend;

import java.sql.*;
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

    public void copyInto(String tableName, List<String> files) throws SQLException {
        String filesStr = "'" + String.join("','", files) + "'";
        String copyIntoSql = String.format("copy into %s from @~ files=(%s) file_format=(type=NDJSON) purge=true;", tableName, filesStr);
        Connection connection = createConnection();
        try (Statement statement = connection.createStatement()) {
            statement.execute(copyIntoSql);
            System.out.println("Copied files into " + tableName);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}

