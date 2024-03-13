package com.databend.ktobend;

import java.sql.SQLException;

public class Ttt {
    public static void main(String[] args) {
        Databendconn databendconn = new Databendconn();
        try {
            System.out.println(databendconn.exec("select 1"));
        } catch (SQLException  e) {
            e.printStackTrace();
        }
    }
}
