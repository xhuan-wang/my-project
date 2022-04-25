package com.wxh.clickhouse;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * @author: xhuan_wang
 * @descrition:
 * @date 2022-04-25 23:57
 */
public class Test {
    public static void main(String[] args) throws Exception {
        Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
        String url = "jdbc:clickhouse://localhost:8123/default";
        String user = "default";
        String password = "";
        Connection con = DriverManager.getConnection(url, user, password);
        System.out.println(con);
        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery("select 1");
        rs.next();
        System.out.println(rs.getInt(1));
    }
}
