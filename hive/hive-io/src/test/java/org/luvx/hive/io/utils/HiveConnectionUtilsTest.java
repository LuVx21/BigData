package org.luvx.hive.io.utils;

import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class HiveConnectionUtilsTest {
    Connection conn = null;

    @Before
    public void before() {
        try {
            conn = HiveConnectionUtils.getConnection();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void createTableTest() throws SQLException {
        String sql = "create table t_student (id int, name string) partitioned by (pt_dt string)";
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        }
    }

    @Test
    public void insertTest() throws SQLException {
        String sql = "insert into table t_student partition(pt_dt = '20191111') values(1,'luvx')";
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        }
    }

    @Test
    public void selectTest() throws SQLException {
        String sql = "select * from t_student";
        try (Statement stmt = conn.createStatement()) {
            try (ResultSet rs = stmt.executeQuery(sql)) {
                while (rs.next()) {
                    System.out.println(rs.getString("id") + "\t" + rs.getString("name"));
                }
            }
        }
    }

    @Test
    public void loadDataTest() throws SQLException {
        String filePath = "./data/student.txt";
        String sql = "load data local inpath '" + filePath + "' overwrite into table t_student";
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        }
    }
}