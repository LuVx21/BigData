package org.luvx.hive.io.utils;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Properties;


/**
 * @author Ren, Xie
 */
@Slf4j
public class HiveConnectionUtils {
    public static Connection getConnection() throws SQLException {
        Properties props = PropertiesUtils.load("hive.properties");
        Objects.requireNonNull(props, "配置文件加载异常");

        String drive = props.getProperty("hive.drive");
        String url = props.getProperty("hive.url");
        String user = props.getProperty("hive.user");
        String password = props.getProperty("hive.password");

        try {
            Class.forName(drive);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("加载hive.drive异常");
        }

        java.sql.Connection conn = DriverManager.getConnection(url, user, password);
        return conn;
    }
}
