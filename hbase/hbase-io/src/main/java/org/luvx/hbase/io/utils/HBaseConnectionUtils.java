package org.luvx.hbase.io.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;
import java.util.Objects;
import java.util.Properties;

/**
 * @author Ren, Xie
 */
@Slf4j
public class HBaseConnectionUtils {
    public static Connection getConnection() {
        Connection connection = null;
        try {
            connection = ConnectionFactory.createConnection(getConfiguration());
        } catch (IOException e) {
            log.error("创建HBase连接异常");
        }
        return connection;
    }

    private static Configuration getConfiguration() {
        Properties props = PropertiesUtils.load("hbase.properties");
        Objects.requireNonNull(props, "加载配置文件异常");

        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.property.clientPort", props.getProperty("hbase.zookeeper.property.clientPort"));
        config.set("hbase.zookeeper.quorum", props.getProperty("hbase.zookeeper.quorum"));
        return config;
    }
}
