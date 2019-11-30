package org.luvx.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;

public class SinkToMySQL extends RichSinkFunction<Object> {
    private Connection        connection;
    private PreparedStatement stmt;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // String sql = "insert into student(name, age, address) values (?, ?, ?);";
        // ps = connection.prepareStatement(sql);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (connection != null) {
            connection.close();
        }
        if (stmt != null) {
            stmt.close();
        }
    }
}
