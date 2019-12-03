package org.luvx.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.luvx.entity.UserBehavior;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class SinkToMySQL extends RichSinkFunction<UserBehavior> {
    private Connection        connection;
    private PreparedStatement stmt;

    @Override
    public void invoke(UserBehavior value, Context context) throws Exception {
        stmt.setLong(1, value.getUserId());
        stmt.setLong(2, value.getItemId());
        stmt.setInt(3, value.getCategoryId());
        stmt.setString(4, value.getBehavior());
        stmt.setLong(5, value.getTimestamp());
        stmt.executeUpdate();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = getConnection();
        String sql = "insert into user_behavior1 values (?, ?, ?, ?, ?);";
        stmt = connection.prepareStatement(sql);
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

    private static Connection getConnection() {
        Connection conn = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/boot?useUnicode=true&characterEncoding=UTF-8", "root", "1121");
        } catch (Exception e) {

        }
        return conn;
    }
}
