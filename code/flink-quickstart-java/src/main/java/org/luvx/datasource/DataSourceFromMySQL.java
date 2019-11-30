package org.luvx.datasource;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.luvx.entity.UserBehavior;

import java.sql.*;

public class DataSourceFromMySQL extends RichSourceFunction<UserBehavior> {

    private Connection        conn;
    private PreparedStatement stmt;

    @Override
    public void run(SourceContext<UserBehavior> sourceContext) {
        try (ResultSet rs = stmt.executeQuery()) {
            while (rs.next()) {
                UserBehavior u = UserBehavior.builder()
                        .userId(rs.getLong("user_id"))
                        .itemId(rs.getLong("item_id"))
                        .categoryId(rs.getInt("category_id"))
                        .behavior(rs.getString("behavior"))
                        .timestamp(rs.getLong("timestamp"))
                        .build();
                sourceContext.collect(u);
            }
        } catch (SQLException e) {
        }
    }

    @Override
    public void cancel() {
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        conn = getConnection();
        String sql = "select * from user_behavior;";
        stmt = this.conn.prepareStatement(sql);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (conn != null) {
            conn.close();
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
