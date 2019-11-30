package org.luvx.compute;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.luvx.datasource.DataSourceFromMySQL;
import org.luvx.entity.UserBehavior;

public class FromMySQL {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<UserBehavior> stream = env.addSource(new DataSourceFromMySQL());

        stream.print();
        env.execute("xxx");
    }
}
