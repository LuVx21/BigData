package org.luvx.window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.luvx.entity.LogEvent;

import java.sql.Timestamp;

/**
 * @ClassName: org.luvx.window
 * @Description:
 * @Author: Ren, Xie
 */
public class Main1 {
    private static final String host = "192.168.224.129";
    private static final int    port = 9000;

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.socketTextStream(host, port, "\n");

        SingleOutputStreamOperator<LogEvent> operator = source.map(
                new MapFunction<String, LogEvent>() {
                    @Override
                    public LogEvent map(String s) throws Exception {
                        String[] tokens = s.toLowerCase().split("\\s+");
                        return LogEvent.of(tokens);
                    }
                }
        );
        operator.print("事件");

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        SingleOutputStreamOperator<LogEvent> operator1 = operator.assignTimestampsAndWatermarks(
                new AscendingTimestampExtractor<LogEvent>() {
                    @Override
                    public long extractAscendingTimestamp(LogEvent element) {
                        return element.getEventTime();
                    }
                }
        );

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        sql(tEnv, operator);

        env.execute("table window");
    }

    /**
     * <pre>
     * select
     *   tumble_start(eventTime, interval '10' second) as window_start,
     *   tumble_end(eventTime, interval '10' second) as window_end,
     *   count(url)
     * from log
     * group by tumble(eventTime, interval '10' second)
     * </pre>
     *
     * @param tEnv
     * @param operator
     */
    private static void sql(StreamTableEnvironment tEnv, SingleOutputStreamOperator<LogEvent> operator) {
        tEnv.registerDataStream("log", operator, "ip, eventTime.rowtime, timeZone, type, url");

        StringBuilder sb = new StringBuilder();
        sb.append(" select");
        sb.append("   tumble_start(eventTime, interval '10' second) as window_start,");
        sb.append("   tumble_end(eventTime, interval '10' second) as window_end,");
        sb.append("   count(url)");
        sb.append(" from log");
        sb.append(" group by tumble(eventTime, interval '10' second)");

        Table result = tEnv.sqlQuery(sb.toString());

        TypeInformation<Tuple3<Timestamp, Timestamp, Long>> type =
                new TypeHint<Tuple3<Timestamp, Timestamp, Long>>() {
                }.getTypeInfo();

        tEnv.toAppendStream(result, type).print("result");
    }
}
