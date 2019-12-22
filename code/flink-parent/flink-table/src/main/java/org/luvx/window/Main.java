package org.luvx.window;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import java.sql.Timestamp;
import java.util.Arrays;

/**
 * @ClassName: org.luvx.window
 * @Description:
 * @Author: Ren, Xie
 */
public class Main {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<Log> log = env.fromCollection(Arrays.asList(
                new Log(1572591180_000L, "xiao_ming", 300),
                new Log(1572591189_000L, "zhang_san", 303),
                new Log(1572591192_000L, "xiao_li", 204),
                new Log(1572591201_000L, "li_si", 208)
        ));

        SingleOutputStreamOperator<Log> operator = log.assignTimestampsAndWatermarks(
                new AscendingTimestampExtractor<Log>() {
                    @Override
                    public long extractAscendingTimestamp(Log element) {
                        return element.getEventTime();
                    }
                }
        );


        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);


        env.execute("table window");
    }


    private static void sql(StreamTableEnvironment tEnv, SingleOutputStreamOperator<Log> operator) {
        Table logT = tEnv.fromDataStream(operator, "eventTime.rowtime, name, cnt");
        Table result = tEnv.sqlQuery("select tumble_start(eventTime, interval '10' second) as window_start," +
                "tumble_end(eventTime, interval '10' second) as window_end, sum(cnt) from "
                + logT + " group by tumble(eventTime, interval '10' second)");

        // tEnv.registerDataStream("log", operator, "eventTime.rowtime, name, cnt");
        // String sql = "select tumble_start(eventTime, interval '10' second) as window_start, tumble_end(eventTime, interval '10' second) as window_end, sum(cnt) from log group by tumble(eventTime, interval '10' second)";
        // Table result = tEnv.sqlQuery(sql);

        TypeInformation<Tuple3<Timestamp, Timestamp, Integer>> type =
                new TypeHint<Tuple3<Timestamp, Timestamp, Integer>>() {
                }.getTypeInfo();

        tEnv.toAppendStream(result, type).print("result");
    }


    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Log {
        private Long    eventTime;
        private String  name;
        private Integer cnt;
    }
}
