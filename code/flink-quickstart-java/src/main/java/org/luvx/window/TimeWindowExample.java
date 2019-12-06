package org.luvx.window;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.luvx.utils.TimeUtils;
import org.luvx.window.entity.Log;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @ClassName: org.luvx.window
 * @Description:
 * @Author: Ren, Xie
 * @Date: 2019/12/6 9:13
 */
@Slf4j
public class TimeWindowExample {
    private static final String host = "192.168.224.129";
    private static final int    port = 9000;

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> stream = env.socketTextStream(host, port, "\n");
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        SingleOutputStreamOperator<Log> operator = stream.map(new MapFunction<String, Log>() {
            @Override
            public Log map(String s) throws Exception {
                String[] tokens = s.toLowerCase().split("\\s+");
                return Log.of(tokens);
            }
        });

        operator = operator.assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor<Log>(Time.seconds(3)) {
                    @Override
                    public long extractTimestamp(Log element) {
                        log.info("当前水位线:{}", TimeUtils.epochMilli2Time(getCurrentWatermark().getTimestamp()));
                        return element.getTime();
                    }
                }
                // new AscendingTimestampExtractor<Log>() {
                //     @Override
                //     public long extractAscendingTimestamp(Log element) {
                //         return element.getTime();
                //     }
                // }
        );

        operator.print("luvx");

        SingleOutputStreamOperator<Long> operator1 = operator
                .timeWindowAll(Time.seconds(10), Time.seconds(5))
                .apply(
                        new AllWindowFunction<Log, Long, TimeWindow>() {
                            @Override
                            public void apply(TimeWindow window, Iterable<Log> values, Collector<Long> out) throws Exception {
                                List<Log> logs = new ArrayList<>();
                                Iterator<Log> it = values.iterator();
                                while (it.hasNext()) {
                                    Log log = it.next();
                                    logs.add(log);
                                }
                                log.info("所有事件:{}", logs);
                                log.info("窗口时间:{} ~ {}", TimeUtils.epochMilli2Time(window.getStart()), TimeUtils.epochMilli2Time(window.getEnd()));

                                out.collect((long) logs.size());
                            }
                        }
                );
        operator1.print("count");

        env.execute("window test example");
    }
}
