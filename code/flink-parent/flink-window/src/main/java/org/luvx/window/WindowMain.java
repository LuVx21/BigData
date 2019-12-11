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
import org.luvx.entity.LogEvent;
import org.luvx.common.utils.DateTimeUtils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @ClassName: org.luvx
 * @Description: kc -lk 9000  使用request_log.txt中数据
 * @Author: Ren, Xie
 */
@Slf4j
public class WindowMain {
    private static final String host = "192.168.224.129";
    private static final int    port = 9000;

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> stream = env.socketTextStream(host, port, "\n");
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        SingleOutputStreamOperator<LogEvent> operator = stream.map(
                new MapFunction<String, LogEvent>() {
                    @Override
                    public LogEvent map(String s) throws Exception {
                        String[] tokens = s.toLowerCase().split("\\s+");
                        return LogEvent.of(tokens);
                    }
                }
        );

        operator = operator.assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor<LogEvent>(Time.seconds(3)) {
                    @Override
                    public long extractTimestamp(LogEvent element) {
                        log.info("当前水位线:{}", DateTimeUtils.epochMilli2Time(getCurrentWatermark().getTimestamp()));
                        return element.getTime();
                    }
                }
        );

        operator.print("luvx");

        SingleOutputStreamOperator<Long> operator1 = operator
                .timeWindowAll(Time.seconds(10), Time.seconds(5))
                .apply(
                        new AllWindowFunction<LogEvent, Long, TimeWindow>() {
                            @Override
                            public void apply(TimeWindow window, Iterable<LogEvent> values, Collector<Long> out) throws Exception {
                                log.info("----------------- 窗口汇总↓ -----------------");
                                List<LogEvent> logs = new ArrayList<>();
                                Iterator<LogEvent> it = values.iterator();
                                while (it.hasNext()) {
                                    LogEvent log = it.next();
                                    logs.add(log);
                                }
                                log.info("所有事件({}个事件):{}", logs.size(), logs);
                                log.info("窗口时间:{} ~ {}", DateTimeUtils.epochMilli2Time(window.getStart()), DateTimeUtils.epochMilli2Time(window.getEnd()));
                                log.info("----------------- 窗口汇总↑ -----------------");

                                out.collect((long) logs.size());
                            }
                        }
                );
        operator1.print("count");

        env.execute("window test example");
    }
}
