package org.luvx.compute;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.io.PojoCsvInputFormat;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.luvx.entity.*;

import java.io.File;
import java.net.URL;

public class FromFile {

    private static final String LOCAL_LOCATION = "datasource/UserBehavior.csv";

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        URL url = FromFile.class.getClassLoader().getResource(LOCAL_LOCATION);
        String path = url.getPath();
        Path filePath = Path.fromLocalFile(new File(path));

        PojoTypeInfo<UserBehavior> pojoTypeInfo = (PojoTypeInfo<UserBehavior>) TypeExtractor.createTypeInfo(UserBehavior.class);
        String[] column = new String[]{"userId", "itemId", "categoryId", "behavior", "timestamp"};
        PojoCsvInputFormat<UserBehavior> csvInputFormat = new PojoCsvInputFormat<>(filePath, pojoTypeInfo, column);

        DataStream<UserBehavior> dataSource = env.createInput(csvInputFormat, pojoTypeInfo);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<UserBehavior> timeData = dataSource
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
                    @Override
                    public long extractAscendingTimestamp(UserBehavior element) {
                        // 原始单位是秒，需要乘以 1000 ，转换成毫秒
                        return element.getTimestamp() * 1000;
                    }
                });

        // 使用过滤算子 filter，筛选出操作行为中是 pv 的数据
        DataStream<UserBehavior> pvData = timeData
                .filter(new FilterFunction<UserBehavior>() {
                    @Override
                    public boolean filter(UserBehavior value) throws Exception {
                        return "pv".equals(value.getBehavior());
                    }
                });

        // 设定滑动窗口 sliding window，每隔五分钟统计最近一个小时的每个商品的点击量
        // 经历过程 dataStream -> keyStream -> dataStream
        DataStream<ItemViewCount> windowData = pvData
                .keyBy("itemId")
                .timeWindow(Time.minutes(60), Time.minutes(5))
                .aggregate(new CountAgg(), new WindowResultFunction());

        // 统计最热门商品
        DataStream<String> topItems = windowData
                .keyBy("windowEnd")
                .process(new TopNHotItems(3));

        topItems.print();
        env.execute("Test Hot Items Job");
    }
}

