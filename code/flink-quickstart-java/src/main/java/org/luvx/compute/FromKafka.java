package org.luvx.compute;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.luvx.entity.UserBehavior;
import org.luvx.utils.KafkaUtils;
import org.luvx.utils.KafkaUtils2;

import java.util.Objects;

public class FromKafka {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> stream = env.addSource(new FlinkKafkaConsumer<>(
                KafkaUtils2.topic,
                new SimpleStringSchema(),
                KafkaUtils.getConsumerProp()
        ));
        SingleOutputStreamOperator<UserBehavior> operator = map(stream);

        // map1(operator);
        // flatMap(operator);
        // filter(operator);
        // keyBy(operator);

        env.execute("compute from kafka");
    }

    private static SingleOutputStreamOperator<UserBehavior> map(DataStreamSource<String> stream) {
        SingleOutputStreamOperator<UserBehavior> operator = stream.map(
                s -> JSON.parseObject(s, UserBehavior.class)
        );
        return operator;
    }

    private static void map1(SingleOutputStreamOperator<UserBehavior> stream) {
        SingleOutputStreamOperator<UserBehavior> operator = stream.map(
                s -> {
                    return UserBehavior.builder()
                            .userId(s.getUserId())
                            .itemId(s.getItemId())
                            .categoryId(s.getCategoryId())
                            .behavior(s.getBehavior())
                            .timestamp(s.getTimestamp() / 1000)
                            .build();
                }
        );
        operator.print();
    }

    private static void flatMap(SingleOutputStreamOperator<UserBehavior> operator) {
        SingleOutputStreamOperator<UserBehavior> temp = operator.flatMap(
                new FlatMapFunction<UserBehavior, UserBehavior>() {
                    @Override
                    public void flatMap(UserBehavior userBehavior, Collector<UserBehavior> collector) throws Exception {
                        if (userBehavior.getUserId() % 2 == 0) {
                            collector.collect(userBehavior);
                        }
                    }
                }
        );
        temp.print();
    }

    private static void filter(SingleOutputStreamOperator<UserBehavior> operator) {
        SingleOutputStreamOperator<UserBehavior> temp = operator.filter(
                userBehavior -> Objects.equals(userBehavior.getBehavior(), "pv")
        );
        temp.print();
    }

    private static void keyBy(SingleOutputStreamOperator<UserBehavior> operator) {
        KeyedStream<UserBehavior, String> stream1 = operator.keyBy(
                new KeySelector<UserBehavior, String>() {
                    @Override
                    public String getKey(UserBehavior userBehavior) throws Exception {
                        return userBehavior.getBehavior();
                    }
                }
        );
        SingleOutputStreamOperator<UserBehavior> stream2 = stream1.reduce(
                new ReduceFunction<UserBehavior>() {
                    @Override
                    public UserBehavior reduce(UserBehavior t, UserBehavior t1) throws Exception {
                        return UserBehavior.builder()
                                .userId(t.getUserId() + t1.getUserId())
                                .build();
                    }
                });

        stream2.print();
    }
}
