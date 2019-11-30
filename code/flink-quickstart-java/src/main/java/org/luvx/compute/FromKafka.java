package org.luvx.compute;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.luvx.entity.UserBehavior;
import org.luvx.utils.KafkaUtils;
import org.luvx.utils.KafkaUtils2;
import org.springframework.beans.BeanUtils;

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
        // operator = map(operator);
        operator = flatMap(operator);

        operator.print();
        env.execute("compute from kafka");
    }

    private static SingleOutputStreamOperator<UserBehavior> map(DataStreamSource<String> stream) {
        return stream.map(s -> JSON.parseObject(s, UserBehavior.class));
    }

    private static SingleOutputStreamOperator<UserBehavior> map(SingleOutputStreamOperator<UserBehavior> operator) {
        return operator.map(
                s -> {
                    UserBehavior t = UserBehavior.builder().build();
                    BeanUtils.copyProperties(s, t);
                    t.setTimestamp(s.getTimestamp() / 1000);
                    return t;
                }
        );
    }

    private static SingleOutputStreamOperator<UserBehavior> flatMap(SingleOutputStreamOperator<UserBehavior> operator) {
        return operator.flatMap(
                new FlatMapFunction<UserBehavior, UserBehavior>() {
                    @Override
                    public void flatMap(UserBehavior userBehavior, Collector<UserBehavior> collector) throws Exception {
                        if (userBehavior.getUserId() % 2 == 0) {
                            collector.collect(userBehavior);
                        }
                    }
                }
        );
    }

    private static SingleOutputStreamOperator<UserBehavior> filter(SingleOutputStreamOperator<UserBehavior> operator) {
        return operator.filter(
                userBehavior -> Objects.equals(userBehavior.getBehavior(), "pv")
        );
    }
}
