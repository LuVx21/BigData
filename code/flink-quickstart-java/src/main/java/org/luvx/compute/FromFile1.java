package org.luvx.compute;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.util.Collector;
import org.luvx.entity.UserBehavior;

import java.net.URL;

public class FromFile1 {

    private static final String LOCAL_LOCATION = "datasource/UserBehaviorTest.txt";

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        URL url = FromFile1.class.getClassLoader().getResource(LOCAL_LOCATION);
        String filePath = url.getPath();

        // compute1(env, filePath);
        compute2(env, filePath);

        env.execute("compute Hot Items");
    }

    private static void compute1(StreamExecutionEnvironment env, String filePath) {
        DataStreamSource<String> dataStreamSource = env.readTextFile(filePath);
        dataStreamSource.setParallelism(1);

        SingleOutputStreamOperator<UserBehavior> operator = dataStreamSource.map(
                (s) -> {
                    String[] tokens = s.split("\\W+");
                    return a(tokens);
                }
        );
        operator.print();
    }

    private static void compute2(StreamExecutionEnvironment env, String filePath) {
        TextInputFormat inputFormat = new TextInputFormat(new Path(filePath));
        DataStreamSource<String> dataStreamSource = env.readFile(
                inputFormat, filePath, FileProcessingMode.PROCESS_CONTINUOUSLY,
                100L, TypeExtractor.getInputFormatTypes(inputFormat));
        dataStreamSource.setParallelism(1);

        SingleOutputStreamOperator<UserBehavior> operator = dataStreamSource.flatMap(
                new FlatMapFunction<String, UserBehavior>() {
                    @Override
                    public void flatMap(String s, Collector<UserBehavior> collector) throws Exception {
                        String[] tokens = s.split("\\W+");
                        if (tokens.length > 1) {
                            collector.collect(a(tokens));
                        }
                    }
                }
        );
        operator.print();
    }

    private static UserBehavior a(String[] tokens) {
        return UserBehavior.builder()
                .userId(Long.valueOf(tokens[0]))
                .itemId(Long.valueOf(tokens[1]))
                .categoryId(Integer.valueOf(tokens[2]))
                .behavior(tokens[3])
                .timestamp(Long.valueOf(tokens[4]))
                .build();
    }
}
