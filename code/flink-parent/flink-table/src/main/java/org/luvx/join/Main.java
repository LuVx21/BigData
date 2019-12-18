package org.luvx.join;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.luvx.join.entity.Result;
import org.luvx.join.entity.Score;
import org.luvx.join.entity.Student;

/**
 * @ClassName: org.luvx.join
 * @Description: ↓
 * 动态表, 静态表
 * 表间join(流join)
 * @Author: Ren, Xie
 */
public class Main {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        DataStreamSource<Tuple2<Student, Score>> source = env.addSource(new Source());
        SingleOutputStreamOperator<Student> s = source.map(new MapFunction<Tuple2<Student, Score>, Student>() {
            @Override
            public Student map(Tuple2<Student, Score> value) throws Exception {
                return value.getField(0);
            }
        });
        SingleOutputStreamOperator<Score> ss = source.map(new MapFunction<Tuple2<Student, Score>, Score>() {
            @Override
            public Score map(Tuple2<Student, Score> value) throws Exception {
                return value.getField(1);
            }
        });

        /// table
        // Table student = tEnv.fromDataStream(s, "id, name");
        // Table score = tEnv.fromDataStream(ss, "eid, sid, score");
        // Table t = student.join(score, "id = sid")
        //         .select("id, name, score");

        /// sql
        tEnv.registerDataStream("student", s, "id, name");
        tEnv.registerDataStream("score", ss, "eid, sid, score");
        Table t = tEnv.sqlQuery("select a.id, a.name, b.score from student a, score b where a.id = b.sid");

        tEnv.toAppendStream(t, Result.class).print("result");

        env.execute("table join");
    }
}