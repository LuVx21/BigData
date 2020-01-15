package org.luvx.join;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.luvx.common.value.Const;
import org.luvx.entity.LogEvent;
import org.luvx.entity.join.Score;
import org.luvx.entity.join.Student;
import org.luvx.entity.join.StudentScore;
import org.luvx.source.StudentScoreSource;

/**
 * @ClassName: org.luvx.join
 * @Description: 双流 流中事件重复时 join 的问题
 * 双流 join 时, 某个流中的某个数据 出现了两次 导致的重复计算
 * 测试用数据, 数字是输入顺序
 * <pre>
 *     student:
 * 1    11,foo
 * 4    11,foo1
 *     score:
 * 2    100,math,11,97
 * 3    101,english,11,98
 * </pre>
 * @Author: Ren, Xie
 */
public class JoinMain1 {

    private static final String host  = "127.0.0.1";
    private static final int    port  = 9000;
    private static final int    port1 = 9001;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        SingleOutputStreamOperator<Student> student = env.socketTextStream(host, port, "\n")
                .map(new MapFunction<String, Student>() {
                         @Override
                         public Student map(String s) throws Exception {
                             String[] tokens = s.toLowerCase().split(",");
                             Student ss = new Student();
                             ss.setId(Integer.valueOf(tokens[0]));
                             ss.setName(tokens[1]);
                             return ss;
                         }
                     }
                );

        SingleOutputStreamOperator<Score> score = env.socketTextStream(host, port1, "\n")
                .map(new MapFunction<String, Score>() {
                         @Override
                         public Score map(String s) throws Exception {
                             String[] tokens = s.toLowerCase().split(",");
                             Score ss = new Score();
                             ss.setId(Integer.valueOf(tokens[0]));
                             ss.setName(tokens[1]);
                             ss.setSid(Integer.valueOf(tokens[2]));
                             ss.setScore(Integer.valueOf(tokens[3]));
                             return ss;
                         }
                     }
                );

        /// sql
        tEnv.registerDataStream("student", student, "id, name");
        tEnv.registerDataStream("score", score, "id, name, sid, score");
        Table t = tEnv.sqlQuery("select sum(t2.score) from student t1 left join score t2 on t1.id = t2.sid");

        tEnv.toRetractStream(t, Integer.class).print("result");

        env.execute("table join");
    }
}