package org.luvx.wc;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.luvx.entity.WordWithCount;

/**
 * @ClassName: org.luvx.wordcount
 * @Description:
 * @Author: Ren, Xie
 */
public class WordCountTableMain {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);

        DataSet<WordWithCount> input = env.fromElements(
                new WordWithCount("Hello", 1),
                new WordWithCount("Ciao", 1),
                new WordWithCount("Hello", 1));

        Table table = tEnv.fromDataSet(input);

        Table filtered = table
                .groupBy("word")
                .select("word, cnt.sum as cnt")
                .filter("cnt = 2");

        DataSet<WordWithCount> result = tEnv.toDataSet(filtered, WordWithCount.class);

        result.print();
    }
}

