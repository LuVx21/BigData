package org.luvx.hadoop.jobs.partitioner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.Objects;

/**
 * @author Ren, Xie
 */
public class MyPartitioner extends Partitioner<Text, LongWritable> {
    @Override
    public int getPartition(Text key, LongWritable value, int numPartitions) {
        String k = key.toString();
        if (Objects.equals("xiaomi", k)) {
            return 0;
        }
        if (Objects.equals("huawei", k)) {
            return 1;
        }
        if (Objects.equals("iphone7", k)) {
            return 2;
        }
        return 3;
    }
}