package org.luvx.hadoop.jobs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.luvx.hadoop.jobs.mapper.MyMapper1;
import org.luvx.hadoop.jobs.partitioner.MyPartitioner;
import org.luvx.hadoop.jobs.reducer.MyReducer;
import org.luvx.hadoop.utils.HadoopConnectionUtils;
import org.luvx.hadoop.utils.HadoopUtils;

/**
 * @author Ren, Xie
 */
public class ParititonerJob {
    /**
     * 定义Driver：封装了MapReduce作业的所有信息
     */
    public static void main(String[] args) throws Exception {
        Configuration conf = HadoopConnectionUtils.getConfig();
        FileSystem fs = HadoopConnectionUtils.getFileSystem(conf);
        HadoopUtils.deleteFile(fs, args[1]);

        //创建Job
        Job job = Job.getInstance(conf, "word-count");

        //设置job的处理类
        job.setJarByClass(ParititonerJob.class);

        //设置作业处理的输入路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));

        //设置map相关参数
        job.setMapperClass(MyMapper1.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        //设置reduce相关参数
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //设置job的partition
        job.setPartitionerClass(MyPartitioner.class);
        //设置4个reducer，每个分区一个
        job.setNumReduceTasks(4);

        //设置作业处理的输出路径
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}















