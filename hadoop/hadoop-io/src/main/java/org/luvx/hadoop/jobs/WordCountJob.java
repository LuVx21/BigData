package org.luvx.hadoop.jobs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.luvx.hadoop.jobs.mapper.MyMapper;
import org.luvx.hadoop.jobs.reducer.MyReducer;
import org.luvx.hadoop.utils.HadoopConnectionUtils;

/**
 * 使用MapReduce开发WordCount应用程序
 */
public class WordCountJob {


    /**
     * 定义Driver：封装了MapReduce作业的所有信息
     */
    public static void main(String[] args) throws Exception {
        //创建Configuration
        // Configuration conf = new Configuration();
        Configuration conf = HadoopConnectionUtils.getConfig();

        //创建Job
        Job job = Job.getInstance(conf, "word-count");
        //设置job的处理类
        job.setJarByClass(WordCountJob.class);

        //设置作业处理的输入路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));

        //设置map相关参数
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        //设置reduce相关参数
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //设置作业处理的输出路径
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
