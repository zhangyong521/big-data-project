package com.zhangyong.mapreduce.numsort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @Author zhangyong
 * @Date 2020/4/14 9:39
 * @Version 1.0
 *
 */
public class NumSortDriver {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration ();
        Job job = Job.getInstance (conf);
        job.setJarByClass (NumSortDriver.class);
        job.setMapperClass (NumSortMapper.class);
        job.setMapOutputKeyClass (IntWritable.class);
        job.setMapOutputValueClass (IntWritable.class);
        /**
         * 由于结果文件系统是3个，所以需要在此指定Reduce的分区类和任务数。
         */
        job.setPartitionerClass (AutoPartitioner.class);
        job.setNumReduceTasks (3);
        job.setReducerClass (NumSortReducer.class);
        job.setOutputKeyClass (IntWritable.class);
        job.setOutputValueClass (IntWritable.class);
        FileInputFormat.setInputPaths (job, new Path ("hdfs://hadoop201:9000/numsort/"));
        FileOutputFormat.setOutputPath (job, new Path ("hdfs://hadoop201:9000/result/numsort"));
        job.waitForCompletion (true);
    }
}