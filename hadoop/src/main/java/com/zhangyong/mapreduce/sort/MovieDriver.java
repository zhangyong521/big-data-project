package com.zhangyong.mapreduce.sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Author zhangyong
 * @Date 2020/4/13 9:19
 * @Version 1.0
 */
public class MovieDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration ();
        Job job = Job.getInstance (conf);

        job.setJarByClass (MovieDriver.class);

        job.setMapperClass (MovieMapper.class);

        //加载map输出类型和value的输出类型
        job.setMapOutputKeyClass (MovieBean.class);
        job.setMapOutputValueClass (NullWritable.class);

        FileInputFormat.setInputPaths (job, new Path ("hdfs://hadoop201:9000/sort"));
        FileOutputFormat.setOutputPath (job, new Path ("hdfs://hadoop201:9000/result/sort"));

        job.waitForCompletion (true);
    }
}
