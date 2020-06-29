package com.zhangyong.mapreduce.combine;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Author zhangyong
 * @Date 2020/4/15 7:42
 * @Version 1.0
 */
public class WcDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration ();
        Job job = Job.getInstance (conf);

        job.setJarByClass (WcDriver.class);

        job.setMapperClass (WcMapper.class);
        job.setReducerClass (WcReducer.class);

        job.setMapOutputKeyClass (Text.class);
        job.setMapOutputValueClass (IntWritable.class);

        /**
         * 设置combine组件类，如果不设定，默认是不执行combine过程的。
         * 设置combine的目的是为了让合并工作提前发生一次，在MapTask阶段时合并一次，使Reduce阶段的工作负载。
         * 需要注意的是combine仅仅是做合并的工作，减少工作负载，并不能影响最终的文件结果。
         */
        job.setCombinerClass(WcCombine.class);

        job.setOutputKeyClass (Text.class);
        job.setOutputValueClass (IntWritable.class);

        FileInputFormat.setInputPaths (job, new Path ("hdfs://hadoop201:9000/combine"));
        FileOutputFormat.setOutputPath (job, new Path ("hdfs://hadoop201:9000/result/combine"));

        job.waitForCompletion (true);
    }
}
