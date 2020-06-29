package com.zhangyong.mapreduce.count;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @Author zhangyong
 * @Date 2020/4/14 8:54
 * @Version 1.0
 * job链操作数据
 */
public class CountDriver {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration ();
        Job job = Job.getInstance (conf);
        job.setJarByClass (CountDriver.class);
        job.setMapperClass (OneCountMapper.class);
        job.setReducerClass (OneCountReducer.class);
        job.setMapOutputKeyClass (Text.class);
        job.setMapOutputValueClass (IntWritable.class);
        job.setOutputKeyClass (Text.class);
        job.setOutputValueClass (IntWritable.class);
        FileInputFormat.setInputPaths (job, new Path ("hdfs://hadoop201:9000/count"));
        FileOutputFormat.setOutputPath (job, new Path ("hdfs://hadoop201:9000/result/count"));
        if (job.waitForCompletion (true)) {
            // 设置第二个Job任务
            Job job2 = Job.getInstance (conf);
            // 设置第二个Job任务的Mapper
            job2.setMapperClass (TwoCountMapper.class);
            job2.setMapOutputKeyClass (CountBean.class);
            job2.setMapOutputValueClass (NullWritable.class);
            /**
             * 设置第二个Job任务是输入输出路径。
             * 此处的输入路径是第一个job任务的输出路径
             * 注意设置路径时，里面传入的job应该是当前的job任务，如下所示，应该是job2。
             * 如果写成前面的job任务名称，在运行时则会爆出错误，提示路径不存在。
             */
            FileInputFormat.setInputPaths (job2, new Path ("hdfs://hadoop201:9000/result/count"));
            FileOutputFormat.setOutputPath (job2, new Path ("hdfs://hadoop201:9000/result/count2"));
            // 此处提交任务时，注意用的是job2。
            job2.waitForCompletion (true);
        }
    }

}
