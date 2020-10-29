package com.zhangyong.anshun.distinct;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Author zhangyong
 * @Date 2020/4/7 21:32
 * @Version 1.0
 * 数据去重
 */
public class DisDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration ();
        Job job = Job.getInstance (conf);

        //设置Drive类
        job.setJarByClass (DisReducer.class);

        //设置Mapper、Reduce类
        job.setMapperClass (DisMapper.class);
        job.setReducerClass (DisReducer.class);

        //Mapper的输出
        job.setMapOutputKeyClass (Text.class);
        job.setMapOutputValueClass (NullWritable.class);

        //地址
        FileInputFormat.setInputPaths (job,new Path ("hdfs://hadoop201:9000/distinct"));
        FileOutputFormat.setOutputPath (job,new Path ("hdfs://hadoop201:9000/result/distinct"));

        job.waitForCompletion (true);
    }
}
