package com.zhangyong.mapreduce.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author 17616
 * 官方案例，计算统计
 */
public class WordCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 获取当前的默认配置
        Configuration conf = new Configuration ();

        // 获取代表当前mr作业的job对象
        Job job = Job.getInstance (conf);
        // 指定一下当前程序的入口类
        job.setJarByClass (WordCountDriver.class);

        //指定当前Mapper、Reducer任务的类
        job.setMapperClass (WordCountMapper.class);
        job.setReducerClass (WordCountReducer.class);

        //设置Mapper的结果类型
        job.setMapOutputKeyClass (Text.class);
        job.setMapOutputValueClass (LongWritable.class);

        // 设置Reducer的结果类型
        job.setOutputKeyClass (Text.class);
        job.setOutputValueClass (LongWritable.class);

        //设置待分析的文件夹路径（linux的路径地址）
        FileInputFormat.setInputPaths (job, new Path ("hdfs://hadoop201:9000/mapreduce"));
        FileOutputFormat.setOutputPath (job, new Path ("hdfs://hadoop201:9000/result/mapreduce"));

        if (!job.waitForCompletion (true)) {
            return;
        }

    }
}
