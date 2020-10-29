package com.zhangyong.anshun.average;

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
 * @Date 2020/4/3 23:41
 * @Version 1.0
 * 计算平均值
 */
public class AverageDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //驱动类，入口类
        job.setJarByClass(AverageDriver.class);

        //设置Mapper和Reducer的类
        job.setMapperClass(AverageMapper.class);
        job.setReducerClass(AverageReducer.class);

        //设置Mapper的结果类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //设置Reduce的结果类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //设置待分析的文件夹路径（linux的路径地址）
        FileInputFormat.setInputPaths(job, new Path("hdfs://hadoop201:9000/average"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://hadoop201:9000/result/average"));

        // 7 提交
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);

    }
}
