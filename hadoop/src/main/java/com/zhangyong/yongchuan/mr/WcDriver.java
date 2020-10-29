package com.zhangyong.yongchuan.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Author: 张勇
 * @Blog: https://blog.csdn.net/zy13765287861
 * @Version: 1.0
 * @Date: 2020-10-27 14:17
 * @PS:
 */
public class WcDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //获取连接
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //获取Driver
        job.setJarByClass(WcDriver.class);

        //获取WcMapper和WcReducer
        job.setMapperClass(WcMapper.class);
        job.setReducerClass(WcReducer.class);

        //获取map结果类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //获取Reducer结果类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, new Path("D:\\IntelliJ\\IdeaProjects\\big-data-project\\hadoop\\src\\main\\resources\\input\\wc.txt"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\IntelliJ\\IdeaProjects\\big-data-project\\hadoop\\src\\main\\resources\\output\\yongchuan\\wc"));

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);

    }
}
