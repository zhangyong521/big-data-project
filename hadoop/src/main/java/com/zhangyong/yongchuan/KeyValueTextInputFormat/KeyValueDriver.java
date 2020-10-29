package com.zhangyong.yongchuan.KeyValueTextInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Author: 张勇
 * @Blog: https://blog.csdn.net/zy13765287861
 * @Version: 1.0
 * @Date: 2020-10-27 16:23
 * @PS:
 */
public class KeyValueDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //获取连接
        Configuration conf = new Configuration();
        //设置切割符
        conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR," ");

        Job job = Job.getInstance(conf);

        //获取Driver
        job.setJarByClass(KeyValueDriver.class);

        //获取WcMapper和WcReducer
        job.setMapperClass(KeyValueMapper.class);
        job.setReducerClass(KeyValueReducer.class);

        //获取map结果类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        //获取Reducer结果类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //设置输入格式
        job.setInputFormatClass(KeyValueTextInputFormat.class);

        FileInputFormat.setInputPaths(job, new Path("D:\\IntelliJ\\IdeaProjects\\big-data-project\\hadoop\\src\\main\\resources\\input\\keyValue.txt"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\IntelliJ\\IdeaProjects\\big-data-project\\hadoop\\src\\main\\resources\\output\\yongchuan\\keyValue"));

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);

    }
}
