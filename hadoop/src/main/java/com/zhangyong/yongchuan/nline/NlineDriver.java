package com.zhangyong.yongchuan.nline;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Author: 张勇
 * @Blog: https://blog.csdn.net/zy13765287861
 * @Version: 1.0
 * @Date: 2020-10-27 16:53
 * @PS:
 */
public class NlineDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 1 获取job对象
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        // 7设置每个切片InputSplit中划分三条记录
        NLineInputFormat.setNumLinesPerSplit(job, 3);
        // 8使用NLineInputFormat处理记录数
        job.setInputFormatClass(NLineInputFormat.class);

        // 2设置jar包位置，关联mapper和reducer
        job.setJarByClass(NlineDriver.class);
        job.setMapperClass(NlineMapper.class);
        job.setReducerClass(NlineReducer.class);

        // 3设置map输出kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        // 4设置最终输出kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        // 5设置输入输出数据路径
        FileInputFormat.setInputPaths(job, new Path("D:\\IntelliJ\\IdeaProjects\\big-data-project\\hadoop\\src\\main\\resources\\input\\nline.txt"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\IntelliJ\\IdeaProjects\\big-data-project\\hadoop\\src\\main\\resources\\output\\yongchuan\\nline"));

        // 6提交job
        boolean b = job.waitForCompletion(true);

        System.exit(b ? 0 : 1);

    }
}
