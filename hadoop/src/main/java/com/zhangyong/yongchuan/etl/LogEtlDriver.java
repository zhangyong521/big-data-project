package com.zhangyong.yongchuan.etl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Author: 张勇
 * @Blog: https://blog.csdn.net/zy13765287861
 * @Version: 1.0
 * @Date: 2020-10-28 14:39
 * @PS:
 */
public class LogEtlDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(LogEtlDriver.class);
        job.setMapperClass(LogEtlMapper.class);

        //設置reducer的數為0
        job.setNumReduceTasks(0);

        job.setOutputValueClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path("D:\\IntelliJ\\IdeaProjects\\big-data-project\\hadoop\\src\\main\\resources\\input\\web.log"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\IntelliJ\\IdeaProjects\\big-data-project\\hadoop\\src\\main\\resources\\output\\yongchuan\\etl"));

        boolean b = job.waitForCompletion(true);

        System.exit(b ? 0 : 1);

    }
}
