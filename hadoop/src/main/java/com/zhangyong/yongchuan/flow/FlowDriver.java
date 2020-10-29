package com.zhangyong.yongchuan.flow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Author: 张勇
 * @Blog: https://blog.csdn.net/zy13765287861
 * @Version: 1.0
 * @Date: 2020-10-27 14:45
 * @PS:
 */
public class FlowDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(FlowDriver.class);
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        FileInputFormat.setInputPaths(job, new Path("D:\\IntelliJ\\IdeaProjects\\big-data-project\\hadoop\\src\\main\\resources\\input\\phone_data.txt"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\IntelliJ\\IdeaProjects\\big-data-project\\hadoop\\src\\main\\resources\\output\\yongchuan\\flow"));

        boolean b = job.waitForCompletion(true);

        System.exit(b ? 0 : 1);

    }
}
