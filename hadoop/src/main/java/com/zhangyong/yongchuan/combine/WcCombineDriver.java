package com.zhangyong.yongchuan.combine;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Author: 张勇
 * @Blog: https://blog.csdn.net/zy13765287861
 * @Version: 1.0
 * @Date: 2020-10-27 14:17
 * @PS: 测试combine
 */
public class WcCombineDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //获取连接
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //获取Driver
        job.setJarByClass(WcCombineDriver.class);

        //获取WcMapper和WcReducer
        job.setMapperClass(WcCombineMapper.class);
        job.setReducerClass(WcCombineReducer.class);

        //获取map结果类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //获取Reducer结果类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        /**
         * 如果包里面有3个小文件，总大小小于4M，如果不设置一下两行代码
         * 默认切块为3块，设置以下两个代码，就可以设置为1块
         */
        job.setInputFormatClass(CombineTextInputFormat.class);
        CombineFileInputFormat.setMaxInputSplitSize(job, 4194304);

        FileInputFormat.setInputPaths(job, new Path("D:\\IntelliJ\\IdeaProjects\\big-data-project\\hadoop\\src\\main\\resources\\input\\score"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\IntelliJ\\IdeaProjects\\big-data-project\\hadoop\\src\\main\\resources\\output\\yongchuan\\combine"));

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);

    }
}
