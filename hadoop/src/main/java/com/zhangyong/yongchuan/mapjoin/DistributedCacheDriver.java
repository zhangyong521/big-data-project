package com.zhangyong.yongchuan.mapjoin;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;


/**
 * @Author: 张勇
 * @Blog: https://blog.csdn.net/zy13765287861
 * @Version: 1.0
 * @Date: 2020-10-28 10:16
 * @PS:
 */
public class DistributedCacheDriver {

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(DistributedCacheDriver.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setMapperClass(DistributedCacheMapper.class);

        job.addCacheFile(new URI("file:///D:/IO/input/mapjoin1/pd.txt"));
        job.setNumReduceTasks(0);

        FileInputFormat.setInputPaths(job, new Path("D:\\IO\\input\\mapjoin"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\IO\\output\\mapjoin"));

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
