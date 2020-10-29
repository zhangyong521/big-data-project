package com.zhangyong.anshun.friend;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Author 张勇
 * @Site www.gz708090.com
 * @Version 1.0
 * @Date 2020-04-17 12:36
 */
public class FriendDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        //设置第一轮MapReduce的相应处理类与输入输出
        Job job1 = Job.getInstance(conf);

        job1.setJarByClass(FriendDriver.class);

        job1.setMapperClass(OneFriendMapper.class);
        job1.setReducerClass(OneFriendReducer.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        //设置路径（传输、结果）
        FileInputFormat.setInputPaths(job1, new Path("hdfs://hadoop201:9000/friend"));
        FileOutputFormat.setOutputPath(job1, new Path("hdfs://hadoop201:9000/result/friend"));

        //如果第一轮MapReduce完成再做这里的代码
        if (job1.waitForCompletion(true)) {
            Job job2 = Job.getInstance(conf);
            // 设置第二个Job任务的Mapper
            job2.setMapperClass(TwoFriendMapper.class);
            job2.setMapOutputKeyClass(Text.class);
            job2.setMapOutputValueClass(IntWritable.class);

            // 设置第二个Job任务的Reducer
            job2.setReducerClass(TwoFriendReducer.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(NullWritable.class);

            /**
             * 设置第二个Job任务是输入输出路径。
             * 此处的输入路径是第一个job任务的输出路径
             * 注意设置路径时，里面传入的job应该是当前的job任务，如下所示，应该是job2。
             * 如果写成前面的job任务名称，在运行时则会爆出错误，提示路径不存在。
             */
            FileInputFormat.setInputPaths(job2, new Path("hdfs://hadoop201:9000/result/friend"));
            FileOutputFormat.setOutputPath(job2, new Path("hdfs://hadoop201:9000/result/friend2"));
            // 此处提交任务时，注意用的是job2。
            job2.waitForCompletion(true);
        }
    }
}
