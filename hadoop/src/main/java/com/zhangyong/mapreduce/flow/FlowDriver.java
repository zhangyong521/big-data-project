package com.zhangyong.mapreduce.flow;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Author zhangyong
 * @Date 2020/4/10 8:28
 * @Version 1.0
 * 流量统计
 */
public class FlowDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration ();
        Job job = Job.getInstance (conf);
        // 加载主类
        job.setJarByClass (FlowDriver.class);

        //加载mapper、reduce类
        job.setMapperClass (FlowMapper.class);
        job.setReducerClass (FlowReducer.class);

        //设置map的的key、value
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        //设置输出的的key、value
        job.setOutputKeyClass(FlowBean.class);
        job.setOutputValueClass (NullWritable.class);

        //设置路径（传输、结果）

        FileInputFormat.setInputPaths (job, new Path ("hdfs://hadoop201:9000/flow"));
        FileOutputFormat.setOutputPath (job, new Path ("hdfs://hadoop201:9000/result/flow"));
        job.waitForCompletion (true);

    }
}
