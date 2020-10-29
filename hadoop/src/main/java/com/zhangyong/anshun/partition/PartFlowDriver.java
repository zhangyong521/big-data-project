package com.zhangyong.anshun.partition;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @Author zhangyong
 * @Date 2020/4/11 11:17
 * @Version 1.0
 * 分区案例
 */
public class PartFlowDriver {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration ();

        Job job = Job.getInstance (conf);

        job.setJarByClass (PartFlowDriver.class);

        job.setMapperClass (PartFlowMapper.class);
        job.setReducerClass (PartFlowReducer.class);
        /**
         * 下面的两个类如果不写的话，那么就不会生效。
         */
        // 设置分区类
        job.setPartitionerClass (AddPartitioner.class);
        // 设置分区数量
        job.setNumReduceTasks (3);

        job.setMapOutputKeyClass (Text.class);
        job.setMapOutputValueClass (PartFlowBean.class);

        job.setOutputKeyClass (PartFlowBean.class);
        job.setOutputValueClass (NullWritable.class);

        FileInputFormat.setInputPaths (job, new Path ("hdfs://hadoop201:9000/partition"));
        FileOutputFormat.setOutputPath (job, new Path ("hdfs://hadoop201:9000/result/partition"));

        job.waitForCompletion (true);
    }
}
