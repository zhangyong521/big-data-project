package com.zhangyong.mapreduce.distinct;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author zhangyong
 * @Date 2020/4/7 19:53
 * @Version 1.0
 */
public class DisMapper extends Mapper<LongWritable,Text,Text,NullWritable> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        /**
         * 其中value只是一个变量，此处被当做key进行输出
         */
        context.write (value,NullWritable.get ());
    }
}
