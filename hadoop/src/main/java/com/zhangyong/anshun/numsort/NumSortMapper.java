package com.zhangyong.anshun.numsort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author zhangyong
 * @Date 2020/4/14 9:44
 * @Version 1.0
 */
public class NumSortMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString ();
        String[] data = line.split (" ");
        for (String num : data) {
            context.write (new IntWritable (Integer.parseInt (num)), new IntWritable (1));
        }
    }
}

