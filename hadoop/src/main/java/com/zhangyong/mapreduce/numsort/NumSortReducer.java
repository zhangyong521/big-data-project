package com.zhangyong.mapreduce.numsort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
/**
 * @Author zhangyong
 * @Date 2020/4/14 9:39
 * @Version 1.0
 */
public class NumSortReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
    @Override
    protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int result = 0;
        for (IntWritable count : values) {
            result = result + count.get ();
        }
        context.write (key, new IntWritable (result));
    }
}
