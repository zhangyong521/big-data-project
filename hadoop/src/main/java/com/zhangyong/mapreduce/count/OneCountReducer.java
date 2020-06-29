package com.zhangyong.mapreduce.count;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author zhangyong
 * @Date 2020/4/14 8:54
 * @Version 1.0
 */
public class OneCountReducer extends Reducer<Text , IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //统计利润
        int sum = 0;
        for (IntWritable value : values) {
            sum+=value.get ();
        }
        context.write (key,new IntWritable (sum));
    }
}
