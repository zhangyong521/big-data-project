package com.zhangyong.mapreduce.average;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author zhangyong
 * @Date 2020/4/3 23:43
 * @Version 1.0
 */
public class AverageReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    public void reduce(Text name, Iterable<IntWritable> scores, Context context) throws IOException, InterruptedException {
        int i = 0;
        int score = 0;
        for (IntWritable data : scores) {
            score = score + data.get ();
            i++;
        }
        int average = score / i;
        context.write (name, new IntWritable (average));
    }
}
