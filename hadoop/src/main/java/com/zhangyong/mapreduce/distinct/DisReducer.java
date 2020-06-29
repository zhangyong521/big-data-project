package com.zhangyong.mapreduce.distinct;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author zhangyong
 * @Date 2020/4/7 21:21
 * @Version 1.0
 */
public class DisReducer extends Reducer<Text, NullWritable, Text, NullWritable> {

    @Override
    public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        context.write (key, NullWritable.get ());
    }

}
