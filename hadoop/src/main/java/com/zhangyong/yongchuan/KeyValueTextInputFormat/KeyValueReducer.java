package com.zhangyong.yongchuan.KeyValueTextInputFormat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author: 张勇
 * @Blog: https://blog.csdn.net/zy13765287861
 * @Version: 1.0
 * @Date: 2020-10-27 16:23
 * @PS:
 */
public class KeyValueReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    LongWritable writable = new LongWritable();

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long sum = 0L;

        for (LongWritable value : values) {
            sum += value.get();
        }
        writable.set(sum);
        context.write(key, writable);

    }
}
