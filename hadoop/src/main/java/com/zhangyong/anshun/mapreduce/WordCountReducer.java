package com.zhangyong.anshun.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author 17616
 */
public class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    private LongWritable writable = new LongWritable();

    @Override
    public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        // 定义变量记录单词出现的次数
        int sum = 0;
        for (LongWritable val : values) {
            // 记录总次数
            sum += val.get();
        }
        writable.set(sum);
        // 输出数据，key就是单词，value就是在map阶段这个单词出现的总次数
        context.write(key, writable);
    }
}
