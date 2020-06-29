package com.zhangyong.mapreduce.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author 17616
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 首先获取一行数据
        String line = value.toString ();
        // 将行内的单词进行切分，使用一个数组进行保存，切分数据时根据源数据得知可以使用空格的方式切分。
        String[] arr = line.split (" ");
        for (String str : arr) {
            context.write (new Text (str), new LongWritable (1));
        }
    }
}
