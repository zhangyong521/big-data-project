package com.zhangyong.yongchuan.combine;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author: 张勇
 * @Blog: https://blog.csdn.net/zy13765287861
 * @Version: 1.0
 * @Date: 2020-10-27 14:17
 * @PS:
 */
public class WcCombineMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    Text text = new Text();
    IntWritable num = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取一行数据，并且转换
        String line = value.toString();
        //按照空格切割
        String[] split = line.split(" ");

        //遍历切出来的单词个数
        for (String word : split) {
            text.set(word);
            context.write(text, num);
        }
    }
}
