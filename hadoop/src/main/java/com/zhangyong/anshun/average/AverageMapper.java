package com.zhangyong.anshun.average;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author zhangyong
 * @Date 2020/4/3 23:43
 * @Version 1.0
 */
public class AverageMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Text text = new Text();
    private IntWritable writable = new IntWritable();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取每行的数据内容
        String line = value.toString();
        //按照空格去切会获取到多个数据，所以用数组的方式存储
        String[] data = line.split(" ");
        String name = data[0];
        //Integer做一个数据类型的强制转换。
        int score = Integer.parseInt(data[1]);
        //设置值
        text.set(name);
        writable.set(score);
        //输出数据
        context.write(text, writable);
    }

}
