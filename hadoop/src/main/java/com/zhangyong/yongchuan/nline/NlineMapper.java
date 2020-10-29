package com.zhangyong.yongchuan.nline;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author: 张勇
 * @Blog: https://blog.csdn.net/zy13765287861
 * @Version: 1.0
 * @Date: 2020-10-27 16:53
 * @PS:
 */
public class NlineMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    Text text = new Text();
    LongWritable longWritable = new LongWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] split = value.toString().split(" ");
        for (int i = 0; i < split.length; i++) {
            String s = split[i];
            text.set(s);
        }

        context.write(text, longWritable);
    }

}
