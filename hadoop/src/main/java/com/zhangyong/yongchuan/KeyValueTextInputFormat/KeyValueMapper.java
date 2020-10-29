package com.zhangyong.yongchuan.KeyValueTextInputFormat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author: 张勇
 * @Blog: https://blog.csdn.net/zy13765287861
 * @Version: 1.0
 * @Date: 2020-10-27 16:22
 * @PS:
 */
public class KeyValueMapper extends Mapper<Text, Text, Text, LongWritable> {

    LongWritable t = new LongWritable(1);

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        //map输出格式（banzhang ni hao）
        context.write(key, t);
    }

}
