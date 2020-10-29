package com.zhangyong.anshun.friend;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author 张勇
 * @Site www.gz708090.com
 * @Version 1.0
 * @Date 2020-04-17 12:32
 */
public class TwoFriendMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 获取一行数据
        String line = value.toString();
        // 获取朋友关系的信息
        String friendInfo = line.split("\t")[0];
        // 获取朋友关系的深度
        int deep = Integer.parseInt(line.split("\t")[1]);
        context.write(new Text(friendInfo), new IntWritable(deep));
    }
}
