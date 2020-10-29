package com.zhangyong.anshun.friend;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author 张勇
 * @Site www.gz708090.com
 * @Version 1.0
 * @Date 2020-04-17 12:08
 */
public class OneFriendMapper extends Mapper<LongWritable, Text, Text, Text> {
    /**
     * 输入的key和value是根据文件内容来确定。
     * 输出的key和value是因为在业务逻辑中设定的输出是name-friend好友关系。
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 获取每行的数据
        String line = value.toString();
        // 获取姓名
        String name = line.split(" ")[0];
        // 获取好友
        String friend = line.split(" ")[1];
        context.write(new Text(name), new Text(friend));
    }
}
