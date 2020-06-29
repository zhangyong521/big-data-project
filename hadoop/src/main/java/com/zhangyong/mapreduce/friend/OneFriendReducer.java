package com.zhangyong.mapreduce.friend;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

/**
 * @Author 张勇
 * @Site www.gz708090.com
 * @Version 1.0
 * @Date 2020-04-17 12:28
 */

public class OneFriendReducer extends Reducer<Text, Text, Text, IntWritable> {
    /**
     * 输入key和value要和mapper的输出保持一致。
     * Text和IntWritable：
     * 如果是好友-1，如果不是好友就用-2。
     */
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        ArrayList<String> friendList = new ArrayList<>();
        //处理好友关系
        for (Text value : values) {
            friendList.add(value.toString());
            if (key.toString().compareTo(value.toString()) < 0) {
                context.write(new Text(key + "-" + value), new IntWritable(1));
            } else {
                context.write(new Text(value + "-" + key), new IntWritable(1));
            }
        }
        // 处理可能相识的好友。
        for (int i = 0; i < friendList.size(); i++) {
            for (int j = 0; j < friendList.size(); j++) {
                String friend1 = friendList.get(i);
                String friend2 = friendList.get(j);
                if (friend1.compareTo(friend2) < 0) {
                    context.write(new Text(friend1 + "-" + friend2), new IntWritable(2));
                }
            }
        }
    }
}
