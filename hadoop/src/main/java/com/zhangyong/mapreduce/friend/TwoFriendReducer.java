package com.zhangyong.mapreduce.friend;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author 张勇
 * @Site www.gz708090.com
 * @Version 1.0
 * @Date 2020-04-17 12:34
 */
public class TwoFriendReducer extends Reducer<Text, IntWritable, Text, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        Boolean flag = true;
        /**
         * 设定好友关系为true的时候进行输出
         * 因为题目要求是输出可能相识的好友。所以为true的代码应该是2
         * 也就是好友关系为1的时候设置变量为false
         */
        for (IntWritable value : values) {
            if (value.get() == 1) {
                flag = false;
            }
        }
        if (flag) {
            context.write(key, NullWritable.get());
        }
    }
}