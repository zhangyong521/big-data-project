package com.zhangyong.yongchuan.etl;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author: 张勇
 * @Blog: https://blog.csdn.net/zy13765287861
 * @Version: 1.0
 * @Date: 2020-10-28 14:24
 * @PS:
 */
public class LogEtlMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    Text text = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取一行日志
        String line = value.toString();
        //对一行数据做解析
        boolean flag = parseLog(line, context);

        //判断长度是否大于11
        if (!flag) {
            return;
        }

        text.set(line);

        context.write(text, NullWritable.get());
    }

    /**
     * 解析日志
     *
     * @param line    获取的每行数据
     * @param context
     * @return
     */
    private Boolean parseLog(String line, Context context) {
        //按照空格切分
        String[] split = line.split(" ");

        if (split.length > 11) {
            context.getCounter("map", "true").increment(1);
            return true;
        } else {
            context.getCounter("map", "false").increment(1);
            return false;
        }

    }
}
