package com.zhangyong.anshun.count;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author zhangyong
 * @Date 2020/4/14 8:54
 * @Version 1.0
 */
public class OneCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString ();
        /**
         * 通过 | 进行切分时会得到一个数组，数组中的0号下标为序号，1号下标中有name和profit的数据。
         * 再通过切分1号下标中的数据时可以获取到name和profit的数据。
         */
        String name = line.split ("\\|")[1].split (" ")[0];
        int count = Integer.parseInt (line.split ("\\|")[1].split (" ")[1]);

        context.write (new Text (name),new IntWritable (count));

    }
}
