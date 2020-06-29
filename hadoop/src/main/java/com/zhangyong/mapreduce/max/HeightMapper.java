package com.zhangyong.mapreduce.max;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author 17616
 */
public class HeightMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取一段数据
        String line = value.toString ();

        //获取年份
        String year = line.substring (8, 12);

        //获取温度(强制转换一下)
        int t = Integer.parseInt (line.substring (18, 22));

        context.write (new Text (year),new LongWritable (t));

    }
}