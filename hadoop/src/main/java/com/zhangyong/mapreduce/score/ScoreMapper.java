package com.zhangyong.mapreduce.score;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
/**
 * @Author zhangyong
 * @Date 2020/4/10 10:03
 * @Version 1.0
 */
public class ScoreMapper extends Mapper<LongWritable, Text, Text, ScoreBean> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // 获取一行数据
        String line = value.toString();
        String[] data = line.split(" ");

        ScoreBean ss = new ScoreBean();
        ss.setName(data[1]);

        /**
         * 注意，此处导包的时候不要导错，应该导入的是org.apache.hadoop.mapreduce.lib.input.FileSplit;
         * 通过获取当前map阶段的MapTask处理的切片信息来获取文件名。
         */
        FileSplit split = (FileSplit) context.getInputSplit();

        if (split.getPath().getName().equals("chinese.txt")) {
            ss.setChinese(Integer.parseInt(data[2]));
        } else if (split.getPath().getName().equals("math.txt")) {
            ss.setMath(Integer.parseInt(data[2]));
        } else if (split.getPath().getName().equals("english.txt")) {
            ss.setEnglish(Integer.parseInt(data[2]));
        }

        context.write(new Text(ss.getName()), ss);
    }
}
