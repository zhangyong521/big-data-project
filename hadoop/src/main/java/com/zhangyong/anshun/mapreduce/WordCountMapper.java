package com.zhangyong.anshun.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author 17616
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    private Text w = new Text();
    private LongWritable one = new LongWritable(1);

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 首先获取一行数据
        String line = value.toString();
        // 将行内的单词进行切分，使用一个数组进行保存，切分数据时根据源数据得知可以使用空格的方式切分。
        String[] words = line.split(" ");
        for (String word : words) {
            w.set(word);
            context.write(w, one);
        }
    }

}
