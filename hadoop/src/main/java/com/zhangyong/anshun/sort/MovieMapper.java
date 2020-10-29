package com.zhangyong.anshun.sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author zhangyong
 * @Date 2020/4/13 8:52
 * @Version 1.0
 */
public class MovieMapper extends Mapper<LongWritable, Text, MovieBean, NullWritable> {

    /**
     * 封装对象
     */
    MovieBean movieBean = new MovieBean ();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取一行
        String line = value.toString ();
        //截取数据
        String[] split = line.split (" ");

        movieBean.setName (split[0]);
        movieBean.setHot (Integer.parseInt (split[1]));

        //输出
        context.write (movieBean, NullWritable.get ());

    }

}
