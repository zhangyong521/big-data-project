package com.zhangyong.yongchuan.reducejion;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @Author: 张勇
 * @Blog: https://blog.csdn.net/zy13765287861
 * @Version: 1.0
 * @Date: 2020-10-28 9:05
 * @PS:
 */
public class TableMapper extends Mapper<LongWritable, Text, Text, TableBean> {
    String name;
    TableBean bean = new TableBean();
    Text k = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        //1、获取输入文件的切片
        FileSplit split = (FileSplit) context.getInputSplit();

        //2、获取输入文件名称
        name = split.getPath().getName();

    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1、获取并切分数据
        String[] fields = value.toString().split("\t");

        //2、不同文件分别处理
        if (name.startsWith("order")) {
            bean.setOrder_id(fields[0]);
            bean.setP_id(fields[1]);
            bean.setAmount(Integer.parseInt(fields[2]));
            bean.setPname("");
            bean.setFlag("order");

            k.set(fields[1]);
        } else {
            bean.setP_id(fields[0]);
            bean.setPname(fields[1]);
            bean.setFlag("pd");
            bean.setAmount(0);
            bean.setOrder_id("");

            k.set(fields[0]);
        }
        //3、写出
        context.write(k, bean);
    }


}
