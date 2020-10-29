package com.zhangyong.yongchuan.flow;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author: 张勇
 * @Blog: https://blog.csdn.net/zy13765287861
 * @Version: 1.0
 * @Date: 2020-10-27 14:45
 * @PS:
 */
public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
    Text t = new Text();
    FlowBean flowBean = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取一行数据
        String line = value.toString();
        //按照\t切分
        String[] split = line.split("\t");
        //获取到手机号
        String phone = split[1];
        //获取到上行数据
        long up = Long.parseLong(split[split.length - 3]);
        //获取到下行数据
        long down = Long.parseLong(split[split.length - 2]);
        //把电话设置到进去
        t.set(phone);
        flowBean.set(up, down);

        context.write(t, flowBean);

    }

}
