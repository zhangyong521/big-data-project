package com.zhangyong.anshun.count;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author zhangyong
 * @Date 2020/4/14 15:27
 * @Version 1.0
 */
public class TwoCountMapper extends Mapper<LongWritable, Text, CountBean, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString ();
        /**
         * 因为第二个Mapper任务要读取的数据内容是第一个MR任务的结果文件，通常MR任务的结果文件是以TAB的方式来展示数据的。
         * 所以当第二个Mapper任务要执行切分时，所使用的分隔符应该是\t——制表符。
         */
        String name = line.split ("\t")[0];
        int count = Integer.parseInt (line.split ("\t")[1]);

        CountBean bean = new CountBean ();
        bean.setName (name);
        bean.setCount (count);

        context.write (bean, NullWritable.get ());
    }
}

