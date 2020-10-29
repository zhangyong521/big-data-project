package com.zhangyong.anshun.flow;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


/**
 * @Author zhangyong
 * @Date 2020/4/10 8:10
 * @Version 1.0
 */
public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取行
        String line = value.toString ();
        /**
         * [13901000123,zk,bj,343]
         *  phone = 13901000123;
         *  name = zk;
         *  addr = bj;
         *  flow = 343;
         */
        String[] info = line.split (" ");
        FlowBean flowBean = new FlowBean ();

        flowBean.setPhone (info[0]);
        flowBean.setName (info[1]);
        flowBean.setAddr (info[2]);
        flowBean.setFlow (Integer.parseInt (info[3]));

        context.write (new Text (flowBean.getName ()), flowBean);

    }
}
