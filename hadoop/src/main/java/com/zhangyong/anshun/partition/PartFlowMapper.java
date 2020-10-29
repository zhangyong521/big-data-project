package com.zhangyong.anshun.partition;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author zhangyong
 * @Date 2020/4/10 10:51
 * @Version 1.0
 */
public class PartFlowMapper extends Mapper<LongWritable, Text, Text, PartFlowBean> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws
            IOException, InterruptedException {
        String line = value.toString ();
         /**
         [13901000123,zk,bj,343]
         phone = 13901000123;
         name = zk;
         addr = bj;
         flow = 343;
         */
        String[] info = line.split (" ");
        PartFlowBean flowBean = new PartFlowBean ();
        flowBean.setPhone (info[0]);
        flowBean.setName (info[1]);
        flowBean.setAddr (info[2]);
        flowBean.setFlow (Integer.parseInt (info[3]));
        context.write (new Text (flowBean.getName ()), flowBean);
    }
}
