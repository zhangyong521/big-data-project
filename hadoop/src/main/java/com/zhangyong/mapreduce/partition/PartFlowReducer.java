package com.zhangyong.mapreduce.partition;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author zhangyong
 * @Date 2020/4/10 10:23
 * @Version 1.0
 */
public class PartFlowReducer extends Reducer<Text, PartFlowBean, PartFlowBean,
        NullWritable> {
    @Override
    public void reduce(Text key, Iterable<PartFlowBean> values, Context
            context) throws IOException, InterruptedException {
        PartFlowBean result = new PartFlowBean ();
        for (PartFlowBean value : values) {
            result.setPhone (value.getPhone ());
            result.setPhone (value.getPhone ());
            result.setName (value.getName ());
            result.setAddr (value.getAddr ());
            result.setFlow (result.getFlow () + value.getFlow ());
        }
        context.write (result, NullWritable.get ());
    }
}

