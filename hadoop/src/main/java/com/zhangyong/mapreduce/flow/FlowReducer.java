package com.zhangyong.mapreduce.flow;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author zhangyong
 * @Date 2020/4/10 8:23
 * @Version 1.0
 */
public class FlowReducer extends Reducer<Text, FlowBean, FlowBean, NullWritable> {
    @Override
    public void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        FlowBean result = new FlowBean ();

        for (FlowBean value : values) {
            result.setPhone (value.getPhone ());
            result.setName (value.getName ());
            result.setAddr (value.getAddr ());
            result.setFlow (result.getFlow () + value.getFlow ());
        }
        context.write (result, NullWritable.get ());
    }
}
