package com.zhangyong.anshun.partition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @Author zhangyong
 * @Date 2020/4/10 10:42
 * @Version 1.0
 */
public class AddPartitioner extends Partitioner<Text, PartFlowBean> {
    @Override
    public int getPartition(Text text, PartFlowBean flowBean, int
            numPartitioner) {
        String addr = flowBean.getAddr();
        if (addr.equals("bj")) {
            return 0;
        } else if (addr.equals("sh")) {
            return 1;
        } else {
            return 2;
        }
    }
}
