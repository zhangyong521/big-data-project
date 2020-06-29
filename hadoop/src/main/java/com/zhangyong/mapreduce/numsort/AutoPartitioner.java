package com.zhangyong.mapreduce.numsort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @Author zhangyong
 * @Date 2020/4/14 9:39
 * @Version 1.0
 * 全排序
 * 将上述文件内容按照数字位数分别写入三个文件，如下
 * 0-99的写入到文件1
 * 100-999写入到文件2
 * 1000-其他数据写入到文件3
 */
public class AutoPartitioner extends Partitioner<IntWritable, IntWritable> {
    @Override
    public int getPartition(IntWritable key, IntWritable value, int numPartitions) {
        String num = String.valueOf (key.get ());
        if (num.matches ("[0-9][0-9]") || num.matches ("[0-9]")) {
            return 0;
        } else if (num.matches ("[0-9][0-9][0-9]")) {
            return 1;
        } else {
            return 2;
        }
    }
}
