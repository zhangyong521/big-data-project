package com.zhangyong.yongchuan.mapjoin;


import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @Author: 张勇
 * @Blog: https://blog.csdn.net/zy13765287861
 * @Version: 1.0
 * @Date: 2020-10-28 10:10
 * @PS:
 */
public class DistributedCacheMapper extends Mapper<LongWritable, Text, Text, NullWritable> {


    HashMap<String, String> pdMap = new HashMap<>();
    Text text = new Text();


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // 获取到缓存文件，是一个 URI 的数组
        URI[] cacheFiles = context.getCacheFiles();
        //由于只有一个缓存文件 pd.txt，我们这里只需要拿到第一个元素即可
        URI pdUri = cacheFiles[0];
        //获取到缓存文件的路径
        String path = pdUri.getPath();

        //获取到bufferedReader对象（缓冲字符流）
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(path)));

        //对每行数据做迭代，进行切割，切割后的数据放入到map中
        String line;
        //while (!(line = bufferedReader.readLine()).isEmpty()) {
        while (StringUtils.isNotEmpty(line = bufferedReader.readLine())) {
            String[] split = line.split("\t");
            pdMap.put(split[0], split[1]);
        }
        //关闭资源
        IOUtils.closeStream(bufferedReader);

    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //读取order.txt 的每行数据，并进行切割
        String[] split = value.toString().split("\t");
        //获取 pid 公共字段
        String pid = split[1];
        //根据pid从map中获取到pname
        String pname = pdMap.get(pid);

        //拼接最后的结果
        text.set(split[0] + "\t" + pname + "\t" + split[2]);
        context.write(text, NullWritable.get());

    }
}
