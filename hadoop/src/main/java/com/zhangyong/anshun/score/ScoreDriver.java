package com.zhangyong.anshun.score;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @Author zhangyong
 * @Date 2020/4/10 10:10
 * @Version 1.0
 * 统计成绩
 */
public class ScoreDriver {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //加载Drive类
        job.setJarByClass(ScoreDriver.class);

        //加载Mapper、Reducer类
        job.setMapperClass(ScoreMapper.class);
        job.setReducerClass(ScoreReducer.class);

        //加载map输出类型和value的输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(ScoreBean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(ScoreBean.class);

        FileInputFormat.setInputPaths(job, new Path("D:\\IntelliJ\\IdeaProjects\\big-data-project\\hadoop\\src\\main\\resources\\input\\score"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\IntelliJ\\IdeaProjects\\big-data-project\\hadoop\\src\\main\\resources\\output\\score"));

        boolean b = job.waitForCompletion(true);

        System.exit(b ? 0 : 1);
    }

}
