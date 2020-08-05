package com.zhangyong.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @Author: 张勇
 * @Blog: https://blog.csdn.net/zy13765287861
 * @Version: 1.0
 * @Date: 2020-08-05 11:59
 * @PS:
 */
public class HdfsIoTest {

    /**
     * 文件上传
     *
     * @throws URISyntaxException
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void putFileToHdfs() throws URISyntaxException, IOException, InterruptedException {
        //虚拟机连接名，必须在本地配置域名，不然只能IP地址访问
        String hdfs = "hdfs://hadoop201:9000";
        // 1 获取文件系统
        Configuration cfg = new Configuration();
        FileSystem fs = FileSystem.get(new URI(hdfs), cfg, "zhangyong");
        //2 创建输入流
        FileInputStream fis = new FileInputStream(new File("D:\\IO\\call.log"));
        //3 获取输出流
        FSDataOutputStream fos = fs.create(new Path("/call.log"));
        //4 流对拷
        IOUtils.copyBytes(fis, fos, cfg);
        //5 关闭资源
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        fs.close();
    }

    /**
     * 下载到本地
     *
     * @throws URISyntaxException
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void getFileFromHdfs() throws URISyntaxException, IOException, InterruptedException {
        //虚拟机连接名，必须在本地配置域名，不然只能IP地址访问
        String hdfs = "hdfs://hadoop201:9000";
        // 1 获取文件系统
        Configuration cfg = new Configuration();
        FileSystem fs = FileSystem.get(new URI(hdfs), cfg, "zhangyong");
        //2、获取输入流
        FSDataInputStream fis = fs.open(new Path("/call.log"));
        //3、获取输出流
        FileOutputStream fos = new FileOutputStream(new File("D:\\call.log"));
        //4、流的拷贝
        IOUtils.copyBytes(fis, fos, cfg);
        //5、关闭流
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        fs.close();
    }


}
