package com.zhangyong.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @Author: 张勇
 * @Blog: https://blog.csdn.net/zy13765287861
 * @Version: 1.0
 * @Date: 2020-06-29 10:54
 * @PS: HDFS的客户端基本操作
 */
public class HdfsTest {
    /**
     * 测试java连接Hadoop的HDFS
     *
     * @throws URISyntaxException
     * @throws IOException
     */
    @Test
    public void test() throws URISyntaxException, IOException, InterruptedException {
        //虚拟机连接名，必须在本地配置域名，不然只能IP地址访问
        String hdfs = "hdfs://hadoop201:9000";
        // 1 获取文件系统
        Configuration cfg = new Configuration();
        FileSystem fs = FileSystem.get(new URI(hdfs), cfg, "zhangyong");

        System.out.println(cfg);
        System.out.println(fs);
        System.out.println("HDFS开启了！！！");
    }

    /**
     * 从本地中上传文件到HDFS
     *
     * @throws URISyntaxException
     * @throws IOException
     */
    @Test
    public void put() throws IOException, InterruptedException {
        //虚拟机连接名，必须在本地配置域名，不然只能IP地址访问
        String hdfs = "hdfs://hadoop201:9000";
        //1、读取一个HDFS的抽象封装对象
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(URI.create(hdfs), configuration, "zhangyong");

        //用这个对象操作文件系统
        fileSystem.copyFromLocalFile(new Path("d:\\IO\\upload"), new Path("/"));

        System.out.println("上传完成");
        //关闭文件系统
        fileSystem.close();
    }

    /**
     * 从HDFS中下载文件
     *
     * @throws URISyntaxException
     * @throws IOException
     */
    @Test
    public void get() throws IOException, InterruptedException {
        //虚拟机连接名，必须在本地配置域名，不然只能IP地址访问
        String hdfs = "hdfs://hadoop201:9000";
        //1、读取一个HDFS的抽象封装对象
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(URI.create(hdfs), configuration, "zhangyong");

        //用这个对象操作文件系统
        fileSystem.copyToLocalFile(new Path("/result"), new Path("d:\\"));

        System.out.println("下载完成");
        //关闭文件系统
        fileSystem.close();
    }

    /**
     * 更改HDFS文件的名称
     *
     * @throws URISyntaxException
     * @throws IOException
     */
    @Test
    public void rename() throws IOException, InterruptedException {
        //虚拟机连接名，必须在本地配置域名，不然只能IP地址访问
        String hdfs = "hdfs://hadoop201:9000";
        //1、读取一个HDFS的抽象封装对象
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(URI.create(hdfs), configuration, "zhangyong");

        //用这个对象操作文件系统
        fileSystem.rename(new Path("/upload"), new Path("/up"));

        System.out.println("更改完成");
        //关闭文件系统
        fileSystem.close();
    }

    /**
     * HDFS文件夹删除
     *
     * @throws IOException
     * @throws InterruptedException
     * @throws URISyntaxException
     */
    @Test
    public void Delete() throws IOException, InterruptedException, URISyntaxException {
        //虚拟机连接名，必须在本地配置域名，不然只能IP地址访问
        String hdfs = "hdfs://hadoop201:9000";
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI(hdfs), configuration, "zhangyong");
        // 2 执行删除
        fileSystem.delete(new Path("/anshun/"), true);
        // 3 关闭资源
        fileSystem.close();
        System.out.println("删除完成");
    }

    /**
     * 查看文件名称、权限、长度、块信息
     *
     * @throws IOException
     * @throws InterruptedException
     * @throws URISyntaxException
     */
    @Test
    public void ListFiles() throws IOException, InterruptedException, URISyntaxException {
        //虚拟机连接名，必须在本地配置域名，不然只能IP地址访问
        String hdfs = "hdfs://hadoop201:9000";
        // 1获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI(hdfs), configuration, "zhangyong");

        // 2 获取文件详情
        RemoteIterator<LocatedFileStatus> listFiles = fileSystem.listFiles(new Path("/"), true);

        while (listFiles.hasNext()) {
            LocatedFileStatus status = listFiles.next();

            // 输出详情
            // 文件名称
            System.out.println("文件名称=" + status.getPath().getName());
            // 长度
            System.out.println("文件长度=" + status.getLen());
            // 权限
            System.out.println("文件权限=" + status.getPermission());
            // 分组
            System.out.println("文件分组=" + status.getGroup());

            // 获取存储的块信息
            BlockLocation[] blockLocations = status.getBlockLocations();

            for (BlockLocation blockLocation : blockLocations) {

                // 获取块存储的主机节点
                String[] hosts = blockLocation.getHosts();

                for (String host : hosts) {
                    System.out.println(host);
                }
            }

            System.out.println("-----------分割线----------");
        }

        // 3 关闭资源
        fileSystem.close();
    }

    /**
     * 查看HDFS的判断是文件还是文件夹
     *
     * @throws IOException
     * @throws InterruptedException
     * @throws URISyntaxException
     */
    @Test
    public void ListStatus() throws IOException, InterruptedException, URISyntaxException {
        //虚拟机连接名，必须在本地配置域名，不然只能IP地址访问
        String hdfs = "hdfs://hadoop201:9000";
        // 1 获取文件配置信息
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI(hdfs), configuration, "zhangyong");

        // 2 判断是文件还是文件夹
        FileStatus[] listStatus = fileSystem.listStatus(new Path("/"));

        for (FileStatus fileStatus : listStatus) {

            // 如果是文件
            if (fileStatus.isFile()) {
                System.out.println("文件:" + fileStatus.getPath().getName());
            } else {
                System.out.println("文件夹:" + fileStatus.getPath().getName());
            }
        }

        // 3 关闭资源
        fileSystem.close();
    }

    /**
     * 在HDFS创建目录
     *
     * @throws Exception
     */
    @Test
    public void mkdir() throws Exception {

        //虚拟机连接名，必须在本地配置域名，不然只能IP地址访问
        String hdfs = "hdfs://hadoop201:9000";
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI(hdfs), configuration, "zhangyong");
        // 2 执行删除
        fileSystem.mkdirs(new Path("/anshun"));
        System.out.println("创建完成");
        // 3 关闭资源
        fileSystem.close();
    }

}
