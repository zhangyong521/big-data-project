package com.zhangyong;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceNotFoundException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.junit.Test;

import java.io.IOException;

/**
 * @Author: 张勇
 * @Blog: https://blog.csdn.net/zy13765287861
 * @Version: 1.0
 * @Date: 2020-05-29 14:16
 * @PS: HBase测试
 */
public class HBaseAPITest {

    /**
     * 判断HBase的命名空间是否存在,不存在则创建
     */
    @Test
    public void isNameExist() throws IOException {
        //ZK获取HBase配置
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "hadoop201:2181,hadoop202:2181,hadoop203:2181");
        //HBase的客户端连接
        Connection connection = ConnectionFactory.createConnection(conf);
        //获取权限
        Admin admin = connection.getAdmin();

        try {
            // 1.判断HBase里面的命名空间
            NamespaceDescriptor zhangyong = admin.getNamespaceDescriptor("zhangyong");
            System.out.println(zhangyong);
        } catch (NamespaceNotFoundException e) {
            // 2. 如果不存在则创建
            NamespaceDescriptor nd = NamespaceDescriptor.create("zhangyong").build();
            admin.createNamespace(nd);
        }
    }

    /**
     * 判断HBase的表是否存在
     */
    @Test
    public void isTableExist() throws IOException {
        //ZK获取HBase配置
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "hadoop201:2181,hadoop202:2181,hadoop203:2181");
        //HBase的客户端连接
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();

        // 判断HBase里面是否有某张表
        //获取表
        TableName tableName = TableName.valueOf("student");
        boolean flg = admin.tableExists(tableName);
        System.out.println(flg);

    }

    /**
     * HBase列出表
     */
    @Test
    public void listTables() throws IOException {
        //ZK获取HBase配置
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "hadoop201:2181,hadoop202:2181,hadoop203:2181");
        //HBase的客户端连接
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();

        // 列出表
        HTableDescriptor[] hTableDescriptors = admin.listTables();
        //循环表
        for (HTableDescriptor hTableDescriptor : hTableDescriptors) {
            System.out.println(hTableDescriptor.getTableName());
        }

    }

    /**
     * HBase判断表是否禁用
     */
    @Test
    public void disabledTables() throws IOException {
        //ZK获取HBase配置
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "hadoop201:2181,hadoop202:2181,hadoop203:2181");
        //HBase的客户端连接
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();

        TableName tableName = TableName.valueOf("student");
        boolean tableDisabled = admin.isTableDisabled(tableName);
        System.out.println(tableDisabled);

    }


    /**
     * 查看所有命名空间
     *
     * @throws IOException
     */
    @Test
    public void listNamespace() throws IOException {
        //ZK获取HBase配置
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "hadoop201:2181,hadoop202:2181,hadoop203:2181");
        //HBase的客户端连接
        Connection connection = ConnectionFactory.createConnection(conf);
        //获取权限
        Admin admin = connection.getAdmin();

        // 1.判断HBase里面的命名空间

        NamespaceDescriptor[] descriptors = admin.listNamespaceDescriptors();
        for (NamespaceDescriptor descriptor : descriptors) {
            System.out.println(descriptor.getName());
        }

    }

}
