package com.zhangyong.zk;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

/**
 * @Author: 张勇
 * @Blog: https://blog.csdn.net/zy13765287861
 * @Version: 1.0
 * @Date: 2020-08-05 16:36
 * @PS: zookeeper的客户端操作
 */
public class zkCli {
    private static String connectString = "hadoop201:2181,hadoop202:2181,hadoop203:2181";
    private static int sessionTimeout = 2000;
    private ZooKeeper zkClient = null;

    /**
     * 创建ZooKeeper客户端
     *
     * @throws Exception
     */
    @Before
    public void init() throws Exception {
        zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                // 收到事件通知后的回调函数（用户的业务逻辑）
                System.out.println(event.getType() + "--" + event.getPath());
                // 再次启动监听
                try {
                    zkClient.getChildren("/", true);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }

    /**
     * 创建子节点
     *
     * @throws Exception
     */
    @Test
    public void create() throws Exception {
        // 参数1：要创建的节点的路径； 参数2：节点数据 ； 参数3：节点权限 ；参数4：节点的类型
        String nodeCreated = zkClient.create("/zhangyong", "zhangrui".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    /**
     * 获取子节点并监听节点变化
     *
     * @throws Exception
     */
    @Test
    public void getChildren() throws Exception {
        List<String> children = zkClient.getChildren("/", true);
        for (String child : children) {
            System.out.println(child);
        }
        // 延时阻塞
        Thread.sleep(Long.MAX_VALUE);
    }

    /**
     * 判断znode是否存在
     *
     * @throws Exception
     */
    @Test
    public void exist() throws Exception {
        Stat stat = zkClient.exists("/zhangyong", false);
        System.out.println(stat == null ? "not exist" : "exist");
    }
}
