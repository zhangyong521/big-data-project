package com.zhangyong.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * @Author: 张勇
 * @Blog: https://blog.csdn.net/zy13765287861
 * @Version: 1.0
 * @Date: 2020-08-05 17:18
 * @PS: Hive自定义操作
 */
public class HiveUdf extends UDF {
    /**
     * 自定义开发的转大写功能。
     * 覆盖evaluate方法
     */
    public String evaluate(String string) {
        return string.toUpperCase();
    }

}
