package com.zhangyong.yongchuan.reducejion;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

/**
 * @Author: 张勇
 * @Blog: https://blog.csdn.net/zy13765287861
 * @Version: 1.0
 * @Date: 2020-10-28 9:05
 * @PS:
 */
public class TableReducer extends Reducer<Text, TableBean, TableBean, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Context context) throws IOException, InterruptedException {
        //1、准备存储订单的集合
        ArrayList<TableBean> orderBeans = new ArrayList<>();

        //2、准备bean对象
        TableBean pdBean = new TableBean();

        for (TableBean value : values) {
            if ("order".equals(value.getFlag())) {
                TableBean orderBean = new TableBean();
                try {
                    BeanUtils.copyProperties(orderBean, value);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                orderBeans.add(orderBean);
            } else {
                try {
                    BeanUtils.copyProperties(pdBean, value);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        // 3 表的拼接
        for (TableBean bean : orderBeans) {
            bean.setPname(pdBean.getPname());
            // 4 数据写出去
            context.write(bean, NullWritable.get());
        }

    }
}
