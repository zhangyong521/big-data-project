package com.zhangyong.spark.hbase

import java.sql.DriverManager

import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration}
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: 张勇
 * @Blog: https://blog.csdn.net/zy13765287861
 * @Version: 1.0
 * @Date: 2020-07-31 17:33
 * @PS: 写数据
 */
object WriteHBaseRDD {

  def main(args: Array[String]): Unit = {
    //1.初始化spark配置信息并建立与spark的连接
    val config = new SparkConf().setMaster("local[*]").setAppName("Practice")
    //2.创建SparkContext连接
    val sc = new SparkContext(config)

    //3.定义连接mysql的参数
    //3.构建HBase配置信息
    val conf = HBaseConfiguration.create()
    conf.set(TableInputFormat.INPUT_TABLE, "rddtable")

    val dataRDD = sc.makeRDD(List(("1002","zhangsi"),("1003","zhangwu"),("1004","zhangliu")))

    val putRDD = dataRDD.map {
      case (rowkey, name) => {
        val put = new Put(Bytes.toBytes(rowkey))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(name))

        (new ImmutableBytesWritable(Bytes.toBytes(rowkey)), put)
      }
    }
    val jobConf = new JobConf(conf)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE,"rddtable")

    putRDD.saveAsHadoopDataset(jobConf)

    //6.关闭
    sc.stop()
  }
}
