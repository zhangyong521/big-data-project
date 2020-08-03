package com.zhangyong.spark.hbase


import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration}
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: 张勇
 * @Blog: https://blog.csdn.net/zy13765287861
 * @Version: 1.0
 * @Date: 2020-07-31 17:33
 * @PS: 读数据
 */
object ReadHBaseRDD {

  def main(args: Array[String]): Unit = {
    //1.初始化spark配置信息并建立与spark的连接
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Practice")
    //2.创建SparkContext连接
    val sc = new SparkContext(config)

    //3.构建HBase配置信息
    val conf = HBaseConfiguration.create()
    conf.set(TableInputFormat.INPUT_TABLE, "rddtable")

    //从HBase读取数据形成RDD
    val hbaseRDD = sc.newAPIHadoopRDD(
      conf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]
    )
    hbaseRDD.foreach {
      case (rowkey, result) => {
        val cells = result.rawCells()
        for (cell <- cells) {
          println(Bytes.toString(CellUtil.cloneValue(cell)))
        }
      }
    }

    //6.关闭
    sc.stop()
  }
}
