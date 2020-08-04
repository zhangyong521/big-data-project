package com.zhangyong.spark.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructType}

/**
 * @Author: 张勇
 * @Blog: https://blog.csdn.net/zy13765287861
 * @Version: 1.0
 * @Date: 2020-08-03 17:05
 * @PS: 自定义聚合函数
 */
object SparkSQL05_UDAF {
  def main(args: Array[String]): Unit = {

    //1.初始化spark配置信息并建立与spark的连接
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL01_Demo")
    //2.创建SparkSQL连接

    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    //自定义聚合函数
    //创建聚合函数对象
    val udaf = new MyAgeAvgFunction
    //注册聚合函数
    spark.udf.register("avgAge", udaf)

    //使用聚合函数
    //读取数据
    val frame = spark.read.json("spark/in/user.json")
    frame.createOrReplaceTempView("user")

    spark.sql("select avgAge(age) from user").show()

    //释放资源
    spark.stop()

  }

  //1、继承UserDefinedAggregateFunction
  //2、实现方法
  class MyAgeAvgFunction extends UserDefinedAggregateFunction {
    //函数输入的数据结构
    override def inputSchema: StructType = {
      new StructType().add("age", LongType)
    }

    //计算时的数据结构
    override def bufferSchema: StructType = {
      new StructType().add("sum", LongType).add("count", LongType)
    }

    //函数返回的数据类型
    override def dataType: DataType = DoubleType

    //函数是否稳定
    override def deterministic: Boolean = true

    //计算之前的缓冲区的初始值
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = 0L
      buffer(1) = 0L
    }

    //根据查询结果更新缓冲区数据
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      buffer(0) = buffer.getLong(0) + input.getLong(0)
      buffer(1) = buffer.getLong(1) + 1
    }

    //将多个节点的缓冲区合并
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      //sum
      buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
      //count
      buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
    }

    //计算
    override def evaluate(buffer: Row): Any = {
      buffer.getLong(0).toDouble / buffer.getLong(1)
    }

  }

}

