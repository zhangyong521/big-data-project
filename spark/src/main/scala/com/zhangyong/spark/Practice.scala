package com.zhangyong.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: 张勇
 * @Blog: https://blog.csdn.net/zy13765287861
 * @Version: 1.0
 * @Date: 2020-07-30 17:01
 * @PS: 统计出每个省份广告被点击次数的TOP3
 */
object Practice {
  def main(args: Array[String]): Unit = {
    //1.初始化spark配置信息并建立与spark的连接
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Practice")
    //创建Spark上下文对象
    val sc = new SparkContext(config)

    //2.读取数据生成RDD：TS，province，city，User，AD
    val line = sc.textFile("D:\\IntelliJ\\IdeaProjects\\big-data-project\\spark\\src\\main\\resources\\agent.log")

    //3.按照最小粒度聚合
    val provinceAdToOne = line.map {x =>
        val fields = x.split(" ")
        ((fields(1),fields(4)), 1)
    }

    //4.计算每个省份每个广告被点击的次数：（（province，AD），sum）
    val provinceToSum = provinceAdToOne.reduceByKey(_+_)

    //5.将省份作为key，广告加点击数为value：（（Province）,(AD,sum)）
    val provinceAdToSum = provinceToSum.map(x=>(x._1._1,(x._1._2,x._2)))

    //6.将同一个省份的所有广告进行聚合
    val provinceGroup = provinceAdToSum.groupByKey()

    //7.对同一个省份所有的广告的集合进行排序并取前3条，排序规则为广告点击总数
    val provinceAdTop3 = provinceGroup.mapValues {
      x => x.toList.sortWith((x, y) => x._2 > y._2).take(3)
    }

    //8.将数据拉取到Driver段并打印
    provinceAdTop3.collect().foreach(println)

    //9.关闭与spark的连接
    sc.stop()




  }
}
