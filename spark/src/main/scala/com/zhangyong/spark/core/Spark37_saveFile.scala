package com.zhangyong.spark.core
import org.apache.spark.{SparkConf, SparkContext}
/**
 * @Author: 张勇
 * @Blog: https://blog.csdn.net/zy13765287861
 * @Version: 1.0
 * @Date: 2020-07-28 17:36
 * @PS: 将数据集的元素以textfile的形式保存到HDFS文件系统或者其他支持的文件系统，
 *     对于每个元素，spark将会调用toString方法，将他转换为文件中的文本
 */
object Spark37_saveFile {
  def main(args: Array[String]): Unit = {
    //local模式
    //创建sparkConf对象
    //设定spark计算框架的运算（部署）环境
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc = new SparkContext(config)


    /**
     * 1.saveAsTestFile(path)将数据集的元素以textfile的形式保存到HDFS文件系统或者其他支持的文件系统，
     * 对于每个元素，spark将会调用toString方法，将他转换为文件中的文本
     * 2.saveAsSequenceFile(path)将数据集中的元素以Hadoop sequence file的格式保存到指定的目录下，
     * 可以使HDFS或者其他Hadoop支持的文件系统
     * 3.saveAsObjectFile(path)用于将RDD中的元素序列化成对象，存储到文件中
     */

  }

}
