package com.zhangyong.spark.streaming

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @Author: 张勇
 * @Blog: https://blog.csdn.net/zy13765287861
 * @Version: 1.0
 * @Date: 2020-08-04 11:23
 * @PS: 自定义函数采集器
 */
object SparkStreaming03_MyReceiver {
  def main(args: Array[String]): Unit = {
    //spark配置对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming01_WordCount")

    //实时数据分析环境对象
    //采集周期,以指定的时间为周期采集实时数据
    val streamingContext = new StreamingContext(sparkConf, Seconds(5))

    //从指定的端口采集数据
    val receiverStream = streamingContext.receiverStream(new MyReceiver("hadoop201", 9999))

    //将采集的数据进行分解（扁平化）
    val wordDStream = receiverStream.flatMap(line => line.split(" "))

    //将数据进行结构的转换方便统计分析
    val mapDStream = wordDStream.map((_, 1))

    //将转换化的数据进行聚合
    val wordToSumStream = mapDStream.reduceByKey(_ + _)

    //打印结构
    wordToSumStream.print()

    //不能停止采集程序
    //streamingContext.stop()

    //启动采集器
    streamingContext.start()

    //Driver等待采集器的执行
    streamingContext.awaitTermination();
  }

  class MyReceiver(host: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_ONLY) {

    var socket: java.net.Socket = null;

    def receive(): Unit = {
      socket = new Socket(host, port)
      val reader = new BufferedReader(new InputStreamReader(socket.getInputStream, "UTF-8"))
      var line: String = null;
      while ((line = reader.readLine()) != null) {
        //将采集的数据存储到采集器的内部进行转换
        if ("END".equals(line)) {
          return
        } else {
          this.store(line)
        }
      }
    }

    override def onStart(): Unit = {
      new Thread(new Runnable {
        override def run(): Unit = {
          receive()
        }
      }).start()
    }

    override def onStop(): Unit = {
      if (socket != null) {
        socket.close()
        socket = null
      }
    }
  }

}
