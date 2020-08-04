package com.atguigu.base

/**
 * @Author: 张勇
 * @Blog: https://blog.csdn.net/zy13765287861
 * @Version: 1.0
 * @Date: 2020-06-06 14:20
 * @PS:
 */
object Test {
  def main(args: Array[String]): Unit = {
    for(i<-1 to 9;j<-1 to i){
      print(j+"*"+i+"="+j*i+" ")
      if(j==i)  println()
    }
  }
}
