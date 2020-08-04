package com.atguigu.base

/**
 * @Author: 张勇
 * @Blog: https://blog.csdn.net/zy13765287861
 * @Version: 1.0
 * @Date: 2020-06-01 12:49
 * @PS:
 */
object ScalaTest {
  def main(args: Array[String]): Unit = {
    val name = "ApacheCN"
    val age = 1
    val url = "www.zhangyong.com"

    var num: Int = 0
    var score: Double = 1.0
    var gender: Char = 'N'
    var studentName:String = "scott"



    //字符串通过+号连接（类似于java)
    println("name=" + name + ",age=" + age + ",url=" + url)
    //printf用法（类似于C语言）字符串通过%传值。（格式化输出）
    printf("name=%s,age=%d,url=%s\n", name, age, url)
    //字符串插值：通过$引用（类似于PHP)
    println(s"name=$name,age$age,url=$url")

  }
}
