package com.zhangyong.scala

/**
 * @Author: 张勇
 * @Blog: https://blog.csdn.net/zy13765287861
 * @Version: 1.0
 * @Date: 2020-06-12 10:05
 * @PS:
 */
object HelloScala {

  def f2(x: Double, n: Int): Double = {
    if (n == 0) 1
    else if (n > 0 && n % 2 == 0) f2(x, n / 2) * f2(x, n / 2)
    else if (n > 0 && n % 2 == 1) x * f2(x, n - 1)
    else 1 / f2(x, -n)

  }

  def main(args: Array[String]): Unit = {
    println(f2(2, 4))
    println(f2(2, -2))
    println(f2(2, 0))
  }
}
