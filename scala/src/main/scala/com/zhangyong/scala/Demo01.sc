object Demo01 {
  //定义i
  var i = 1
  //打印1-99
  for (i <- 1 to 99) {
    //判断条件
    if (i % 7 == 0 || i % 10 == 7 || i / 10 % 10 == 7) {
      print("过")

    } else {
      printf(" ")
      println(i)
    }
  }

}