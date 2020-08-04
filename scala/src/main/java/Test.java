/**
 * @Author: 张勇
 * @Blog: https://blog.csdn.net/zy13765287861
 * @Version: 1.0
 * @Date: 2020-06-13 17:54
 * @PS:
 */
public class Test {
    public static void main(String[] args) {
        for (int i = 1; i <= 99; i++) {
            if (i % 7 == 0 || i % 10 == 7 || i / 10 % 10 == 7) {
                System.out.println( "跳过");
            } else {
                System.out.print(" ");
                System.out.print(i);
            }
        }
    }
}