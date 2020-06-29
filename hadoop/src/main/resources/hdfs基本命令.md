### hdfs的shell命令
#### 1．基本语法
    bin/hadoop fs 具体命令   OR  bin/hdfs dfs 具体命令
    dfs是fs的实现类。
#### 2．命令大全
    [zhangyong@hadoop102 hadoop-2.7.2]$ bin/hadoop fs
    
    [-appendToFile <localsrc> ... <dst>]
            [-cat [-ignoreCrc] <src> ...]
            [-checksum <src> ...]
            [-chgrp [-R] GROUP PATH...]
            [-chmod [-R] <MODE[,MODE]... | OCTALMODE> PATH...]
            [-chown [-R] [OWNER][:[GROUP]] PATH...]
            [-copyFromLocal [-f] [-p] <localsrc> ... <dst>]
            [-copyToLocal [-p] [-ignoreCrc] [-crc] <src> ... <localdst>]
            [-count [-q] <path> ...]
            [-cp [-f] [-p] <src> ... <dst>]
            [-createSnapshot <snapshotDir> [<snapshotName>]]
            [-deleteSnapshot <snapshotDir> <snapshotName>]
            [-df [-h] [<path> ...]]
            [-du [-s] [-h] <path> ...]
            [-expunge]
            [-get [-p] [-ignoreCrc] [-crc] <src> ... <localdst>]
            [-getfacl [-R] <path>]
            [-getmerge [-nl] <src> <localdst>]
            [-help [cmd ...]]
            [-ls [-d] [-h] [-R] [<path> ...]]
            [-mkdir [-p] <path> ...]
            [-moveFromLocal <localsrc> ... <dst>]
            [-moveToLocal <src> <localdst>]
            [-mv <src> ... <dst>]
            [-put [-f] [-p] <localsrc> ... <dst>]
            [-renameSnapshot <snapshotDir> <oldName> <newName>]
            [-rm [-f] [-r|-R] [-skipTrash] <src> ...]
            [-rmdir [--ignore-fail-on-non-empty] <dir> ...]
            [-setfacl [-R] [{-b|-k} {-m|-x <acl_spec>} <path>]|[--set <acl_spec> <path>]]
            [-setrep [-R] [-w] <rep> <path> ...]
            [-stat [format] <path> ...]
            [-tail [-f] <file>]
            [-test -[defsz] <path>]
            [-text [-ignoreCrc] <src> ...]
            [-touchz <path> ...]
            [-usage [cmd ...]]
#### 3．常用命令实操
##### （0）启动Hadoop集群（方便后续的测试）
    [zhangyong@hadoop102 hadoop-2.7.2]$ sbin/start-dfs.sh
    [zhangyong@hadoop103 hadoop-2.7.2]$ sbin/start-yarn.sh
#### （1）-help：输出这个命令参数
    [zhangyong@hadoop102 hadoop-2.7.2]$ hadoop fs -help rm
#### （2）-ls: 显示目录信息
    [zhangyong@hadoop102 hadoop-2.7.2]$ hadoop fs -ls /
#### （3）-mkdir：在HDFS上创建目录
    [zhangyong@hadoop102 hadoop-2.7.2]$ hadoop fs -mkdir -p /sanguo/shuguo
#### （4）-moveFromLocal：从本地剪切粘贴到HDFS
    [zhangyong@hadoop102 hadoop-2.7.2]$ touch kongming.txt
    [zhangyong@hadoop102 hadoop-2.7.2]$ hadoop fs  -moveFromLocal  ./kongming.txt  /sanguo/shuguo
#### （5）-appendToFile：追加一个文件到已经存在的文件末尾
    [zhangyong@hadoop102 hadoop-2.7.2]$ touch liubei.txt
    [zhangyong@hadoop102 hadoop-2.7.2]$ vi liubei.txt
    输入
    san gu mao lu
    [zhangyong@hadoop102 hadoop-2.7.2]$ hadoop fs -appendToFile liubei.txt /sanguo/shuguo/kongming.txt
#### （6）-cat：显示文件内容
    [zhangyong@hadoop102 hadoop-2.7.2]$ hadoop fs -cat /sanguo/shuguo/kongming.txt
#### （7）-chgrp 、-chmod、-chown：Linux文件系统中的用法一样，修改文件所属权限
    [zhangyong@hadoop102 hadoop-2.7.2]$ hadoop fs  -chmod  666  /sanguo/shuguo/kongming.txt
    [zhangyong@hadoop102 hadoop-2.7.2]$ hadoop fs  -chown  zhangyong:zhangyong   /sanguo/shuguo/kongming.txt
#### （8）-copyFromLocal：从本地文件系统中拷贝文件到HDFS路径去
    [zhangyong@hadoop102 hadoop-2.7.2]$ hadoop fs -copyFromLocal README.txt /
#### （9）-copyToLocal：从HDFS拷贝到本地
    [zhangyong@hadoop102 hadoop-2.7.2]$ hadoop fs -copyToLocal /sanguo/shuguo/kongming.txt ./
#### （10）-cp ：从HDFS的一个路径拷贝到HDFS的另一个路径
    [zhangyong@hadoop102 hadoop-2.7.2]$ hadoop fs -cp /sanguo/shuguo/kongming.txt /zhuge.txt
#### （11）-mv：在HDFS目录中移动文件
    [zhangyong@hadoop102 hadoop-2.7.2]$ hadoop fs -mv /zhuge.txt /sanguo/shuguo/
#### （12）-get：等同于copyToLocal，就是从HDFS下载文件到本地
    [zhangyong@hadoop102 hadoop-2.7.2]$ hadoop fs -get /sanguo/shuguo/kongming.txt ./
#### （13）-getmerge：合并下载多个文件，比如HDFS的目录 /user/zhangyong/test下有多个文件:log.1, log.2,log.3,...
    [zhangyong@hadoop102 hadoop-2.7.2]$ hadoop fs -getmerge /user/zhangyong/test/* ./zaiyiqi.txt
#### （14）-put：等同于copyFromLocal
    [zhangyong@hadoop102 hadoop-2.7.2]$ hadoop fs -put ./zaiyiqi.txt /user/zhangyong/test/
#### （15）-tail：显示一个文件的末尾
    [zhangyong@hadoop102 hadoop-2.7.2]$ hadoop fs -tail /sanguo/shuguo/kongming.txt
#### （16）-rm：删除文件或文件夹
    [zhangyong@hadoop102 hadoop-2.7.2]$ hadoop fs -rm /user/zhangyong/test/jinlian2.txt
#### （17）-rmdir：删除空目录
    [zhangyong@hadoop102 hadoop-2.7.2]$ hadoop fs -mkdir /test
    [zhangyong@hadoop102 hadoop-2.7.2]$ hadoop fs -rmdir /test
#### （18）-du统计文件夹的大小信息
    [zhangyong@hadoop102 hadoop-2.7.2]$ hadoop fs -du -s -h /user/zhangyong/test
        2.7 K  /user/zhangyong/test
    [zhangyong@hadoop102 hadoop-2.7.2]$ hadoop fs -du  -h /user/zhangyong/test
        1.3 K  /user/zhangyong/test/README.txt
        15     /user/zhangyong/test/jinlian.txt
        1.4 K  /user/zhangyong/test/zaiyiqi.txt
#### （19）-setrep：设置HDFS中文件的副本数量
    [zhangyong@hadoop102 hadoop-2.7.2]$ hadoop fs -setrep 10 /sanguo/shuguo/kongming.txt
