### 2.4.2 安装使用
###### 0）下载spark安装包解压到/opt/module目录中
###### 1）进入spark安装目录下的conf文件夹
    [zhangyong@hadoop201 module]$ cd spark/conf/
###### 2）修改配置文件名称
    [zhangyong@hadoop201 conf]$ mv slaves.template slaves
    [zhangyong@hadoop201 conf]$ mv spark-env.sh.template spark-env.sh
###### 3）修改slave文件，添加work节点：
    [zhangyong@hadoop201 conf]$ vim slaves
    hadoop201
    hadoop202
    hadoop203
###### 4）修改spark-env.sh文件，添加如下配置：
    [zhangyong@hadoop201 conf]$ vim spark-env.sh
    SPARK_MASTER_HOST=hadoop201
    SPARK_MASTER_PORT=7077
###### 5）分发spark包
    [zhangyong@hadoop201 module]$ xsync spark/
###### 6）启动
    [zhangyong@hadoop201 spark]$ sbin/start-all.sh
    [zhangyong@hadoop201 spark]$ jpsall 
    ================zhangyong@hadoop201================
    3330 Jps
    3238 Worker
    3163 Master
    ================zhangyong@hadoop202================
    2966 Jps
    2908 Worker
    ================zhangyong@hadoop203================
    2978 Worker
    3036 Jps
###### 网页查看：hadoop201:8080
    注意：如果遇到 “JAVA_HOME not set” 异常，可以在sbin目录下的spark-config.sh 文件中加入如下配置：
    export JAVA_HOME=XXXX
###### 7）官方求PI案例
    [zhangyong@hadoop201 spark]$ bin/spark-submit \
    --class org.apache.spark.examples.SparkPi \
    --master spark://hadoop201:7077 \
    --executor-memory 1G \
    --total-executor-cores 2 \
    ./examples/jars/spark-examples_2.11-2.1.1.jar \
    100
###### 8）启动spark shell
    [zhangyong@hadoop201 spark]$/bin/spark-shell \
    --master spark://hadoop201:7077 \
    --executor-memory 1g \
    --total-executor-cores 2
    参数：--master spark://hadoop201:7077指定要连接的集群的master
    执行WordCount程序
######  案例：
    scala>sc.textFile("input").flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).collect
    res0: Array[(String, Int)] = Array((hadoop,6), (oozie,3), (spark,3), (hive,3), (zhangyong,3), (hbase,6))
### 2.4.3 JobHistoryServer配置
###### 1）修改spark-default.conf.template名称
    [zhangyong@hadoop201 conf]$ mv spark-defaults.conf.template spark-defaults.conf
###### 2）修改spark-default.conf文件，开启Log：
    [zhangyong@hadoop201 conf]$ vi spark-defaults.conf
    spark.eventLog.enabled           true
    spark.eventLog.dir               hdfs://hadoop201:9000/directory
###### 注意：HDFS上的目录需要提前存在。
    [zhangyong@hadoop201 hadoop]$ hadoop fs –mkdir /directory
###### 3）修改spark-env.sh文件，添加如下配置：
    [zhangyong@hadoop201 conf]$ vi spark-env.sh
    export SPARK_HISTORY_OPTS="-Dspark.history.ui.port=18080 
    -Dspark.history.retainedApplications=30 
    -Dspark.history.fs.logDirectory=hdfs://hadoop201:9000/directory"
    参数描述：
    spark.eventLog.dir：Application在运行过程中所有的信息均记录在该属性指定的路径下； 
    spark.history.ui.port=18080  WEBUI访问的端口号为18080
    spark.history.fs.logDirectory=hdfs://hadoop201:9000/directory  配置了该属性后，在start-history-server.sh时就无需再显式的指定路径，Spark History Server页面只展示该指定路径下的信息
    spark.history.retainedApplications=30指定保存Application历史记录的个数，如果超过这个值，旧的应用程序信息将被删除，这个是内存中的应用数，而不是页面上显示的应用数。
###### 4）分发配置文件
    [zhangyong@hadoop201 conf]$ xsync spark-defaults.conf
    [zhangyong@hadoop201 conf]$ xsync spark-env.sh
###### 5）启动历史服务
    [zhangyong@hadoop201 spark]$ sbin/start-history-server.sh
###### 6）再次执行任务
    [zhangyong@hadoop201 spark]$ bin/spark-submit \
    --class org.apache.spark.examples.SparkPi \
    --master spark://hadoop201:7077 \
    --executor-memory 1G \
    --total-executor-cores 2 \
    ./examples/jars/spark-examples_2.11-2.1.1.jar \
    100
###### 7）查看历史服务
    hadoop201:18080
