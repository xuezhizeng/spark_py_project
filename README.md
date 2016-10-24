## Spark (Lightning-fast cluster computing)

[spark docs](http://spark.apache.org/docs/latest/)


新增环境变量
```
$ sudo vim /etc/profile
```

内容如下
```
export SPARK_HOME=$HOME/tools/spark-2.0.0-bin-hadoop2.7
export PYTHONPATH=$SPARK_HOME/python/:$SPARK_HOME/python/lib/py4j-0.10.1-src.zip:$PYTHONPATH
```

立即生效
```
$ source /etc/profile
```

执行示例脚本
```
$ cd examples/src/main/python
$ python pi.py
```

结果如下
```
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel).
16/09/17 21:15:54 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
16/09/17 21:15:55 WARN Utils: Your hostname, ThinkPad-L421 resolves to a loopback address: 127.0.1.1; using 192.168.1.180 instead (on interface wlan0)
16/09/17 21:15:55 WARN Utils: Set SPARK_LOCAL_IP if you need to bind to another address
[Stage 0:>                                                          (0 + 0) / 2]16/09/17 21:15:59 WARN TaskSetManager: Stage 0 contains a task of very large size (368 KB). The maximum recommended task size is 100 KB.
Pi is roughly 3.142540 
```


注意：使用 zsh 的用户, 需要为 zsh 设置环境变量
```
$ vim ~/.zshrc
$ source ~/.zshrc
```

或者仅为当前用户设置环境变量
```
$ vim ~/.bashrc
$ source ~/.bashrc
```


配置 pycharm 环境
```
$ ln -s ~/tools/spark-2.0.0-bin-hadoop2.7 spark
spark/python >> Mark Directory as >> Sources Root
$ pip install py4j
```

## 运行

1、启动 master
```
$ spark/sbin/start-master.sh
```

http://localhost:8080

Spark Master at spark://zhanghedeMacBook-Pro.local:7077

关闭 master
```
$ spark/sbin/stop-master.sh
```

2、启动 worker
```
$ spark/bin/spark-class org.apache.spark.deploy.worker.Worker spark://0.0.0.0:7077
```

3、启动 spark-shell
```
$ spark/bin/spark-submit --master=spark://zhanghedeMacBook-Pro.local:7077 sms/run.py
```


## 配置

解决 spark-shell 输出日志信息过多

配置 log4j.properties 日志级别
```
cp spark/conf/log4j.properties.template spark/conf/log4j.properties
vim spark/conf/log4j.properties
log4j.rootCategory=INFO, console 中的 INFO 改为 WARN
```
