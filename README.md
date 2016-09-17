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
