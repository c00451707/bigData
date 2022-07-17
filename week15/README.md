# week15-homework

#### 介绍

极客时间第 15 周 Flink 作业

#### 作业说明

在 `table-walkthrough` 的 `SpendReport.java` 中有如下代码：
```java
report(transactions).executeInsert(“spend_report”);
```
实现 `report` 方法，将 transactions 表经过 report 函数处理后写入到 `spend_report` 表。

每分钟（或小时）计算在五分钟（或小时）内每个账号的平均交易金额（滑动窗口）？使用分钟还是小时作为单位均可。

`docker` 和 `table-walkthrough` 目录来自 [apache/flink-playgrounds](https://github.com/apache/flink-playgrounds)

#### 代码实现

```java
public class SpendReport {
    public static Table report(Table transactions) {
        return transactions
                .window(Slide.over(lit(5).minutes())
                        .every(lit(1).minutes())
                        .on($("transaction_time"))
                        .as("window")
                )
                .groupBy($("account_id"), $("window"))
                .select(
                        $("account_id"),
                        $("window").start().as("log_ts"),
                        $("amount").avg().as("amount"));
    }
}
```

思路：
1. 使用 `Flink Table API`，定义滑动窗口，每分钟触发一次，对 5 分钟窗口进行计算
2. 窗口中按账号(`account_id`)分组，计算平均值

#### 使用说明

使用 `table-walkthrough` 中的 `Dockerfile` 和 `docker-compose.yml` 启动项目

```shell
cd table-walkthrough && docker-compose up -d --build
```

等待运行结果

```shell
.....
Creating network "table-walkthrough_default" with the default driver
Creating table-walkthrough_zookeeper_1 ... done
Creating table-walkthrough_mysql_1     ... done
Creating table-walkthrough_grafana_1        ... done
Creating table-walkthrough_kafka_1     ... done
Creating table-walkthrough_jobmanager_1     ... done
Creating table-walkthrough_data-generator_1 ... done
Creating table-walkthrough_taskmanager_1    ... done
```

打开 [Flink UI](http://localhost:8082) 进行验证，可以看到任务已经启动

![image-20220709224437703](https://testpicp.oss-cn-shanghai.aliyuncs.com/uPic/image-20220709224437703.png)

任务运行情况

![image-20220709230015929](https://testpicp.oss-cn-shanghai.aliyuncs.com/uPic/image-20220709230015929.png)

在 [Grafana](http://localhost:3000/d/FOe0PbmGk/walkthrough?viewPanel=2&orgId=1&refresh=5s) 中查看结果：

![image-20220709230033215](/Users/fuwuchen/Library/Application Support/typora-user-images/image-20220709230033215.png)

在 MySQL 中查看结果

```shell
docker-compose exec mysql mysql -Dsql-demo -usql-demo -pdemo-sql
mysql> use sql-demo;
mysql> select count(*) from spend_report;
```

![image-20220709230117461](https://testpicp.oss-cn-shanghai.aliyuncs.com/uPic/image-20220709230117461.png)



