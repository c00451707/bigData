作业说明
在 table-walkthrough 的 SpendReport.java 中有如下代码：

report(transactions).executeInsert(“spend_report”);
实现 report 方法，将 transactions 表经过 report 函数处理后写入到 spend_report 表。

每分钟（或小时）计算在五分钟（或小时）内每个账号的平均交易金额（滑动窗口）？使用分钟还是小时作为单位均可。

docker 和 table-walkthrough 目录来自 apache/flink-playgrounds

代码实现
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
思路：

使用 Flink Table API，定义滑动窗口，每分钟触发一次，对 5 分钟窗口进行计算
窗口中按账号(account_id)分组，计算平均值
使用说明
使用 table-walkthrough 中的 Dockerfile 和 docker-compose.yml 启动项目

cd table-walkthrough && docker-compose up -d --build
等待运行结果

.....
Creating network "table-walkthrough_default" with the default driver
Creating table-walkthrough_zookeeper_1 ... done
Creating table-walkthrough_mysql_1     ... done
Creating table-walkthrough_grafana_1        ... done
Creating table-walkthrough_kafka_1     ... done
Creating table-walkthrough_jobmanager_1     ... done
Creating table-walkthrough_data-generator_1 ... done
Creating table-walkthrough_taskmanager_1    ... done


参考链接：
https://gitee.com/zero-night-01/week15-homework