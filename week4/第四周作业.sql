-- 创建库
create database huster_chenbin;

-- LOCATION '/data/hive/users';
-- LOCATION '/data/hive/users/users.dat'; 加上这个报错  org.apache.hadoop.fs.FileAlreadyExistsException Path is not a directory: /data/hive/users/users.dat

-- 创建表
CREATE EXTERNAL TABLE IF NOT EXISTS t_user (
    user_id VARCHAR(256) COMMENT '用户ID',
    sex VARCHAR(256) COMMENT '用户性别',
    age INT COMMENT '用户年龄',
    occupation INT COMMENT '用户职业',
    zip_code INT COMMENT '用户编码'
)
COMMENT '用户表'
CLUSTERED BY (user_id) INTO 7 BUCKETS 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '::'
LINES TERMINATED BY '\n'
STORED AS ORC
LOCATION '/data/hive/users';

OK
Time taken: 0.174 seconds

select * from t_user limit 10;
OK
Failed with exception java.io.IOException:java.lang.RuntimeException: ORC split generation failed with exception: org.apache.orc.FileFormatException: Malformed ORC file hdfs://emr-header-1.cluster-285604:9000/data/hive/users/users.dat. Invalid postscript.

原因：
ORC格式是列式存储的表，不能直接从本地文件导入数据，只有当数据源表也是ORC格式存储时，才可以直接加载，否则会出现上述报错。

解决办法：
要么将数据源表改为以ORC格式存储的表，要么新建一个以textfile格式的临时表先将源文件数据加载到该表，然后在从textfile表中insert数据到ORC目标表中。


DROP TABLE t_user;
OK
Time taken: 0.056 seconds


CREATE EXTERNAL TABLE IF NOT EXISTS t_user (
    user_id VARCHAR(256) COMMENT '用户ID',
    sex VARCHAR(256) COMMENT '用户性别',
    age INT COMMENT '用户年龄',
    occupation INT COMMENT '用户职业',
    zip_code INT COMMENT '用户编码'
)
COMMENT '用户表'
CLUSTERED BY (user_id) INTO 7 BUCKETS 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '::'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/data/hive/users';

hive> select * from t_user limit 10;
OK
1		NULL	NULL	1
2		NULL	NULL	56
3		NULL	NULL	25
4		NULL	NULL	45
5		NULL	NULL	25
6		NULL	NULL	50
7		NULL	NULL	35
8		NULL	NULL	25
9		NULL	NULL	25
10		NULL	NULL	35

获取的数据不对

Hive 设置多字符作为分隔符的方法。
https://blog.csdn.net/qiulinsama/article/details/86655194

https://blog.csdn.net/fighting_one_piece/article/details/37610085

查看别人的建表语句
show create table t_user;
OK
CREATE EXTERNAL TABLE `t_user`(
  `userid` int COMMENT 'from deserializer', 
  `sex` string COMMENT 'from deserializer', 
  `age` int COMMENT 'from deserializer', 
  `occupation` int COMMENT 'from deserializer', 
  `zipcode` int COMMENT 'from deserializer')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'='::') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://emr-header-1.cluster-285604:9000/data/hive/users'
TBLPROPERTIES (
  'bucketing_version'='2', 
  'transient_lastDdlTime'='1647783953')
Time taken: 0.07 seconds, Fetched: 19 row(s)



-- 根据博客以及查看别人的建表语句，写出自己的。
CREATE EXTERNAL TABLE IF NOT EXISTS t_user (
    user_id VARCHAR(256) COMMENT '用户ID',
    sex VARCHAR(256) COMMENT '用户性别',
    age INT COMMENT '用户年龄',
    occupation INT COMMENT '用户职业',
    zip_code INT COMMENT '用户编码'
)
COMMENT '用户表'
CLUSTERED BY (user_id) INTO 7 BUCKETS 
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'='::')
STORED AS TEXTFILE
LOCATION '/data/hive/users';

查询结果：
select * from t_user limit 10;
OK
5029	M	25	4	70115
5030	M	18	1	70006
5031	F	25	4	89431
5032	M	18	17	2139
5033	F	56	13	60506
5034	F	25	15	62901
5035	F	25	17	19810
5036	M	35	7	14850
5037	M	35	12	11375
5038	M	25	20	10010
Time taken: 0.1 seconds, Fetched: 10 row(s)


截止目前，建表语句都可以OK，下面就是写SQL了。






