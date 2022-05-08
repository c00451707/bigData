-- 作业二：构建SQL 满足如下要求
-- 通过set spark.sql.planChangeLog.level=WARN，查看：
-- 1. 构建一条SQL，同时apply 下面三条优化规则：
-- CombineFilters、CollapseProject、BooleanSimplification
-- 2. 构建一条SQL，同时apply 下面五条优化规则：
-- ConstantFolding、PushDownPredicates、ReplaceDistinctWithAggregate、
-- ReplaceExceptWithAntiJoin、FoldablePropagation

-- 设置
set spark.sql.planChangeLog.level=WARN

-- 检表语句
CREATE TABLE t1(a1 INT, a2 INT) USING parquet;
CREATE TABLE t2(b1 INT, b2 INT) USING parquet;

-- 第一个SQL
SELECT a11, (a2 + 1) AS a21
FROM (
SELECT (a1 + 1) AS a11, a2 FROM t1 WHERE a1 > 10
) WHERE a11 > 1 AND 1 = 1;

-- 第二个SQL
SELECT DISTINCT a1, a2, 'custom' a3
FROM (
SELECT * FROM t1 WHERE a2 = 10 AND 1 = 1
) WHERE a1 > 5 AND 1 = 1
EXCEPT SELECT b1, b2, 1.0 b3 FROM t2 WHERE b2 = 10 ;
