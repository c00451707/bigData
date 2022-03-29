-- 题目一
-- 展示电影 ID 为 2116 这部电影各年龄段的平均影评分。

SELECT
    t1.age         as   age,
    avg(t2.rate)   as   avgRate
FROM
    t_user t1,
    t_rating t2
WHERE
    t1.user_id = t2.user_id AND t2.movie_id = '2116'
GROUP BY
    t1.age;


-- 题目二

-- 影评次数大于50次的  （错误的SQL， 条件 rateNum > 50要拿到外面去执行）
SELECT 
  movie_id,
  count(*)     as     rateNum
FROM 
  t_rating
WHERE rateNum > 50
GROUP BY 
  movie_id;

-- 男性评分 降序排序的所有电影  （可执行）
SELECT t1.movie_id, avg(t1.rate) as avgRate FROM t_user t2 INNER JOIN t_rating t1 ON t1.user_id = t2.user_id WHERE t2.sex = 'M' GROUP BY t1.movie_id, t1.movie_name ORDER BY avgRate DESC;



-- 男性评分 降序排序的所有电影 而且 影评次数大于50次的
SELECT
    t7.movie_id,
    t.movie_name,
    t8.avg_rate,
    t7.rate_num 
FROM
    t_movie t,
    ( SELECT movie_id, count(*) AS rate_num FROM t_rating GROUP BY movie_id ) t7,
    (
    SELECT
        t1.movie_id,
        avg( t1.rate ) AS avg_rate 
    FROM
        t_user t2
        INNER JOIN t_rating t1 ON t1.user_id = t2.user_id 
    WHERE
        t2.sex = 'M' 
    GROUP BY
        t1.movie_id 
    ) t8 
WHERE
    t7.movie_id = t8.movie_id 
    AND t7.movie_id = t.movie_id 
    AND t7.rate_num > 50 
ORDER BY
    t8.avg_rate DESC 
    LIMIT 10;
 

