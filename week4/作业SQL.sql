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
